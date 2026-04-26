using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

/// <summary>
/// Pins the strict-null contract on <see cref="TupleColumnWriter"/>. The
/// original code silently passed <c>null</c> elements to inner writers when
/// the outer Tuple row was null (<c>values[row]?[e]</c>), causing numeric
/// inner writers to land zero bytes — silent data corruption with the
/// fixed-arity columnar wire layout.
/// </summary>
/// <remarks>
/// No <c>NullableWrap_*</c> test here: ClickHouse rejects
/// <c>Nullable(Tuple(...))</c> at DDL, so wrapping the writer in
/// <see cref="NullableRefColumnWriter{T}"/> is unreachable in practice. The
/// <c>NullPlaceholder</c> declaration on Phase 1 was preventive — its
/// arity-sized all-null array would propagate null elements to inner writers
/// (which their own strict-null contracts reject), so the wrap path can only
/// produce valid bytes when every inner element writer is itself a Nullable
/// wrapper. We do not exercise that here because no production codepath
/// constructs it.
/// </remarks>
public class TupleColumnWriterNullTests
{
    private static TupleColumnWriter NewWriter() =>
        new(new IColumnWriter[] { new Int32ColumnWriter(), new StringColumnWriter() });

    [Fact]
    public void NullPlaceholder_IsArityZeroedArray()
    {
        var placeholder = NewWriter().NullPlaceholder;
        Assert.Equal(2, placeholder.Length);
        Assert.All(placeholder, e => Assert.Null(e));
    }

    [Fact]
    public void Typed_WriteColumn_NullRow_Throws()
    {
        var sut = NewWriter();
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try
        {
            sut.WriteColumn(ref writer, new[]
            {
                new object[] { 1, "a" },
                null!,
                new object[] { 2, "b" },
            });
        }
        catch (InvalidOperationException ex) { caught = ex; }

        Assert.NotNull(caught);
        Assert.Contains("row 1", caught!.Message);
        Assert.Contains("Tuple", caught.Message);
    }

    [Fact]
    public void Typed_WriteValue_Null_Throws()
    {
        var sut = NewWriter();
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try { sut.WriteValue(ref writer, null!); }
        catch (InvalidOperationException ex) { caught = ex; }
        Assert.NotNull(caught);
    }

    [Fact]
    public void NonGeneric_WriteColumn_NullRow_Throws()
    {
        IColumnWriter sut = NewWriter();
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try
        {
            sut.WriteColumn(ref writer, new object?[]
            {
                new object[] { 1, "a" },
                null,
            });
        }
        catch (InvalidOperationException ex) { caught = ex; }

        Assert.NotNull(caught);
        Assert.Contains("row 1", caught!.Message);
    }

    [Fact]
    public void NonGeneric_WriteValue_Null_Throws()
    {
        IColumnWriter sut = NewWriter();
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try { sut.WriteValue(ref writer, null); }
        catch (InvalidOperationException ex) { caught = ex; }
        Assert.NotNull(caught);
    }

    [Fact]
    public void NonGeneric_WriteColumn_UnsupportedType_Throws()
    {
        // Phase 2 follow-up: the silent Array.Empty<object>() fallback for
        // non-(object[]/ITuple) types is now a throw — silent garbage on a
        // fixed-arity columnar wire layout is data corruption.
        IColumnWriter sut = NewWriter();
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try
        {
            sut.WriteColumn(ref writer, new object?[]
            {
                new object[] { 1, "a" },
                "not a tuple",
            });
        }
        catch (InvalidOperationException ex) { caught = ex; }

        Assert.NotNull(caught);
        Assert.Contains("row 1", caught!.Message);
        Assert.Contains("unsupported value type", caught.Message);
    }

    [Fact]
    public void NonGeneric_WriteValue_UnsupportedType_Throws()
    {
        IColumnWriter sut = NewWriter();
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try { sut.WriteValue(ref writer, "not a tuple"); }
        catch (InvalidOperationException ex) { caught = ex; }

        Assert.NotNull(caught);
        Assert.Contains("unsupported value type", caught!.Message);
    }

    [Fact]
    public void Typed_WriteColumn_AllNonNull_RoundTripsColumnarLayout()
    {
        // Sanity: with no nulls, the writer emits all first-elements then all
        // second-elements (columnar). Locks in the happy-path wire shape.
        var sut = NewWriter();
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        sut.WriteColumn(ref writer, new[]
        {
            new object[] { 1, "a" },
            new object[] { 2, "bc" },
        });

        var span = buffer.WrittenSpan;
        // 2 × Int32 (8 bytes) + VarInt(1) "a" + VarInt(2) "bc" = 8 + 2 + 3 = 13.
        Assert.Equal(8 + 2 + 3, span.Length);
        Assert.Equal(1, BitConverter.ToInt32(span[0..4]));
        Assert.Equal(2, BitConverter.ToInt32(span[4..8]));
        Assert.Equal(0x01, span[8]);
        Assert.Equal((byte)'a', span[9]);
        Assert.Equal(0x02, span[10]);
        Assert.Equal((byte)'b', span[11]);
        Assert.Equal((byte)'c', span[12]);
    }
}
