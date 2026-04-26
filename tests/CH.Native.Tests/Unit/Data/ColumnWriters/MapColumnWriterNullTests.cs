using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

/// <summary>
/// Pins the strict-null contract on <see cref="MapColumnWriter{TKey,TValue}"/>.
/// The original code silently emitted an empty Map for a null Dictionary
/// (<c>?.Count ?? 0</c>), which is indistinguishable from a real empty
/// Dictionary. The fix throws on the bare path and substitutes
/// <c>new Dictionary&lt;K,V&gt;()</c> via the Nullable wrapper.
/// </summary>
public class MapColumnWriterNullTests
{
    private static MapColumnWriter<string, int> NewWriter() =>
        new(new StringColumnWriter(), new Int32ColumnWriter());

    [Fact]
    public void NullPlaceholder_IsEmptyDictionary()
    {
        var placeholder = NewWriter().NullPlaceholder;
        Assert.NotNull(placeholder);
        Assert.Empty(placeholder);
    }

    [Fact]
    public void Typed_WriteColumn_NullEntry_Throws()
    {
        var sut = NewWriter();
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try
        {
            sut.WriteColumn(ref writer, new[]
            {
                new Dictionary<string, int> { ["a"] = 1 },
                null!,
                new Dictionary<string, int> { ["b"] = 2 },
            });
        }
        catch (InvalidOperationException ex) { caught = ex; }

        Assert.NotNull(caught);
        Assert.Contains("row 1", caught!.Message);
        Assert.Contains("Map", caught.Message);
        Assert.Contains("Nullable(Map", caught.Message);
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
    public void NonGeneric_WriteColumn_NullEntry_Throws()
    {
        IColumnWriter sut = NewWriter();
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try
        {
            sut.WriteColumn(ref writer, new object?[]
            {
                new Dictionary<string, int> { ["a"] = 1 },
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
    public void NullableWrap_WriteColumn_NullSlot_WritesBitmapAndEmptyMapOffset()
    {
        // Nullable(Map(String, Int32)): bitmap byte per row + cumulative
        // UInt64 offsets + flat key column + flat value column. A null slot
        // becomes an empty Dictionary, which contributes 0 to entries but the
        // cumulative offset still increments by 0.
        var sut = new NullableRefColumnWriter<Dictionary<string, int>>(NewWriter());
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        sut.WriteColumn(ref writer, new[]
        {
            new Dictionary<string, int> { ["a"] = 1 },
            null!,
            new Dictionary<string, int> { ["b"] = 2 },
        });

        var span = buffer.WrittenSpan;
        // 3 bitmap bytes + 3 × UInt64 offsets + 2 keys (varint-len-1 + 1 byte each)
        // + 2 × Int32 values.
        Assert.Equal(3 + 3 * 8 + 2 * 2 + 2 * 4, span.Length);

        // Bitmap
        Assert.Equal(0x00, span[0]);
        Assert.Equal(0x01, span[1]);
        Assert.Equal(0x00, span[2]);

        // Cumulative offsets
        Assert.Equal(1UL, BitConverter.ToUInt64(span[3..11]));   // row 0: 1 entry
        Assert.Equal(1UL, BitConverter.ToUInt64(span[11..19]));  // row 1 (null → empty): still 1
        Assert.Equal(2UL, BitConverter.ToUInt64(span[19..27]));  // row 2: +1 entry

        // Key column: VarInt(1) "a" then VarInt(1) "b"
        Assert.Equal(0x01, span[27]);
        Assert.Equal((byte)'a', span[28]);
        Assert.Equal(0x01, span[29]);
        Assert.Equal((byte)'b', span[30]);

        // Value column: Int32(1), Int32(2)
        Assert.Equal(1, BitConverter.ToInt32(span[31..35]));
        Assert.Equal(2, BitConverter.ToInt32(span[35..39]));
    }
}
