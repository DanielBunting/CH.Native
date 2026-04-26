using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

/// <summary>
/// Pins the strict-null contract on <see cref="StringColumnWriter"/> and the
/// substitution-via-<see cref="IColumnWriter{T}.NullPlaceholder"/> contract on
/// <see cref="NullableRefColumnWriter{T}"/>. Without these, null into a non-
/// nullable String column is silently coerced to "" — see
/// <c>BulkInsertExtractionFailureTests.Extraction_NullForNonNullableStringColumn_ThrowsCleanly</c>.
/// </summary>
public class StringColumnWriterNullTests
{
    [Fact]
    public void NullPlaceholder_IsEmptyString()
    {
        Assert.Equal(string.Empty, new StringColumnWriter().NullPlaceholder);
    }

    [Fact]
    public void Typed_WriteColumn_NullEntry_Throws()
    {
        var sut = new StringColumnWriter();
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try { sut.WriteColumn(ref writer, new[] { "first", null!, "third" }); }
        catch (InvalidOperationException ex) { caught = ex; }

        Assert.NotNull(caught);
        Assert.Contains("row 1", caught!.Message);
        Assert.Contains("Nullable(String)", caught.Message);
    }

    [Fact]
    public void Typed_WriteValue_Null_Throws()
    {
        var sut = new StringColumnWriter();
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
        IColumnWriter sut = new StringColumnWriter();
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try { sut.WriteColumn(ref writer, new object?[] { "first", null, "third" }); }
        catch (InvalidOperationException ex) { caught = ex; }

        Assert.NotNull(caught);
        Assert.Contains("row 1", caught!.Message);
    }

    [Fact]
    public void NonGeneric_WriteValue_Null_Throws()
    {
        IColumnWriter sut = new StringColumnWriter();
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try { sut.WriteValue(ref writer, null); }
        catch (InvalidOperationException ex) { caught = ex; }
        Assert.NotNull(caught);
    }

    [Fact]
    public void NullableWrap_WriteColumn_NullSlot_WritesBitmapAndEmptyPlaceholder()
    {
        // The wrapper substitutes NullPlaceholder (empty string) into the slot
        // before delegating to the strict inner. Wire format: bitmap then values.
        var sut = new NullableRefColumnWriter<string>(new StringColumnWriter());
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        sut.WriteColumn(ref writer, new[] { "hi", null!, "bye" });

        var span = buffer.WrittenSpan;
        // 3 bitmap bytes + "hi" (1+2) + "" (1) + "bye" (1+3) = 11 bytes
        Assert.Equal(3 + 3 + 1 + 4, span.Length);
        Assert.Equal(0x00, span[0]);
        Assert.Equal(0x01, span[1]);
        Assert.Equal(0x00, span[2]);
        Assert.Equal(0x02, span[3]); // VarInt length 2
        Assert.Equal((byte)'h', span[4]);
        Assert.Equal((byte)'i', span[5]);
        Assert.Equal(0x00, span[6]); // empty placeholder length
        Assert.Equal(0x03, span[7]); // VarInt length 3
        Assert.Equal((byte)'b', span[8]);
        Assert.Equal((byte)'y', span[9]);
        Assert.Equal((byte)'e', span[10]);
    }

    [Fact]
    public void NullableWrap_NonGeneric_WriteColumn_NullSlot_WritesBitmapAndEmptyPlaceholder()
    {
        IColumnWriter sut = new NullableRefColumnWriter<string>(new StringColumnWriter());
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        sut.WriteColumn(ref writer, new object?[] { "hi", null });

        var span = buffer.WrittenSpan;
        // 2 bitmap bytes + "hi" (1+2) + "" (1) = 6 bytes
        Assert.Equal(2 + 3 + 1, span.Length);
        Assert.Equal(0x00, span[0]);
        Assert.Equal(0x01, span[1]);
        Assert.Equal(0x02, span[2]);
        Assert.Equal((byte)'h', span[3]);
        Assert.Equal((byte)'i', span[4]);
        Assert.Equal(0x00, span[5]);
    }

    [Fact]
    public void NullableWrap_WriteValue_Null_WritesBitmapAndEmptyPlaceholder()
    {
        var sut = new NullableRefColumnWriter<string>(new StringColumnWriter());
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        sut.WriteValue(ref writer, null);

        var span = buffer.WrittenSpan;
        Assert.Equal(2, span.Length);
        Assert.Equal(0x01, span[0]); // null
        Assert.Equal(0x00, span[1]); // empty string length
    }
}
