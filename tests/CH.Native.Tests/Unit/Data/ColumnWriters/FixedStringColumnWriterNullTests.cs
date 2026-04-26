using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

/// <summary>
/// Mirror of <see cref="StringColumnWriterNullTests"/> for FixedString. The
/// silent zero-padding of null in the original code was even more dangerous
/// than null-to-"" — a zero-padded buffer is indistinguishable from a real
/// all-zero payload at audit time.
/// </summary>
public class FixedStringColumnWriterNullTests
{
    private const int Length = 4;

    [Fact]
    public void NullPlaceholder_IsEmptyByteArray()
    {
        Assert.Equal(Array.Empty<byte>(), new FixedStringColumnWriter(Length).NullPlaceholder);
    }

    [Fact]
    public void Typed_WriteColumn_NullEntry_Throws()
    {
        var sut = new FixedStringColumnWriter(Length);
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try { sut.WriteColumn(ref writer, new[] { new byte[] { 0x01 }, null! }); }
        catch (InvalidOperationException ex) { caught = ex; }

        Assert.NotNull(caught);
        Assert.Contains("row 1", caught!.Message);
        Assert.Contains("FixedString", caught.Message);
    }

    [Fact]
    public void Typed_WriteValue_Null_Throws()
    {
        var sut = new FixedStringColumnWriter(Length);
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
        IColumnWriter sut = new FixedStringColumnWriter(Length);
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try { sut.WriteColumn(ref writer, new object?[] { new byte[] { 0x01 }, null }); }
        catch (InvalidOperationException ex) { caught = ex; }

        Assert.NotNull(caught);
        Assert.Contains("row 1", caught!.Message);
    }

    [Fact]
    public void NonGeneric_WriteValue_Null_Throws()
    {
        IColumnWriter sut = new FixedStringColumnWriter(Length);
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try { sut.WriteValue(ref writer, null); }
        catch (InvalidOperationException ex) { caught = ex; }
        Assert.NotNull(caught);
    }

    [Fact]
    public void NullableWrap_WriteColumn_NullSlot_WritesBitmapAndZeroPaddedPlaceholder()
    {
        var sut = new NullableRefColumnWriter<byte[]>(new FixedStringColumnWriter(Length));
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        sut.WriteColumn(ref writer, new[] { new byte[] { 0x41, 0x42 }, null! });

        var span = buffer.WrittenSpan;
        // 2 bitmap bytes + 2 × FixedString(4) = 10 bytes
        Assert.Equal(2 + 2 * Length, span.Length);
        Assert.Equal(0x00, span[0]);
        Assert.Equal(0x01, span[1]);
        // "AB" zero-padded to 4
        Assert.Equal(0x41, span[2]);
        Assert.Equal(0x42, span[3]);
        Assert.Equal(0x00, span[4]);
        Assert.Equal(0x00, span[5]);
        // null placeholder: 4 zero bytes
        Assert.Equal(0x00, span[6]);
        Assert.Equal(0x00, span[7]);
        Assert.Equal(0x00, span[8]);
        Assert.Equal(0x00, span[9]);
    }

    [Fact]
    public void NullableWrap_WriteValue_Null_WritesBitmapAndZeroPaddedPlaceholder()
    {
        var sut = new NullableRefColumnWriter<byte[]>(new FixedStringColumnWriter(Length));
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        sut.WriteValue(ref writer, null);

        var span = buffer.WrittenSpan;
        Assert.Equal(1 + Length, span.Length);
        Assert.Equal(0x01, span[0]);
        for (int i = 1; i <= Length; i++) Assert.Equal(0x00, span[i]);
    }
}
