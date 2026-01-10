using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Protocol;

public class VarIntTests
{
    [Theory]
    [InlineData(0UL, new byte[] { 0x00 })]
    [InlineData(1UL, new byte[] { 0x01 })]
    [InlineData(127UL, new byte[] { 0x7F })]
    [InlineData(128UL, new byte[] { 0x80, 0x01 })]
    [InlineData(16383UL, new byte[] { 0xFF, 0x7F })]
    [InlineData(16384UL, new byte[] { 0x80, 0x80, 0x01 })]
    public void Write_BoundaryValues_EncodesCorrectly(ulong value, byte[] expected)
    {
        Span<byte> buffer = stackalloc byte[VarInt.MaxLength];
        int written = VarInt.Write(buffer, value);

        Assert.Equal(expected.Length, written);
        Assert.True(buffer[..written].SequenceEqual(expected));
    }

    [Theory]
    [InlineData(new byte[] { 0x00 }, 0UL)]
    [InlineData(new byte[] { 0x01 }, 1UL)]
    [InlineData(new byte[] { 0x7F }, 127UL)]
    [InlineData(new byte[] { 0x80, 0x01 }, 128UL)]
    [InlineData(new byte[] { 0xFF, 0x7F }, 16383UL)]
    [InlineData(new byte[] { 0x80, 0x80, 0x01 }, 16384UL)]
    public void Read_BoundaryValues_DecodesCorrectly(byte[] encoded, ulong expected)
    {
        var result = VarInt.Read(encoded, out int bytesRead);

        Assert.Equal(expected, result);
        Assert.Equal(encoded.Length, bytesRead);
    }

    [Theory]
    [InlineData(0UL)]
    [InlineData(1UL)]
    [InlineData(127UL)]
    [InlineData(128UL)]
    [InlineData(16383UL)]
    [InlineData(16384UL)]
    [InlineData(2097151UL)]
    [InlineData(268435455UL)]
    [InlineData(ulong.MaxValue)]
    public void RoundTrip_VariousValues_PreservesValue(ulong value)
    {
        Span<byte> buffer = stackalloc byte[VarInt.MaxLength];
        int written = VarInt.Write(buffer, value);
        var result = VarInt.Read(buffer, out int bytesRead);

        Assert.Equal(value, result);
        Assert.Equal(written, bytesRead);
    }

    [Fact]
    public void Write_MaxValue_Uses10Bytes()
    {
        Span<byte> buffer = stackalloc byte[VarInt.MaxLength];
        int written = VarInt.Write(buffer, ulong.MaxValue);

        Assert.Equal(10, written);
    }
}
