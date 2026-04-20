using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

public class BFloat16ColumnWriterTests
{
    [Fact]
    public void TypeName_IsBFloat16()
    {
        Assert.Equal("BFloat16", new BFloat16ColumnWriter().TypeName);
    }

    [Fact]
    public void ClrType_IsFloat()
    {
        Assert.Equal(typeof(float), new BFloat16ColumnWriter().ClrType);
    }

    [Theory]
    [InlineData(0.0f, (ushort)0x0000)]
    [InlineData(-0.0f, (ushort)0x8000)]
    [InlineData(1.0f, (ushort)0x3F80)]
    [InlineData(-1.0f, (ushort)0xBF80)]
    [InlineData(2.0f, (ushort)0x4000)]
    [InlineData(float.PositiveInfinity, (ushort)0x7F80)]
    [InlineData(float.NegativeInfinity, (ushort)0xFF80)]
    public void WriteValue_TruncatesToHigh16Bits(float value, ushort expected)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new BFloat16ColumnWriter().WriteValue(ref writer, value);

        Assert.Equal(2, buffer.WrittenCount);
        Assert.Equal(expected, BitConverter.ToUInt16(buffer.WrittenSpan));
    }

    [Fact]
    public void WriteValue_NaN_StaysNaN()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new BFloat16ColumnWriter().WriteValue(ref writer, float.NaN);

        var raw = BitConverter.ToUInt16(buffer.WrittenSpan);
        var bits = ((uint)raw) << 16;
        var roundTrip = BitConverter.UInt32BitsToSingle(bits);
        Assert.True(float.IsNaN(roundTrip));
    }

    [Theory]
    [InlineData(1.0f)]
    [InlineData(-1.0f)]
    [InlineData(3.14f)] // truncates but should still be ~pi
    [InlineData(float.PositiveInfinity)]
    [InlineData(float.NegativeInfinity)]
    public void WriteValue_RoundTripsThroughReader(float value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new BFloat16ColumnWriter().WriteValue(ref writer, value);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var result = new BFloat16ColumnReader().ReadValue(ref reader);

        // Round-trip is lossy for the low 16 mantissa bits but the high bits must match exactly
        var inputBits = BitConverter.SingleToUInt32Bits(value) >> 16;
        var resultBits = BitConverter.SingleToUInt32Bits(result) >> 16;
        Assert.Equal(inputBits, resultBits);
    }

    [Fact]
    public void Registry_ResolvesBFloat16Writer()
    {
        var writer = ColumnWriterRegistry.Default.GetWriter("BFloat16");
        Assert.IsType<BFloat16ColumnWriter>(writer);
    }
}
