using System.Buffers;
using System.Buffers.Binary;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

public class Float32ColumnWriterTests
{
    [Fact]
    public void TypeName_IsFloat32() => Assert.Equal("Float32", new Float32ColumnWriter().TypeName);

    [Fact]
    public void ClrType_IsFloat() => Assert.Equal(typeof(float), new Float32ColumnWriter().ClrType);

    [Theory]
    [InlineData(0.0f)]
    [InlineData(1.0f)]
    [InlineData(-1.0f)]
    [InlineData(float.MaxValue)]
    [InlineData(float.MinValue)]
    [InlineData(float.Epsilon)] // smallest positive subnormal
    public void WriteValue_RoundTripsExactBits(float value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Float32ColumnWriter().WriteValue(ref writer, value);

        Assert.Equal(4, buffer.WrittenCount);
        var roundTrip = BinaryPrimitives.ReadSingleLittleEndian(buffer.WrittenSpan);
        Assert.Equal(BitConverter.SingleToUInt32Bits(value), BitConverter.SingleToUInt32Bits(roundTrip));
    }

    [Fact]
    public void WriteValue_NaN_PreservesBitPattern()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Float32ColumnWriter().WriteValue(ref writer, float.NaN);

        var roundTrip = BinaryPrimitives.ReadSingleLittleEndian(buffer.WrittenSpan);
        Assert.True(float.IsNaN(roundTrip));
    }

    [Theory]
    [InlineData(float.PositiveInfinity)]
    [InlineData(float.NegativeInfinity)]
    [InlineData(-0.0f)]
    public void WriteValue_SpecialValues_RoundTripExact(float value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Float32ColumnWriter().WriteValue(ref writer, value);

        var roundTrip = BinaryPrimitives.ReadSingleLittleEndian(buffer.WrittenSpan);
        Assert.Equal(BitConverter.SingleToUInt32Bits(value), BitConverter.SingleToUInt32Bits(roundTrip));
    }

    [Fact]
    public void WriteColumn_RoundTripsThroughReader()
    {
        var values = new[] { -1f, 0f, 1f, 3.14159f, float.MaxValue, float.MinValue };
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Float32ColumnWriter().WriteColumn(ref writer, values);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new Float32ColumnReader().ReadTypedColumn(ref reader, values.Length);

        Assert.Equal(values.Length, column.Count);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], column[i]);
    }

    [Fact]
    public void Registry_ResolvesFloat32Writer()
    {
        Assert.IsType<Float32ColumnWriter>(ColumnWriterRegistry.Default.GetWriter("Float32"));
    }
}
