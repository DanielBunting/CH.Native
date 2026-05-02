using System.Buffers;
using System.Buffers.Binary;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

public class Float64ColumnWriterTests
{
    [Fact]
    public void TypeName_IsFloat64() => Assert.Equal("Float64", new Float64ColumnWriter().TypeName);

    [Fact]
    public void ClrType_IsDouble() => Assert.Equal(typeof(double), new Float64ColumnWriter().ClrType);

    [Theory]
    [InlineData(0.0)]
    [InlineData(Math.PI)]
    [InlineData(-Math.E)]
    [InlineData(double.MaxValue)]
    [InlineData(double.MinValue)]
    [InlineData(double.Epsilon)]
    public void WriteValue_RoundTripsExactBits(double value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Float64ColumnWriter().WriteValue(ref writer, value);

        Assert.Equal(8, buffer.WrittenCount);
        var roundTrip = BinaryPrimitives.ReadDoubleLittleEndian(buffer.WrittenSpan);
        Assert.Equal(BitConverter.DoubleToUInt64Bits(value), BitConverter.DoubleToUInt64Bits(roundTrip));
    }

    [Theory]
    [InlineData(double.NaN)]
    [InlineData(double.PositiveInfinity)]
    [InlineData(double.NegativeInfinity)]
    [InlineData(-0.0)]
    public void WriteValue_SpecialValues_PreserveBits(double value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Float64ColumnWriter().WriteValue(ref writer, value);

        var raw = BinaryPrimitives.ReadDoubleLittleEndian(buffer.WrittenSpan);
        if (double.IsNaN(value))
            Assert.True(double.IsNaN(raw));
        else
            Assert.Equal(BitConverter.DoubleToUInt64Bits(value), BitConverter.DoubleToUInt64Bits(raw));
    }

    [Fact]
    public void WriteColumn_RoundTripsThroughReader()
    {
        var values = new[] { -Math.E, 0.0, 1.0, Math.PI, double.MaxValue, double.MinValue };
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Float64ColumnWriter().WriteColumn(ref writer, values);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new Float64ColumnReader().ReadTypedColumn(ref reader, values.Length);

        Assert.Equal(values.Length, column.Count);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], column[i]);
    }

    [Fact]
    public void Registry_ResolvesFloat64Writer()
    {
        Assert.IsType<Float64ColumnWriter>(ColumnWriterRegistry.Default.GetWriter("Float64"));
    }
}
