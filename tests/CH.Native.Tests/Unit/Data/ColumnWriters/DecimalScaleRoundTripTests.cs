using System.Buffers;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

/// <summary>
/// <see cref="DecimalWriterOverflowTests"/> only covers overflow rejection.
/// This pins the round-trip per scale across all four widths so a regression
/// in scale arithmetic surfaces immediately.
/// </summary>
public class DecimalScaleRoundTripTests
{
    [Theory]
    [InlineData(0, "1234")]
    [InlineData(2, "12.34")]
    [InlineData(4, "0.1234")]
    [InlineData(9, "0.123456789")]
    public void Decimal32_RoundTripsAcrossScales(int scale, string asString)
    {
        var value = decimal.Parse(asString, System.Globalization.CultureInfo.InvariantCulture);
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new Decimal32ColumnWriter(scale).WriteValue(ref writer, value);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var result = new Decimal32ColumnReader(scale).ReadValue(ref reader);
        Assert.Equal(value, result);
    }

    [Theory]
    [InlineData(0, "1234567890")]
    [InlineData(2, "12345678.90")]
    [InlineData(8, "12.34567890")]
    [InlineData(18, "1.234567890123456789")]
    public void Decimal64_RoundTripsAcrossScales(int scale, string asString)
    {
        var value = decimal.Parse(asString, System.Globalization.CultureInfo.InvariantCulture);
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new Decimal64ColumnWriter(scale).WriteValue(ref writer, value);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var result = new Decimal64ColumnReader(scale).ReadValue(ref reader);
        Assert.Equal(value, result);
    }

    [Theory]
    [InlineData("0")]
    [InlineData("-1")]
    [InlineData("1.234567890123456789")]
    public void Decimal128_RoundTrips(string asString)
    {
        var value = decimal.Parse(asString, System.Globalization.CultureInfo.InvariantCulture);
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new Decimal128ColumnWriter(18).WriteValue(ref writer, value);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var result = new Decimal128ColumnReader(18).ReadValue(ref reader);
        Assert.Equal(value, result);
    }

    [Theory]
    [InlineData("0")]
    [InlineData("-99.99")]
    [InlineData("12345678901234567890.0")]
    public void Decimal256_RoundTrips(string asString)
    {
        var value = decimal.Parse(asString, System.Globalization.CultureInfo.InvariantCulture);
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new Decimal256ColumnWriter(18).WriteValue(ref writer, value);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var result = new Decimal256ColumnReader(18).ReadValue(ref reader);
        Assert.Equal(value, result);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(10)]
    public void Decimal32_OutOfRangeScale_Throws(int scale)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new Decimal32ColumnWriter(scale));
    }
}
