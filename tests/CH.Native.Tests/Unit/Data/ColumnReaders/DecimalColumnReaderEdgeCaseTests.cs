using System.Buffers;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

/// <summary>
/// Decimal* readers reverse the writer's "scale → integer mantissa" trick.
/// Decimal32 stores Int32, Decimal64 stores Int64. Reader must accept both
/// the unscaled-zero case (all-zero bytes round-trip to 0m) and signed
/// negatives (two's-complement decode).
/// </summary>
public class DecimalColumnReaderEdgeCaseTests
{
    [Fact]
    public void Decimal32_TypeName_EmbedsScale()
    {
        Assert.Equal("Decimal32(4)", new Decimal32ColumnReader(4).TypeName);
    }

    [Fact]
    public void Decimal32_OutOfRangeScale_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new Decimal32ColumnReader(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new Decimal32ColumnReader(10));
    }

    [Fact]
    public void Decimal64_OutOfRangeScale_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new Decimal64ColumnReader(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new Decimal64ColumnReader(19));
    }

    [Theory]
    [InlineData(0, "1234")]
    [InlineData(2, "12.34")]
    [InlineData(4, "0.1234")]
    [InlineData(9, "0.123456789")]
    [InlineData(0, "-1234")]
    [InlineData(4, "-0.5")]
    public void Decimal32_RoundTripsAcrossScales(int scale, string asString)
    {
        var value = decimal.Parse(asString, System.Globalization.CultureInfo.InvariantCulture);
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new Decimal32ColumnWriter(scale).WriteValue(ref writer, value);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        Assert.Equal(value, new Decimal32ColumnReader(scale).ReadValue(ref reader));
    }

    [Theory]
    [InlineData(0, "9876543210")]
    [InlineData(8, "1.23456789")]
    [InlineData(18, "0.123456789012345678")]
    [InlineData(2, "-1234567.89")]
    public void Decimal64_RoundTripsAcrossScales(int scale, string asString)
    {
        var value = decimal.Parse(asString, System.Globalization.CultureInfo.InvariantCulture);
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new Decimal64ColumnWriter(scale).WriteValue(ref writer, value);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        Assert.Equal(value, new Decimal64ColumnReader(scale).ReadValue(ref reader));
    }

    [Fact]
    public void Decimal32_ZeroValue_DecodesAsZero()
    {
        var bytes = new byte[4];  // all zero
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        Assert.Equal(0m, new Decimal32ColumnReader(4).ReadValue(ref reader));
    }

    [Fact]
    public void Decimal32_ReadColumn_StridesByFourBytes()
    {
        var values = new[] { 1.0m, 2.5m, -3.75m, 0m, 99.99m };
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new Decimal32ColumnWriter(2).WriteColumn(ref writer, values);

        Assert.Equal(values.Length * 4, buffer.WrittenCount);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new Decimal32ColumnReader(2).ReadTypedColumn(ref reader, values.Length);

        for (int i = 0; i < values.Length; i++) Assert.Equal(values[i], column[i]);
    }

    [Fact]
    public void Decimal64_ReadColumn_StridesByEightBytes()
    {
        var values = new[] { 1.5m, -2.75m, 0m };
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new Decimal64ColumnWriter(4).WriteColumn(ref writer, values);

        Assert.Equal(values.Length * 8, buffer.WrittenCount);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new Decimal64ColumnReader(4).ReadTypedColumn(ref reader, values.Length);

        for (int i = 0; i < values.Length; i++) Assert.Equal(values[i], column[i]);
    }
}
