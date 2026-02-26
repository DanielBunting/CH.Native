using System.Buffers;
using System.Net;
using System.Reflection;
using CH.Native.BulkInsert;
using CH.Native.Mapping;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.BulkInsert;

public class ColumnExtractorFactoryTests
{
    #region Unsupported composite types throw NotSupportedException

    [Fact]
    public void Create_IntArrayProperty_ThrowsNotSupported()
    {
        var property = typeof(ArrayRow).GetProperty(nameof(ArrayRow.Values))!;

        Assert.Throws<NotSupportedException>(() =>
            ColumnExtractorFactory.Create<ArrayRow>(property, "values", "Array(Int32)"));
    }

    [Fact]
    public void Create_DictionaryProperty_ThrowsNotSupported()
    {
        var property = typeof(MapRow).GetProperty(nameof(MapRow.Metadata))!;

        Assert.Throws<NotSupportedException>(() =>
            ColumnExtractorFactory.Create<MapRow>(property, "metadata", "Map(String, Int32)"));
    }

    [Fact]
    public void Create_ObjectArrayProperty_ThrowsNotSupported()
    {
        var property = typeof(TupleRow).GetProperty(nameof(TupleRow.Pair))!;

        Assert.Throws<NotSupportedException>(() =>
            ColumnExtractorFactory.Create<TupleRow>(property, "pair", "Tuple(Int32, String)"));
    }

    #endregion

    #region Non-composite types return typed extractors

    [Fact]
    public void Create_IntProperty_ReturnsTypedExtractor()
    {
        var property = typeof(SimpleRow).GetProperty(nameof(SimpleRow.Id))!;

        var extractor = ColumnExtractorFactory.Create<SimpleRow>(property, "id", "Int32");

        Assert.NotNull(extractor);
    }

    [Fact]
    public void Create_StringProperty_ReturnsTypedExtractor()
    {
        var property = typeof(SimpleRow).GetProperty(nameof(SimpleRow.Name))!;

        var extractor = ColumnExtractorFactory.Create<SimpleRow>(property, "name", "String");

        Assert.NotNull(extractor);
    }

    [Fact]
    public void Create_FixedStringProperty_ReturnsExtractor()
    {
        var property = typeof(SimpleRow).GetProperty(nameof(SimpleRow.Name))!;

        var extractor = ColumnExtractorFactory.Create<SimpleRow>(property, "val", "FixedString(16)");

        Assert.NotNull(extractor);
    }

    [Fact]
    public void Create_NullableFixedStringProperty_ReturnsExtractor()
    {
        var property = typeof(SimpleRow).GetProperty(nameof(SimpleRow.Name))!;

        var extractor = ColumnExtractorFactory.Create<SimpleRow>(property, "val", "Nullable(FixedString(16))");

        Assert.NotNull(extractor);
    }

    [Fact]
    public void Create_LowCardinalityFixedStringProperty_ReturnsExtractor()
    {
        var property = typeof(SimpleRow).GetProperty(nameof(SimpleRow.Name))!;

        var extractor = ColumnExtractorFactory.Create<SimpleRow>(property, "val", "LowCardinality(FixedString(8))");

        Assert.NotNull(extractor);
    }

    #endregion

    #region IPv4 byte order

    [Fact]
    public void ExtractAndWrite_IPv4_WritesLittleEndianByteOrder()
    {
        var property = typeof(IPv4TestRow).GetProperty(nameof(IPv4TestRow.Address))!;
        var extractor = ColumnExtractorFactory.Create<IPv4TestRow>(property, "address", "IPv4");

        var row = new IPv4TestRow { Address = IPAddress.Parse("127.0.0.1") };
        var rows = new List<IPv4TestRow> { row };

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        extractor.ExtractAndWrite(ref writer, rows, 1);

        var written = buffer.WrittenSpan;
        Assert.Equal(4, written.Length);
        Assert.Equal(new byte[] { 0x01, 0x00, 0x00, 0x7F }, written.ToArray());
    }

    [Fact]
    public void ExtractAndWrite_IPv4_AsymmetricAddress_WritesLittleEndianByteOrder()
    {
        var property = typeof(IPv4TestRow).GetProperty(nameof(IPv4TestRow.Address))!;
        var extractor = ColumnExtractorFactory.Create<IPv4TestRow>(property, "address", "IPv4");

        var row = new IPv4TestRow { Address = IPAddress.Parse("192.168.1.100") };
        var rows = new List<IPv4TestRow> { row };

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        extractor.ExtractAndWrite(ref writer, rows, 1);

        var written = buffer.WrittenSpan;
        Assert.Equal(4, written.Length);
        Assert.Equal(new byte[] { 0x64, 0x01, 0xA8, 0xC0 }, written.ToArray());
    }

    #endregion

    #region Nullable Decimal precision and scale

    [Fact]
    public void ExtractAndWrite_NullableDecimal64_WritesCorrectScale()
    {
        var property = typeof(NullableDecimalRow).GetProperty(nameof(NullableDecimalRow.Value))!;
        var extractor = ColumnExtractorFactory.Create<NullableDecimalRow>(property, "value", "Nullable(Decimal64(8))");

        var row = new NullableDecimalRow { Value = 99.12345678m };
        var rows = new List<NullableDecimalRow> { row };

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        extractor.ExtractAndWrite(ref writer, rows, 1);

        var written = buffer.WrittenSpan;
        // 1 byte null indicator + 8 bytes Int64
        Assert.Equal(9, written.Length);
        Assert.Equal(0x00, written[0]); // not null
        var scaledValue = BitConverter.ToInt64(written[1..]);
        Assert.Equal(9_912_345_678L, scaledValue);
    }

    [Fact]
    public void ExtractAndWrite_NullableGenericDecimal_WritesCorrectPrecisionAndScale()
    {
        var property = typeof(NullableDecimalRow).GetProperty(nameof(NullableDecimalRow.Value))!;
        var extractor = ColumnExtractorFactory.Create<NullableDecimalRow>(property, "value", "Nullable(Decimal(9, 4))");

        var row = new NullableDecimalRow { Value = 12345.6789m };
        var rows = new List<NullableDecimalRow> { row };

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        extractor.ExtractAndWrite(ref writer, rows, 1);

        var written = buffer.WrittenSpan;
        // 1 byte null indicator + 4 bytes Int32 (precision 9 → Decimal32)
        Assert.Equal(5, written.Length);
        Assert.Equal(0x00, written[0]); // not null
        var scaledValue = BitConverter.ToInt32(written[1..]);
        Assert.Equal(123_456_789, scaledValue);
    }

    [Fact]
    public void ExtractAndWrite_NullableDecimal64_Null_WritesNullIndicator()
    {
        var property = typeof(NullableDecimalRow).GetProperty(nameof(NullableDecimalRow.Value))!;
        var extractor = ColumnExtractorFactory.Create<NullableDecimalRow>(property, "value", "Nullable(Decimal64(8))");

        var row = new NullableDecimalRow { Value = null };
        var rows = new List<NullableDecimalRow> { row };

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        extractor.ExtractAndWrite(ref writer, rows, 1);

        var written = buffer.WrittenSpan;
        Assert.Equal(9, written.Length);
        Assert.Equal(0x01, written[0]); // null indicator
        // Value bytes should be zero-filled
        Assert.All(written[1..].ToArray(), b => Assert.Equal(0x00, b));
    }

    #endregion

    #region Test POCOs

    private class ArrayRow
    {
        public int[] Values { get; set; } = Array.Empty<int>();
    }

    private class MapRow
    {
        public Dictionary<string, int> Metadata { get; set; } = new();
    }

    private class TupleRow
    {
        public object?[] Pair { get; set; } = Array.Empty<object?>();
    }

    private class SimpleRow
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private class IPv4TestRow
    {
        public IPAddress? Address { get; set; }
    }

    private class DecimalRow
    {
        public decimal Value { get; set; }
    }

    private class NullableDecimalRow
    {
        public decimal? Value { get; set; }
    }

    #endregion
}
