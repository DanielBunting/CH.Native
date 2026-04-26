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

    #region String / FixedString null handling (non-nullable column rejects null; nullable accepts)

    [Fact]
    public void StringExtractor_NonNullable_NullValue_Throws()
    {
        // Regression guard: writing null into a non-nullable String column
        // used to silently coerce to "" via `?? string.Empty`. The fix
        // surfaces a clear InvalidOperationException naming the offending
        // column and row index — analytics-quality data depends on it.
        var property = typeof(StringRow).GetProperty(nameof(StringRow.Value))!;
        var extractor = ColumnExtractorFactory.Create<StringRow>(property, "value", "String");

        var rows = new List<StringRow>
        {
            new() { Value = "first" },
            new() { Value = null! },
        };

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try
        {
            extractor.ExtractAndWrite(ref writer, rows, 2);
        }
        catch (InvalidOperationException ex)
        {
            caught = ex;
        }

        Assert.NotNull(caught);
        Assert.Contains("'value'", caught!.Message);
        Assert.Contains("non-nullable", caught.Message);
        Assert.Contains("row 1", caught.Message);
    }

    [Fact]
    public void StringExtractor_Nullable_NullValue_WritesBitmapAndEmptyPlaceholder()
    {
        // The nullable branch writes the bitmap byte (1 for null) followed
        // by an empty-string placeholder for the value. This is the wire
        // format ClickHouse expects for Nullable(String). Pin the bytes so
        // a future "collapse both branches" refactor cannot silently change
        // how nulls are encoded.
        var property = typeof(StringRow).GetProperty(nameof(StringRow.Value))!;
        var extractor = ColumnExtractorFactory.Create<StringRow>(property, "value", "Nullable(String)");

        var rows = new List<StringRow>
        {
            new() { Value = "hi" },
            new() { Value = null! },
        };

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        extractor.ExtractAndWrite(ref writer, rows, 2);

        var span = buffer.WrittenSpan;
        // Layout: 2 bitmap bytes, then 2 length-prefixed strings ("hi", "").
        // "hi" = VarInt(2) + 'h' + 'i' = 3 bytes; "" = VarInt(0) = 1 byte.
        Assert.Equal(2 + 3 + 1, span.Length);
        Assert.Equal(0x00, span[0]); // not null
        Assert.Equal(0x01, span[1]); // null
        Assert.Equal(0x02, span[2]); // length 2
        Assert.Equal((byte)'h', span[3]);
        Assert.Equal((byte)'i', span[4]);
        Assert.Equal(0x00, span[5]); // length 0 (empty placeholder)
    }

    [Fact]
    public void FixedStringExtractor_NonNullable_NullValue_Throws()
    {
        // Same fix as StringExtractor: silent zero-padding looks like a
        // valid all-zero payload and is indistinguishable from real data.
        var property = typeof(StringRow).GetProperty(nameof(StringRow.Value))!;
        var extractor = ColumnExtractorFactory.Create<StringRow>(property, "value", "FixedString(8)");

        var rows = new List<StringRow>
        {
            new() { Value = null! },
        };

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try
        {
            extractor.ExtractAndWrite(ref writer, rows, 1);
        }
        catch (InvalidOperationException ex)
        {
            caught = ex;
        }

        Assert.NotNull(caught);
        Assert.Contains("'value'", caught!.Message);
        Assert.Contains("FixedString", caught.Message);
        Assert.Contains("row 0", caught.Message);
    }

    [Fact]
    public void FixedStringExtractor_Nullable_NullValue_WritesBitmapAndZeroPaddedPlaceholder()
    {
        // Mirror of StringExtractor_Nullable: pin the wire format for
        // Nullable(FixedString(N)). The placeholder is a zero-padded buffer
        // of exactly N bytes (not an empty buffer).
        var property = typeof(StringRow).GetProperty(nameof(StringRow.Value))!;
        var extractor = ColumnExtractorFactory.Create<StringRow>(property, "value", "Nullable(FixedString(4))");

        var rows = new List<StringRow>
        {
            new() { Value = "ab" },
            new() { Value = null! },
        };

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        extractor.ExtractAndWrite(ref writer, rows, 2);

        var span = buffer.WrittenSpan;
        // Layout: 2 bitmap bytes, then 2 × FixedString(4) = 8 bytes.
        Assert.Equal(2 + 2 * 4, span.Length);
        Assert.Equal(0x00, span[0]); // not null
        Assert.Equal(0x01, span[1]); // null
        // "ab" zero-padded to 4 bytes
        Assert.Equal((byte)'a', span[2]);
        Assert.Equal((byte)'b', span[3]);
        Assert.Equal(0x00, span[4]);
        Assert.Equal(0x00, span[5]);
        // null placeholder: all zero
        Assert.Equal(0x00, span[6]);
        Assert.Equal(0x00, span[7]);
        Assert.Equal(0x00, span[8]);
        Assert.Equal(0x00, span[9]);
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

    private class StringRow
    {
        public string? Value { get; set; }
    }

    #endregion
}
