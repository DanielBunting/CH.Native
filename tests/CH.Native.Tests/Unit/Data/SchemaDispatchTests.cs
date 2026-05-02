using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using Xunit;

namespace CH.Native.Tests.Unit.Data;

/// <summary>
/// The default registry maps ClickHouse type-name strings — including their
/// parameterised forms — to the right reader/writer instance. These are the
/// names that arrive on the wire during the column-header phase, so a
/// dispatch regression breaks every column of an affected type silently.
/// </summary>
public class SchemaDispatchTests
{
    public static IEnumerable<object[]> SimpleTypeNames => new[]
    {
        new object[] { "Int8" },         new object[] { "Int16" },
        new object[] { "Int32" },        new object[] { "Int64" },
        new object[] { "Int128" },       new object[] { "Int256" },
        new object[] { "UInt8" },        new object[] { "UInt16" },
        new object[] { "UInt32" },       new object[] { "UInt64" },
        new object[] { "UInt128" },      new object[] { "UInt256" },
        new object[] { "Float32" },      new object[] { "Float64" },
        new object[] { "Bool" },         new object[] { "String" },
        new object[] { "UUID" },         new object[] { "IPv4" },
        new object[] { "IPv6" },         new object[] { "Date" },
        new object[] { "Date32" },       new object[] { "DateTime" },
        new object[] { "BFloat16" },
    };

    [Theory]
    [MemberData(nameof(SimpleTypeNames))]
    public void Reader_Default_ResolvesSimpleTypeName(string typeName)
    {
        Assert.NotNull(ColumnReaderRegistry.Default.GetReader(typeName));
    }

    [Theory]
    [MemberData(nameof(SimpleTypeNames))]
    public void Writer_Default_ResolvesSimpleTypeName(string typeName)
    {
        Assert.NotNull(ColumnWriterRegistry.Default.GetWriter(typeName));
    }

    [Fact]
    public void Reader_TryGet_UnknownType_ReturnsFalse()
    {
        var ok = ColumnReaderRegistry.Default.TryGetReader("NotARealType123", out var reader);
        Assert.False(ok);
        Assert.Null(reader);
    }

    [Fact]
    public void Writer_TryGet_UnknownType_ReturnsFalse()
    {
        var ok = ColumnWriterRegistry.Default.TryGetWriter("NotARealType123", out var writer);
        Assert.False(ok);
        Assert.Null(writer);
    }

    // Parameterised types — the registry must parse the parameters and
    // construct a configured reader/writer.

    [Theory]
    [InlineData("FixedString(8)", typeof(FixedStringColumnReader))]
    [InlineData("FixedString(16)", typeof(FixedStringColumnReader))]
    [InlineData("FixedString(255)", typeof(FixedStringColumnReader))]
    public void Reader_FixedString_ConstructsConfiguredReader(string typeName, Type expectedReaderType)
    {
        var reader = ColumnReaderRegistry.Default.GetReader(typeName);
        Assert.IsType(expectedReaderType, reader);
        Assert.Equal(typeName, reader.TypeName);
    }

    [Theory]
    [InlineData("Decimal32(4)")]
    [InlineData("Decimal64(8)")]
    [InlineData("Decimal128(18)")]
    [InlineData("Decimal256(30)")]
    public void Reader_Decimal_AcceptsScale(string typeName)
    {
        var reader = ColumnReaderRegistry.Default.GetReader(typeName);
        Assert.NotNull(reader);
    }

    [Theory]
    [InlineData("DateTime64(3)")]
    [InlineData("DateTime64(6)")]
    [InlineData("DateTime64(9)")]
    [InlineData("DateTime64(3, 'UTC')")]
    [InlineData("DateTime64(6, 'Europe/London')")]
    public void Reader_DateTime64_AcceptsPrecisionAndTimezone(string typeName)
    {
        var reader = ColumnReaderRegistry.Default.GetReader(typeName);
        Assert.NotNull(reader);
    }

    [Theory]
    [InlineData("DateTime('UTC')")]
    [InlineData("DateTime('America/New_York')")]
    public void Reader_DateTimeWithTimezone_AcceptsTimezone(string typeName)
    {
        var reader = ColumnReaderRegistry.Default.GetReader(typeName);
        Assert.NotNull(reader);
    }

    [Theory]
    [InlineData("Nullable(Int32)")]
    [InlineData("Nullable(String)")]
    [InlineData("Nullable(DateTime)")]
    [InlineData("Nullable(Decimal64(8))")]
    public void Reader_Nullable_WrapsInner(string typeName)
    {
        var reader = ColumnReaderRegistry.Default.GetReader(typeName);
        Assert.NotNull(reader);
        Assert.Equal(typeName, reader.TypeName);
    }

    [Theory]
    [InlineData("Array(Int32)")]
    [InlineData("Array(String)")]
    [InlineData("Array(Nullable(Int32))")]
    [InlineData("Array(Array(Int32))")]
    [InlineData("Array(LowCardinality(String))")]
    public void Reader_Array_WrapsElementType(string typeName)
    {
        var reader = ColumnReaderRegistry.Default.GetReader(typeName);
        Assert.NotNull(reader);
    }

    [Theory]
    [InlineData("LowCardinality(String)")]
    [InlineData("LowCardinality(Nullable(String))")]
    [InlineData("LowCardinality(FixedString(8))")]
    public void Reader_LowCardinality_WrapsInner(string typeName)
    {
        var reader = ColumnReaderRegistry.Default.GetReader(typeName);
        Assert.NotNull(reader);
    }

    [Theory]
    [InlineData("Map(String, Int32)")]
    [InlineData("Map(String, String)")]
    [InlineData("Map(String, Array(Int32))")]
    public void Reader_Map_AcceptsKeyAndValueTypes(string typeName)
    {
        var reader = ColumnReaderRegistry.Default.GetReader(typeName);
        Assert.NotNull(reader);
    }

    [Theory]
    [InlineData("Tuple(Int32, String)")]
    [InlineData("Tuple(String, Int32, Float64)")]
    [InlineData("Tuple(Nullable(Int32), Array(String))")]
    public void Reader_Tuple_AcceptsMultipleElements(string typeName)
    {
        var reader = ColumnReaderRegistry.Default.GetReader(typeName);
        Assert.NotNull(reader);
    }

    [Theory]
    [InlineData("Enum8('a' = 1, 'b' = 2)")]
    [InlineData("Enum16('alpha' = 1000, 'beta' = 2000)")]
    public void Reader_Enum_AcceptsLabelMappings(string typeName)
    {
        var reader = ColumnReaderRegistry.Default.GetReader(typeName);
        Assert.NotNull(reader);
    }

    [Theory]
    [InlineData("Array(LowCardinality(String))")]
    [InlineData("Array(Tuple(Int32, Nullable(String)))")]
    [InlineData("Map(LowCardinality(String), Array(Nullable(Int32)))")]
    [InlineData("Tuple(Array(Int32), Nullable(String), Map(String, Int32))")]
    public void Reader_DeeplyComposedTypes_Resolve(string typeName)
    {
        // Note: ClickHouse rejects Nullable wrapping non-nullable composites
        // like Array/Map/Tuple, so those forms are not exercised here — they
        // would fail at the NullableInnerValidator with InvalidOperationException
        // (which is the correct behaviour).
        var reader = ColumnReaderRegistry.Default.GetReader(typeName);
        Assert.NotNull(reader);
    }

    [Theory]
    [InlineData("Nullable(Int32)")]
    [InlineData("Array(String)")]
    [InlineData("Map(String, Int32)")]
    [InlineData("Tuple(Int32, String)")]
    [InlineData("LowCardinality(String)")]
    [InlineData("FixedString(16)")]
    [InlineData("Decimal64(8)")]
    [InlineData("DateTime64(3)")]
    public void Writer_ParameterisedTypes_Resolve(string typeName)
    {
        var writer = ColumnWriterRegistry.Default.GetWriter(typeName);
        Assert.NotNull(writer);
    }
}
