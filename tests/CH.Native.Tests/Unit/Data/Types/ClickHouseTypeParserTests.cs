using CH.Native.Data.Types;
using Xunit;

namespace CH.Native.Tests.Unit.Data.Types;

public class ClickHouseTypeParserTests
{
    [Theory]
    [InlineData("Int32")]
    [InlineData("Int64")]
    [InlineData("String")]
    [InlineData("Float64")]
    [InlineData("Bool")]
    [InlineData("UUID")]
    public void Parse_SimpleType_ReturnsCorrectBaseName(string typeName)
    {
        var result = ClickHouseTypeParser.Parse(typeName);

        Assert.Equal(typeName, result.BaseName);
        Assert.False(result.IsParameterized);
        Assert.Empty(result.TypeArguments);
        Assert.Empty(result.Parameters);
    }

    [Theory]
    [InlineData("DateTime64(3)", "DateTime64", "3")]
    [InlineData("Decimal32(4)", "Decimal32", "4")]
    [InlineData("Decimal64(9)", "Decimal64", "9")]
    [InlineData("Decimal128(18)", "Decimal128", "18")]
    [InlineData("FixedString(32)", "FixedString", "32")]
    public void Parse_ParameterizedType_ReturnsCorrectParameters(string typeName, string baseName, string param)
    {
        var result = ClickHouseTypeParser.Parse(typeName);

        Assert.Equal(baseName, result.BaseName);
        Assert.True(result.IsParameterized);
        Assert.Empty(result.TypeArguments);
        Assert.Single(result.Parameters);
        Assert.Equal(param, result.Parameters[0]);
    }

    [Fact]
    public void Parse_DateTime64WithTimezone_ReturnsTimezoneParameter()
    {
        var result = ClickHouseTypeParser.Parse("DateTime64(3, 'UTC')");

        Assert.Equal("DateTime64", result.BaseName);
        Assert.Equal(2, result.Parameters.Count);
        Assert.Equal("3", result.Parameters[0]);
        Assert.Equal("'UTC'", result.Parameters[1]);
    }

    [Theory]
    [InlineData("Nullable(Int32)", "Nullable", "Int32")]
    [InlineData("Array(String)", "Array", "String")]
    [InlineData("LowCardinality(String)", "LowCardinality", "String")]
    public void Parse_NestedType_ReturnsTypeArgument(string typeName, string baseName, string innerType)
    {
        var result = ClickHouseTypeParser.Parse(typeName);

        Assert.Equal(baseName, result.BaseName);
        Assert.True(result.IsParameterized);
        Assert.Single(result.TypeArguments);
        Assert.Equal(innerType, result.TypeArguments[0].BaseName);
    }

    [Fact]
    public void Parse_MapType_ReturnsTwoTypeArguments()
    {
        var result = ClickHouseTypeParser.Parse("Map(String, Int32)");

        Assert.Equal("Map", result.BaseName);
        Assert.Equal(2, result.TypeArguments.Count);
        Assert.Equal("String", result.TypeArguments[0].BaseName);
        Assert.Equal("Int32", result.TypeArguments[1].BaseName);
    }

    [Fact]
    public void Parse_TupleType_ReturnsAllTypeArguments()
    {
        var result = ClickHouseTypeParser.Parse("Tuple(Int32, String, Float64)");

        Assert.Equal("Tuple", result.BaseName);
        Assert.Equal(3, result.TypeArguments.Count);
        Assert.Equal("Int32", result.TypeArguments[0].BaseName);
        Assert.Equal("String", result.TypeArguments[1].BaseName);
        Assert.Equal("Float64", result.TypeArguments[2].BaseName);
    }

    [Fact]
    public void Parse_DeeplyNestedType_ParsesCorrectly()
    {
        var result = ClickHouseTypeParser.Parse("Nullable(Array(String))");

        Assert.Equal("Nullable", result.BaseName);
        Assert.Single(result.TypeArguments);

        var arrayType = result.TypeArguments[0];
        Assert.Equal("Array", arrayType.BaseName);
        Assert.Single(arrayType.TypeArguments);
        Assert.Equal("String", arrayType.TypeArguments[0].BaseName);
    }

    [Fact]
    public void Parse_ComplexNestedType_ParsesCorrectly()
    {
        var result = ClickHouseTypeParser.Parse("Map(String, Array(Nullable(Int32)))");

        Assert.Equal("Map", result.BaseName);
        Assert.Equal(2, result.TypeArguments.Count);
        Assert.Equal("String", result.TypeArguments[0].BaseName);

        var arrayType = result.TypeArguments[1];
        Assert.Equal("Array", arrayType.BaseName);

        var nullableType = arrayType.TypeArguments[0];
        Assert.Equal("Nullable", nullableType.BaseName);
        Assert.Equal("Int32", nullableType.TypeArguments[0].BaseName);
    }

    [Fact]
    public void Parse_Enum8WithValues_ParsesEnumDefinition()
    {
        var result = ClickHouseTypeParser.Parse("Enum8('a' = 1, 'b' = 2)");

        Assert.Equal("Enum8", result.BaseName);
        Assert.Empty(result.TypeArguments);
        Assert.Equal(2, result.Parameters.Count);
        Assert.Equal("'a' = 1", result.Parameters[0]);
        Assert.Equal("'b' = 2", result.Parameters[1]);
    }

    [Fact]
    public void Parse_Enum8WithNegativeValue_ParsesCorrectly()
    {
        var result = ClickHouseTypeParser.Parse("Enum8('negative' = -1, 'positive' = 1)");

        Assert.Equal("Enum8", result.BaseName);
        Assert.Equal(2, result.Parameters.Count);
        Assert.Contains("-1", result.Parameters[0]);
    }

    [Fact]
    public void Parse_DecimalWithPrecisionAndScale_ParsesCorrectly()
    {
        var result = ClickHouseTypeParser.Parse("Decimal(18, 4)");

        Assert.Equal("Decimal", result.BaseName);
        Assert.Equal(2, result.Parameters.Count);
        Assert.Equal("18", result.Parameters[0]);
        Assert.Equal("4", result.Parameters[1]);
    }

    [Theory]
    [InlineData("Nullable(Int32)", true)]
    [InlineData("Int32", false)]
    [InlineData("Nullable(String)", true)]
    public void Parse_IsNullable_ReturnsCorrectValue(string typeName, bool expected)
    {
        var result = ClickHouseTypeParser.Parse(typeName);
        Assert.Equal(expected, result.IsNullable);
    }

    [Theory]
    [InlineData("Array(Int32)", true)]
    [InlineData("Int32", false)]
    public void Parse_IsArray_ReturnsCorrectValue(string typeName, bool expected)
    {
        var result = ClickHouseTypeParser.Parse(typeName);
        Assert.Equal(expected, result.IsArray);
    }

    [Theory]
    [InlineData("Map(String, Int32)", true)]
    [InlineData("Int32", false)]
    public void Parse_IsMap_ReturnsCorrectValue(string typeName, bool expected)
    {
        var result = ClickHouseTypeParser.Parse(typeName);
        Assert.Equal(expected, result.IsMap);
    }

    [Theory]
    [InlineData("LowCardinality(String)", true)]
    [InlineData("String", false)]
    public void Parse_IsLowCardinality_ReturnsCorrectValue(string typeName, bool expected)
    {
        var result = ClickHouseTypeParser.Parse(typeName);
        Assert.Equal(expected, result.IsLowCardinality);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void Parse_EmptyOrWhitespace_ThrowsArgumentException(string typeName)
    {
        Assert.Throws<ArgumentException>(() => ClickHouseTypeParser.Parse(typeName));
    }

    [Fact]
    public void Parse_UnclosedParenthesis_ThrowsFormatException()
    {
        Assert.Throws<FormatException>(() => ClickHouseTypeParser.Parse("Nullable(Int32"));
    }

    [Fact]
    public void TryParse_ValidType_ReturnsType()
    {
        var result = ClickHouseTypeParser.TryParse("Int32");

        Assert.NotNull(result);
        Assert.Equal("Int32", result.BaseName);
    }

    [Fact]
    public void TryParse_InvalidType_ReturnsNull()
    {
        var result = ClickHouseTypeParser.TryParse("Nullable(Int32");

        Assert.Null(result);
    }

    [Fact]
    public void ToString_SimpleType_ReturnsBaseName()
    {
        var type = ClickHouseTypeParser.Parse("Int32");
        Assert.Equal("Int32", type.ToString());
    }

    [Fact]
    public void ToString_NestedType_ReturnsFormattedString()
    {
        var type = ClickHouseTypeParser.Parse("Nullable(Int32)");
        Assert.Equal("Nullable(Int32)", type.ToString());
    }

    [Fact]
    public void ToString_ParameterizedType_ReturnsFormattedString()
    {
        var type = ClickHouseTypeParser.Parse("DateTime64(3)");
        Assert.Equal("DateTime64(3)", type.ToString());
    }

    // Named Tuple Tests

    [Fact]
    public void Parse_NamedTuple_ParsesFieldNamesAndTypes()
    {
        var result = ClickHouseTypeParser.Parse("Tuple(id UInt64, name String)");

        Assert.Equal("Tuple", result.BaseName);
        Assert.Equal(2, result.TypeArguments.Count);
        Assert.Equal("UInt64", result.TypeArguments[0].BaseName);
        Assert.Equal("String", result.TypeArguments[1].BaseName);

        Assert.True(result.HasFieldNames);
        Assert.Equal(2, result.FieldNames.Count);
        Assert.Equal("id", result.FieldNames[0]);
        Assert.Equal("name", result.FieldNames[1]);
    }

    [Fact]
    public void Parse_NamedTupleWithComplexTypes_ParsesCorrectly()
    {
        var result = ClickHouseTypeParser.Parse("Tuple(id UInt64, tags Array(String), metadata Nullable(String))");

        Assert.Equal("Tuple", result.BaseName);
        Assert.Equal(3, result.TypeArguments.Count);

        Assert.Equal("UInt64", result.TypeArguments[0].BaseName);
        Assert.Equal("Array", result.TypeArguments[1].BaseName);
        Assert.Equal("String", result.TypeArguments[1].TypeArguments[0].BaseName);
        Assert.Equal("Nullable", result.TypeArguments[2].BaseName);

        Assert.True(result.HasFieldNames);
        Assert.Equal(3, result.FieldNames.Count);
        Assert.Equal("id", result.FieldNames[0]);
        Assert.Equal("tags", result.FieldNames[1]);
        Assert.Equal("metadata", result.FieldNames[2]);
    }

    [Fact]
    public void Parse_PositionalTuple_HasNoFieldNames()
    {
        var result = ClickHouseTypeParser.Parse("Tuple(Int32, String)");

        Assert.Equal("Tuple", result.BaseName);
        Assert.Equal(2, result.TypeArguments.Count);
        Assert.False(result.HasFieldNames);
        Assert.Empty(result.FieldNames);
    }

    [Fact]
    public void Parse_NestedType_ParsesFieldNamesAndTypes()
    {
        var result = ClickHouseTypeParser.Parse("Nested(id UInt64, name String)");

        Assert.Equal("Nested", result.BaseName);
        Assert.True(result.IsNested);
        Assert.Equal(2, result.TypeArguments.Count);
        Assert.Equal("UInt64", result.TypeArguments[0].BaseName);
        Assert.Equal("String", result.TypeArguments[1].BaseName);

        Assert.True(result.HasFieldNames);
        Assert.Equal(2, result.FieldNames.Count);
        Assert.Equal("id", result.FieldNames[0]);
        Assert.Equal("name", result.FieldNames[1]);
    }

    [Fact]
    public void Parse_NestedWithArrays_ParsesCorrectly()
    {
        var result = ClickHouseTypeParser.Parse("Nested(key String, values Array(Int32))");

        Assert.Equal("Nested", result.BaseName);
        Assert.True(result.IsNested);
        Assert.Equal(2, result.TypeArguments.Count);

        Assert.Equal("String", result.TypeArguments[0].BaseName);
        Assert.Equal("Array", result.TypeArguments[1].BaseName);

        Assert.Equal("key", result.FieldNames[0]);
        Assert.Equal("values", result.FieldNames[1]);
    }

    [Fact]
    public void ToString_NamedTuple_ReturnsFormattedString()
    {
        var type = ClickHouseTypeParser.Parse("Tuple(id UInt64, name String)");
        Assert.Equal("Tuple(id UInt64, name String)", type.ToString());
    }

    [Fact]
    public void ToString_NamedNestedType_ReturnsFormattedString()
    {
        var type = ClickHouseTypeParser.Parse("Nested(id UInt64, name String)");
        Assert.Equal("Nested(id UInt64, name String)", type.ToString());
    }

    [Theory]
    [InlineData("Tuple(a Int32)", 1)]
    [InlineData("Tuple(a Int32, b String)", 2)]
    [InlineData("Tuple(a Int32, b String, c Float64)", 3)]
    public void Parse_NamedTuple_CountsFieldsCorrectly(string typeName, int expectedCount)
    {
        var result = ClickHouseTypeParser.Parse(typeName);

        Assert.Equal(expectedCount, result.TypeArguments.Count);
        Assert.Equal(expectedCount, result.FieldNames.Count);
    }

    [Fact]
    public void Parse_NamedTupleWithUnderscoreFieldName_ParsesCorrectly()
    {
        var result = ClickHouseTypeParser.Parse("Tuple(user_id UInt64, created_at DateTime)");

        Assert.True(result.HasFieldNames);
        Assert.Equal("user_id", result.FieldNames[0]);
        Assert.Equal("created_at", result.FieldNames[1]);
    }
}
