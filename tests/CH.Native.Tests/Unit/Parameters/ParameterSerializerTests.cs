using System.Net;
using CH.Native.Parameters;
using Xunit;

namespace CH.Native.Tests.Unit.Parameters;

public class ParameterSerializerTests
{
    #region String Escaping (Critical for SQL injection prevention)

    [Fact]
    public void EscapeString_SimpleString_WrapsInQuotes()
    {
        var result = ParameterSerializer.EscapeString("hello");
        Assert.Equal("'hello'", result);
    }

    [Fact]
    public void EscapeString_StringWithSingleQuote_EscapesQuote()
    {
        var result = ParameterSerializer.EscapeString("O'Brien");
        Assert.Equal(@"'O\'Brien'", result);
    }

    [Fact]
    public void EscapeString_StringWithBackslash_EscapesBackslash()
    {
        var result = ParameterSerializer.EscapeString(@"path\to\file");
        Assert.Equal(@"'path\\to\\file'", result);
    }

    [Fact]
    public void EscapeString_StringWithTab_PassesThrough()
    {
        // Control characters are passed through as-is in Field dump format
        var result = ParameterSerializer.EscapeString("col1\tcol2");
        Assert.Equal("'col1\tcol2'", result);
    }

    [Fact]
    public void EscapeString_StringWithNewline_PassesThrough()
    {
        // Control characters are passed through as-is in Field dump format
        var result = ParameterSerializer.EscapeString("line1\nline2");
        Assert.Equal("'line1\nline2'", result);
    }

    [Fact]
    public void EscapeString_StringWithCarriageReturn_PassesThrough()
    {
        // Control characters are passed through as-is in Field dump format
        var result = ParameterSerializer.EscapeString("line1\rline2");
        Assert.Equal("'line1\rline2'", result);
    }

    [Fact]
    public void EscapeString_StringWithNullChar_PassesThrough()
    {
        // Control characters are passed through as-is in Field dump format
        var result = ParameterSerializer.EscapeString("before\0after");
        Assert.Equal("'before\0after'", result);
    }

    [Fact]
    public void EscapeString_SqlInjectionAttempt_IsProperlyEscaped()
    {
        // Classic SQL injection attempt
        var result = ParameterSerializer.EscapeString("'; DROP TABLE users; --");
        Assert.Equal(@"'\'; DROP TABLE users; --'", result);
    }

    [Fact]
    public void EscapeString_ComplexSqlInjectionAttempt_IsProperlyEscaped()
    {
        // Input contains backslash followed by quote - both must be escaped
        var input = "\\'; DROP TABLE users; --";
        var result = ParameterSerializer.EscapeString(input);

        // Verify the result starts and ends with quotes
        Assert.StartsWith("'", result);
        Assert.EndsWith("'", result);

        // Verify the backslash was escaped (now appears as \\)
        Assert.Contains("\\\\", result);

        // Verify the quote was escaped (now appears as \')
        Assert.Contains("\\'", result);

        // Verify the DROP TABLE is still there (but safely escaped)
        Assert.Contains("DROP TABLE", result);
    }

    [Fact]
    public void EscapeString_EmptyString_ReturnsEmptyQuotedString()
    {
        var result = ParameterSerializer.EscapeString("");
        Assert.Equal("''", result);
    }

    [Fact]
    public void EscapeString_UnicodeCharacters_PassedThrough()
    {
        var result = ParameterSerializer.EscapeString("你好世界 🌍");
        Assert.Equal("'你好世界 🌍'", result);
    }

    #endregion

    #region Numeric Types

    [Theory]
    [InlineData((sbyte)-128, "'-128'")]
    [InlineData((sbyte)127, "'127'")]
    [InlineData((byte)0, "'0'")]
    [InlineData((byte)255, "'255'")]
    [InlineData((short)-32768, "'-32768'")]
    [InlineData((ushort)65535, "'65535'")]
    [InlineData(int.MinValue, "'-2147483648'")]
    [InlineData(int.MaxValue, "'2147483647'")]
    public void Serialize_IntegerTypes_ReturnsQuotedString(object value, string expected)
    {
        var typeName = ClickHouseTypeMapper.InferType(value);
        var result = ParameterSerializer.Serialize(value, typeName);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Serialize_Float_ReturnsQuotedString()
    {
        var result = ParameterSerializer.Serialize(3.14f, "Float32");
        // Float precision may vary, just check it starts with expected digits
        Assert.StartsWith("'3.14", result);
        Assert.EndsWith("'", result);
    }

    [Fact]
    public void Serialize_Double_ReturnsQuotedString()
    {
        var result = ParameterSerializer.Serialize(3.14159265358979d, "Float64");
        Assert.StartsWith("'3.14159265358979", result);
        Assert.EndsWith("'", result);
    }

    [Fact]
    public void Serialize_FloatNaN_ReturnsQuotedNan()
    {
        Assert.Equal("'nan'", ParameterSerializer.Serialize(float.NaN, "Float32"));
    }

    [Fact]
    public void Serialize_FloatPositiveInfinity_ReturnsQuotedInf()
    {
        Assert.Equal("'inf'", ParameterSerializer.Serialize(float.PositiveInfinity, "Float32"));
    }

    [Fact]
    public void Serialize_FloatNegativeInfinity_ReturnsQuotedNegInf()
    {
        Assert.Equal("'-inf'", ParameterSerializer.Serialize(float.NegativeInfinity, "Float32"));
    }

    [Fact]
    public void Serialize_DoubleNaN_ReturnsQuotedNan()
    {
        Assert.Equal("'nan'", ParameterSerializer.Serialize(double.NaN, "Float64"));
    }

    [Fact]
    public void Serialize_Decimal_ReturnsQuotedString()
    {
        var result = ParameterSerializer.Serialize(123.456m, "Decimal128(18)");
        Assert.Equal("'123.456'", result);
    }

    #endregion

    #region Boolean

    [Fact]
    public void Serialize_BoolTrue_ReturnsQuoted1()
    {
        Assert.Equal("'1'", ParameterSerializer.Serialize(true, "Bool"));
    }

    [Fact]
    public void Serialize_BoolFalse_ReturnsQuoted0()
    {
        Assert.Equal("'0'", ParameterSerializer.Serialize(false, "Bool"));
    }

    #endregion

    #region Date/Time

    [Fact]
    public void Serialize_DateTime_ReturnsFormattedString()
    {
        var dt = new DateTime(2024, 1, 15, 10, 30, 45);
        var result = ParameterSerializer.Serialize(dt, "DateTime");
        Assert.Equal("'2024-01-15 10:30:45'", result);
    }

    [Fact]
    public void Serialize_DateTime64_ReturnsFormattedStringWithMicroseconds()
    {
        var dt = new DateTime(2024, 1, 15, 10, 30, 45, 123).AddTicks(4560);
        var result = ParameterSerializer.Serialize(dt, "DateTime64(6)");
        Assert.StartsWith("'2024-01-15 10:30:45.", result);
    }

    [Fact]
    public void Serialize_DateTimeOffset_ReturnsUtcFormattedString()
    {
        var dto = new DateTimeOffset(2024, 1, 15, 10, 30, 45, TimeSpan.FromHours(5));
        var result = ParameterSerializer.Serialize(dto, "DateTime64(6)");
        Assert.StartsWith("'2024-01-15 05:30:45", result); // UTC time
    }

    [Fact]
    public void Serialize_DateOnly_ReturnsFormattedString()
    {
        var date = new DateOnly(2024, 1, 15);
        var result = ParameterSerializer.Serialize(date, "Date");
        Assert.Equal("'2024-01-15'", result);
    }

    #endregion

    #region Guid

    [Fact]
    public void Serialize_Guid_ReturnsFormattedString()
    {
        var guid = new Guid("12345678-1234-1234-1234-123456789abc");
        var result = ParameterSerializer.Serialize(guid, "UUID");
        Assert.Equal("'12345678-1234-1234-1234-123456789abc'", result);
    }

    #endregion

    #region Arrays

    [Fact]
    public void Serialize_IntArray_ReturnsQuotedBracketedList()
    {
        var result = ParameterSerializer.Serialize(new[] { 1, 2, 3 }, "Array(Int32)");
        Assert.Equal("'[1, 2, 3]'", result);
    }

    [Fact]
    public void Serialize_StringArray_ReturnsQuotedBracketedListWithEscapedStrings()
    {
        var result = ParameterSerializer.Serialize(new[] { "a", "b's", "c" }, "Array(String)");
        // Array elements use EscapeStringForArray which quotes with single quotes
        // The whole array is then wrapped in EscapeString which escapes those single quotes
        // Result: '[\'a\', \'b\\\'s\', \'c\']'
        Assert.Equal("'[\\'a\\', \\'b\\\\\\'s\\', \\'c\\']'", result);
    }

    [Fact]
    public void Serialize_EmptyArray_ReturnsQuotedEmptyBrackets()
    {
        var result = ParameterSerializer.Serialize(Array.Empty<int>(), "Array(Int32)");
        Assert.Equal("'[]'", result);
    }

    [Fact]
    public void Serialize_ListOfInt_ReturnsQuotedBracketedList()
    {
        var result = ParameterSerializer.Serialize(new List<int> { 1, 2, 3 }, "Array(Int32)");
        Assert.Equal("'[1, 2, 3]'", result);
    }

    #endregion

    #region Null Handling

    [Fact]
    public void Serialize_NullWithNullableType_ReturnsNULL()
    {
        var result = ParameterSerializer.Serialize(null, "Nullable(Int32)");
        Assert.Equal("NULL", result);
    }

    [Fact]
    public void Serialize_NullWithNonNullableType_ThrowsInvalidOperationException()
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => ParameterSerializer.Serialize(null, "Int32"));
        Assert.Contains("Cannot pass NULL value for non-nullable type", ex.Message);
    }

    #endregion

    #region IP Address

    [Fact]
    public void Serialize_IPv4Address_ReturnsQuotedString()
    {
        var ip = IPAddress.Parse("192.168.1.1");
        var result = ParameterSerializer.Serialize(ip, "IPv6");
        Assert.Equal("'192.168.1.1'", result);
    }

    [Fact]
    public void Serialize_IPv6Address_ReturnsQuotedString()
    {
        var ip = IPAddress.Parse("::1");
        var result = ParameterSerializer.Serialize(ip, "IPv6");
        Assert.Equal("'::1'", result);
    }

    #endregion
}
