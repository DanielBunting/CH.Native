using CH.Native.Parameters;
using Xunit;

namespace CH.Native.Tests.Unit.Parameters;

public class ClickHouseTypeMapperTests
{
    #region Basic Type Inference

    [Theory]
    [InlineData((sbyte)1, "Int8")]
    [InlineData((byte)1, "UInt8")]
    [InlineData((short)1, "Int16")]
    [InlineData((ushort)1, "UInt16")]
    [InlineData(1, "Int32")]
    [InlineData((uint)1, "UInt32")]
    [InlineData((long)1, "Int64")]
    [InlineData((ulong)1, "UInt64")]
    public void InferType_IntegerTypes_ReturnsCorrectClickHouseType(object value, string expected)
    {
        var result = ClickHouseTypeMapper.InferType(value);
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData(1.0f, "Float32")]
    [InlineData(1.0d, "Float64")]
    public void InferType_FloatingPointTypes_ReturnsCorrectClickHouseType(object value, string expected)
    {
        var result = ClickHouseTypeMapper.InferType(value);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void InferType_Decimal_ReturnsDecimal128()
    {
        var result = ClickHouseTypeMapper.InferType(1.0m);
        Assert.Equal("Decimal128(18)", result);
    }

    [Fact]
    public void InferType_Bool_ReturnsBool()
    {
        Assert.Equal("Bool", ClickHouseTypeMapper.InferType(true));
        Assert.Equal("Bool", ClickHouseTypeMapper.InferType(false));
    }

    [Fact]
    public void InferType_String_ReturnsString()
    {
        Assert.Equal("String", ClickHouseTypeMapper.InferType("test"));
    }

    [Fact]
    public void InferType_DateTime_ReturnsDateTime()
    {
        Assert.Equal("DateTime", ClickHouseTypeMapper.InferType(DateTime.Now));
    }

    [Fact]
    public void InferType_DateTimeOffset_ReturnsDateTime64()
    {
        Assert.Equal("DateTime64(6)", ClickHouseTypeMapper.InferType(DateTimeOffset.Now));
    }

    [Fact]
    public void InferType_DateOnly_ReturnsDate()
    {
        Assert.Equal("Date", ClickHouseTypeMapper.InferType(DateOnly.FromDateTime(DateTime.Today)));
    }

    [Fact]
    public void InferType_Guid_ReturnsUUID()
    {
        Assert.Equal("UUID", ClickHouseTypeMapper.InferType(Guid.NewGuid()));
    }

    #endregion

    #region Array Types

    [Fact]
    public void InferType_IntArray_ReturnsArrayOfInt32()
    {
        Assert.Equal("Array(Int32)", ClickHouseTypeMapper.InferType(new[] { 1, 2, 3 }));
    }

    [Fact]
    public void InferType_StringArray_ReturnsArrayOfString()
    {
        Assert.Equal("Array(String)", ClickHouseTypeMapper.InferType(new[] { "a", "b" }));
    }

    [Fact]
    public void InferType_LongArray_ReturnsArrayOfInt64()
    {
        Assert.Equal("Array(Int64)", ClickHouseTypeMapper.InferType(new long[] { 1L, 2L }));
    }

    [Fact]
    public void InferType_ListOfInt_ReturnsArrayOfInt32()
    {
        Assert.Equal("Array(Int32)", ClickHouseTypeMapper.InferType(new List<int> { 1, 2, 3 }));
    }

    #endregion

    #region Edge Cases

    [Fact]
    public void InferType_Null_ThrowsArgumentException()
    {
        var ex = Assert.Throws<ArgumentException>(() => ClickHouseTypeMapper.InferType(null));
        Assert.Contains("Cannot infer type from null", ex.Message);
    }

    [Fact]
    public void InferType_UnsupportedType_ThrowsNotSupportedException()
    {
        var ex = Assert.Throws<NotSupportedException>(
            () => ClickHouseTypeMapper.InferType(new object()));
        Assert.Contains("Cannot infer ClickHouse type", ex.Message);
    }

    #endregion

    #region InferTypeFromClrType

    [Fact]
    public void InferTypeFromClrType_Int32_ReturnsInt32()
    {
        Assert.Equal("Int32", ClickHouseTypeMapper.InferTypeFromClrType(typeof(int)));
    }

    [Fact]
    public void InferTypeFromClrType_NullableInt32_ReturnsInt32()
    {
        Assert.Equal("Int32", ClickHouseTypeMapper.InferTypeFromClrType(typeof(int?)));
    }

    [Fact]
    public void InferTypeFromClrType_IntArray_ReturnsArrayOfInt32()
    {
        Assert.Equal("Array(Int32)", ClickHouseTypeMapper.InferTypeFromClrType(typeof(int[])));
    }

    #endregion
}
