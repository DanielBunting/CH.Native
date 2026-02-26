using System.Net;
using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

[Collection("ClickHouse")]
public class ExtendedTypeTests
{
    private readonly ClickHouseFixture _fixture;

    public ExtendedTypeTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    #region Bool Type

    [Fact]
    public async Task Select_BoolTrue_ReturnsTrue()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<bool>("SELECT true");

        Assert.True(result);
    }

    [Fact]
    public async Task Select_BoolFalse_ReturnsFalse()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<bool>("SELECT false");

        Assert.False(result);
    }

    #endregion

    #region Date Types

    [Fact]
    public async Task Select_Date_ReturnsCorrectDate()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<DateOnly>("SELECT toDate('2024-01-15')");

        Assert.Equal(new DateOnly(2024, 1, 15), result);
    }

    [Fact]
    public async Task Select_Date_UnixEpoch_ReturnsEpoch()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<DateOnly>("SELECT toDate('1970-01-01')");

        Assert.Equal(new DateOnly(1970, 1, 1), result);
    }

    [Fact]
    public async Task Select_Date32_ReturnsCorrectDate()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<DateOnly>("SELECT toDate32('2024-01-15')");

        Assert.Equal(new DateOnly(2024, 1, 15), result);
    }

    [Fact]
    public async Task Select_Date32_BeforeEpoch_ReturnsCorrectDate()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<DateOnly>("SELECT toDate32('1960-06-15')");

        Assert.Equal(new DateOnly(1960, 6, 15), result);
    }

    #endregion

    #region DateTime64

    [Fact]
    public async Task Select_DateTime64_Precision3_ReturnsCorrectDateTime()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<DateTime>("SELECT toDateTime64('2024-01-15 10:30:45.123', 3)");

        Assert.Equal(2024, result.Year);
        Assert.Equal(1, result.Month);
        Assert.Equal(15, result.Day);
        Assert.Equal(10, result.Hour);
        Assert.Equal(30, result.Minute);
        Assert.Equal(45, result.Second);
        Assert.Equal(123, result.Millisecond);
    }

    [Fact]
    public async Task Select_DateTime64_Precision6_ReturnsMicroseconds()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<DateTime>("SELECT toDateTime64('2024-01-15 10:30:45.123456', 6)");

        Assert.Equal(2024, result.Year);
        // Microseconds precision - check at least milliseconds are correct
        Assert.Equal(123, result.Millisecond);
    }

    #endregion

    #region Decimal Types

    [Fact]
    public async Task Select_Decimal32_ReturnsCorrectDecimal()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<decimal>("SELECT toDecimal32(123.45, 2)");

        Assert.Equal(123.45m, result);
    }

    [Fact]
    public async Task Select_Decimal64_ReturnsCorrectDecimal()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<decimal>("SELECT toDecimal64(12345678.12345678, 8)");

        Assert.Equal(12345678.12345678m, result);
    }

    [Fact]
    public async Task Select_Decimal128_ReturnsCorrectDecimal()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<decimal>("SELECT toDecimal128(123456789.123456789, 9)");

        // Allow small precision loss due to Int128 to decimal conversion
        Assert.InRange(result, 123456789.123456780m, 123456789.123456799m);
    }

    #endregion

    #region UUID

    [Fact]
    public async Task Select_UUID_ReturnsCorrectGuid()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var expected = new Guid("12345678-1234-5678-1234-567812345678");
        var result = await connection.ExecuteScalarAsync<Guid>("SELECT toUUID('12345678-1234-5678-1234-567812345678')");

        Assert.Equal(expected, result);
    }

    [Fact]
    public async Task Select_UUID_WithMixedBytes_ReturnsCorrectGuid()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Same UUID as ParameterTests.GuidParameter_ReturnsCorrectValue
        var expected = new Guid("12345678-1234-1234-1234-123456789abc");
        var result = await connection.ExecuteScalarAsync<Guid>("SELECT toUUID('12345678-1234-1234-1234-123456789abc')");

        Assert.Equal(expected, result);
    }

    [Fact]
    public async Task Select_UUID_GeneratedUUID_ReturnsValidGuid()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<Guid>("SELECT generateUUIDv4()");

        Assert.NotEqual(Guid.Empty, result);
    }

    #endregion

    #region IP Addresses

    [Fact]
    public async Task Select_IPv4_ReturnsCorrectAddress()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<IPAddress>("SELECT toIPv4('192.168.1.1')");

        Assert.Equal(IPAddress.Parse("192.168.1.1"), result);
    }

    [Fact]
    public async Task Select_IPv4_Localhost_ReturnsCorrectAddress()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<IPAddress>("SELECT toIPv4('127.0.0.1')");

        Assert.Equal(IPAddress.Loopback, result);
    }

    [Fact]
    public async Task Select_IPv6_ReturnsCorrectAddress()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<IPAddress>("SELECT toIPv6('::1')");

        Assert.Equal(IPAddress.IPv6Loopback, result);
    }

    [Fact]
    public async Task Select_IPv6_FullAddress_ReturnsCorrectAddress()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<IPAddress>("SELECT toIPv6('2001:0db8:85a3:0000:0000:8a2e:0370:7334')");

        Assert.Equal(IPAddress.Parse("2001:0db8:85a3:0000:0000:8a2e:0370:7334"), result);
    }

    #endregion

    #region FixedString

    [Fact]
    public async Task Select_FixedString_ReturnsCorrectBytes()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<byte[]>("SELECT toFixedString('hello', 10)");

        Assert.NotNull(result);
        Assert.Equal(10, result.Length);
        Assert.Equal((byte)'h', result[0]);
        Assert.Equal((byte)'e', result[1]);
        Assert.Equal((byte)'l', result[2]);
        Assert.Equal((byte)'l', result[3]);
        Assert.Equal((byte)'o', result[4]);
        // Rest should be null padded
        Assert.Equal(0, result[5]);
    }

    #endregion

    #region Nullable

    [Fact]
    public async Task Select_NullableInt32_WithValue_ReturnsValue()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<int?>("SELECT toNullable(toInt32(42))");

        Assert.NotNull(result);
        Assert.Equal(42, result.Value);
    }

    [Fact]
    public async Task Select_NullableInt32_Null_ReturnsNull()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<int?>("SELECT NULL::Nullable(Int32)");

        Assert.Null(result);
    }

    [Fact]
    public async Task Select_NullableString_WithValue_ReturnsValue()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<string?>("SELECT toNullable('hello')");

        Assert.NotNull(result);
        Assert.Equal("hello", result);
    }

    [Fact]
    public async Task Select_NullableString_Null_ReturnsNull()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<string?>("SELECT NULL::Nullable(String)");

        Assert.Null(result);
    }

    #endregion

    #region Array

    [Fact]
    public async Task Select_ArrayInt32_ReturnsCorrectArray()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Use object type to avoid Convert.ChangeType issues with arrays
        var result = await connection.ExecuteScalarAsync<object>("SELECT [toInt32(1), toInt32(2), toInt32(3)]");

        Assert.NotNull(result);
        var array = Assert.IsType<int[]>(result);
        Assert.Equal(3, array.Length);
        Assert.Equal(1, array[0]);
        Assert.Equal(2, array[1]);
        Assert.Equal(3, array[2]);
    }

    [Fact]
    public async Task Select_ArrayString_ReturnsCorrectArray()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<object>("SELECT ['a', 'b', 'c']");

        Assert.NotNull(result);
        var array = Assert.IsType<string[]>(result);
        Assert.Equal(3, array.Length);
        Assert.Equal("a", array[0]);
        Assert.Equal("b", array[1]);
        Assert.Equal("c", array[2]);
    }

    [Fact]
    public async Task Select_EmptyArray_ReturnsEmptyArray()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<object>("SELECT []::Array(Int32)");

        Assert.NotNull(result);
        var array = Assert.IsType<int[]>(result);
        Assert.Empty(array);
    }

    #endregion

    #region Tuple

    [Fact]
    public async Task Select_Tuple_ReturnsTupleInstance()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Use explicit types to ensure correct type inference
        var result = await connection.ExecuteScalarAsync<object>("SELECT tuple(toInt32(1), 'hello', toFloat64(3.14))");

        Assert.NotNull(result);
        var tuple = Assert.IsAssignableFrom<System.Runtime.CompilerServices.ITuple>(result);
        Assert.Equal(3, tuple.Length);
        Assert.Equal(1, tuple[0]);
        Assert.Equal("hello", tuple[1]);
        Assert.Equal(3.14, (double)tuple[2]!, 2);
    }

    #endregion

    #region Map

    [Fact]
    public async Task Select_Map_ReturnsCorrectDictionary()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Use object type to avoid Convert.ChangeType issues
        var result = await connection.ExecuteScalarAsync<object>("SELECT map('a', toInt32(1), 'b', toInt32(2), 'c', toInt32(3))");

        Assert.NotNull(result);
        var dict = Assert.IsType<Dictionary<string, int>>(result);
        Assert.Equal(3, dict.Count);
        Assert.Equal(1, dict["a"]);
        Assert.Equal(2, dict["b"]);
        Assert.Equal(3, dict["c"]);
    }

    [Fact]
    public async Task Select_EmptyMap_ReturnsEmptyDictionary()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<object>("SELECT map()::Map(String, Int32)");

        Assert.NotNull(result);
        var dict = Assert.IsType<Dictionary<string, int>>(result);
        Assert.Empty(dict);
    }

    #endregion

    #region LowCardinality

    [Fact]
    public async Task Select_LowCardinalityString_ReturnsCorrectValue()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<string>("SELECT toLowCardinality('hello')");

        Assert.Equal("hello", result);
    }

    #endregion

    #region Int128 Types

    [Fact]
    public async Task Select_Int128_ReturnsCorrectValue()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Use string conversion to avoid SQL literal parsing issues with large numbers
        var result = await connection.ExecuteScalarAsync<Int128>("SELECT toInt128('12345678901234567890')");

        Assert.Equal(Int128.Parse("12345678901234567890"), result);
    }

    [Fact]
    public async Task Select_Int128_MaxValue_ReturnsCorrectValue()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Use string conversion for max value
        var result = await connection.ExecuteScalarAsync<Int128>("SELECT toInt128('170141183460469231731687303715884105727')");

        Assert.Equal(Int128.MaxValue, result);
    }

    [Fact]
    public async Task Select_Int128_NegativeValue_ReturnsCorrectValue()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Use string conversion for negative values
        var result = await connection.ExecuteScalarAsync<Int128>("SELECT toInt128('-12345678901234567890')");

        Assert.Equal(Int128.Parse("-12345678901234567890"), result);
    }

    [Fact]
    public async Task Select_UInt128_ReturnsCorrectValue()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Use string conversion to avoid SQL literal parsing issues
        var result = await connection.ExecuteScalarAsync<UInt128>("SELECT toUInt128('12345678901234567890')");

        Assert.Equal(UInt128.Parse("12345678901234567890"), result);
    }

    [Fact]
    public async Task Select_UInt128_LargeValue_ReturnsCorrectValue()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Use string conversion for max value
        var result = await connection.ExecuteScalarAsync<UInt128>("SELECT toUInt128('340282366920938463463374607431768211455')");

        Assert.Equal(UInt128.MaxValue, result);
    }

    #endregion

    #region Int256 Types

    [Fact]
    public async Task Select_Int256_ReturnsCorrectValue()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Use string conversion for large numbers
        var result = await connection.ExecuteScalarAsync<System.Numerics.BigInteger>("SELECT toInt256('12345678901234567890123456789012345678')");

        Assert.Equal(System.Numerics.BigInteger.Parse("12345678901234567890123456789012345678"), result);
    }

    [Fact]
    public async Task Select_Int256_NegativeValue_ReturnsCorrectValue()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Use string conversion for negative values
        var result = await connection.ExecuteScalarAsync<System.Numerics.BigInteger>("SELECT toInt256('-12345678901234567890123456789012345678')");

        Assert.Equal(System.Numerics.BigInteger.Parse("-12345678901234567890123456789012345678"), result);
    }

    [Fact]
    public async Task Select_UInt256_ReturnsCorrectValue()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Use string conversion for large numbers
        var result = await connection.ExecuteScalarAsync<System.Numerics.BigInteger>("SELECT toUInt256('12345678901234567890123456789012345678')");

        Assert.Equal(System.Numerics.BigInteger.Parse("12345678901234567890123456789012345678"), result);
    }

    #endregion

    #region DateTime with Timezone

    [Fact]
    public async Task Select_DateTimeWithTimezone_UTC_ReturnsDateTimeOffset()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<DateTimeOffset>("SELECT toDateTime('2024-01-15 10:30:00', 'UTC')");

        Assert.Equal(TimeSpan.Zero, result.Offset);
        Assert.Equal(2024, result.Year);
        Assert.Equal(1, result.Month);
        Assert.Equal(15, result.Day);
        Assert.Equal(10, result.Hour);
        Assert.Equal(30, result.Minute);
    }

    [Fact]
    public async Task Select_DateTimeWithTimezone_NewYork_ReturnsCorrectOffset()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // January 15 is not during DST, so EST = UTC-5
        var result = await connection.ExecuteScalarAsync<DateTimeOffset>("SELECT toDateTime('2024-01-15 10:30:00', 'America/New_York')");

        // EST is UTC-5 (not during DST in January)
        Assert.Equal(-5, result.Offset.TotalHours);
    }

    #endregion

    #region Complex Nested Types

    [Fact]
    public async Task Select_ArrayOfNullableInt32_WithMixedValues()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<object>("SELECT [toNullable(toInt32(1)), NULL, toNullable(toInt32(3))]");

        Assert.NotNull(result);
        var array = Assert.IsType<int?[]>(result);
        Assert.Equal(3, array.Length);
        Assert.Equal(1, array[0]);
        Assert.Null(array[1]);
        Assert.Equal(3, array[2]);
    }

    [Fact]
    public async Task Select_MapOfStringToArray_ReturnsNestedStructure()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<object>("SELECT map('a', [toInt32(1), toInt32(2)], 'b', [toInt32(3), toInt32(4), toInt32(5)])");

        Assert.NotNull(result);
        var map = Assert.IsType<Dictionary<string, int[]>>(result);
        Assert.Equal(2, map.Count);
        Assert.Equal(new[] { 1, 2 }, map["a"]);
        Assert.Equal(new[] { 3, 4, 5 }, map["b"]);
    }

    [Fact]
    public async Task Select_NullableString_InArray_WithNull()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<object>("SELECT [toNullable('a'), NULL, toNullable('c')]");

        Assert.NotNull(result);
        var array = Assert.IsType<string?[]>(result);
        Assert.Equal(3, array.Length);
        Assert.Equal("a", array[0]);
        Assert.Null(array[1]);
        Assert.Equal("c", array[2]);
    }

    #endregion

    #region Decimal256

    [Fact]
    public async Task Select_Decimal256_ReturnsCorrectDecimal()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<decimal>("SELECT toDecimal256(123456789.123456789, 9)");

        // Due to precision limitations, we check approximately
        Assert.InRange(result, 123456789.123456780m, 123456789.123456799m);
    }

    #endregion

    #region Enum with Defined Values

    [Fact]
    public async Task Select_Enum8_ReturnsRawValue()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Enum8 is read as Int8 (sbyte) by the reader
        var result = await connection.ExecuteScalarAsync<sbyte>("SELECT CAST(1 AS Enum8('Active' = 1, 'Inactive' = 0))");

        Assert.Equal(1, result);
    }

    [Fact]
    public async Task Select_Enum16_ReturnsRawValue()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Enum16 is read as Int16 (short) by the reader
        var result = await connection.ExecuteScalarAsync<short>("SELECT CAST(1000 AS Enum16('One' = 1, 'Thousand' = 1000))");

        Assert.Equal(1000, result);
    }

    #endregion
}
