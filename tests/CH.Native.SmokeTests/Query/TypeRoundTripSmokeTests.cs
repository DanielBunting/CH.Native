using CH.Native.SmokeTests.Fixtures;
using CH.Native.SmokeTests.Helpers;
using Xunit;

namespace CH.Native.SmokeTests.Query;

[Collection("SmokeTest")]
public class TypeRoundTripSmokeTests
{
    private readonly SmokeTestFixture _fixture;

    public TypeRoundTripSmokeTests(SmokeTestFixture fixture)
    {
        _fixture = fixture;
    }

    private async Task RunTypeTest(string columnDef, string insertValues, string selectExpr = "*")
    {
        var table = $"smoke_type_{Guid.NewGuid():N}";
        try
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"CREATE TABLE {table} ({columnDef}) ENGINE = Memory");

            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"INSERT INTO {table} VALUES {insertValues}");

            var native = await NativeQueryHelper.QueryAsync(
                _fixture.NativeConnectionString,
                $"SELECT {selectExpr} FROM {table} ORDER BY 1");

            var driver = await DriverQueryHelper.QueryAsync(
                _fixture.DriverConnectionString,
                $"SELECT {selectExpr} FROM {table} ORDER BY 1");

            ResultComparer.AssertResultsEqual(native, driver, $"Type: {columnDef}");
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public Task Bool() =>
        RunTypeTest("val Bool", "(true),(false)");

    [Fact]
    public Task Int8() =>
        RunTypeTest("val Int8", "(-128),(0),(127)");

    [Fact]
    public Task Int16() =>
        RunTypeTest("val Int16", "(-32768),(0),(32767)");

    [Fact]
    public Task Int32() =>
        RunTypeTest("val Int32", "(-2147483648),(0),(2147483647)");

    [Fact]
    public Task Int64() =>
        RunTypeTest("val Int64", "(-9223372036854775808),(0),(9223372036854775807)");

    [Fact]
    public Task Int128() =>
        RunTypeTest("val Int128", "(0),(170141183460469231731687303715884105727),(-170141183460469231731687303715884105727)");

    [Fact]
    public Task Int256() =>
        RunTypeTest("val Int256", "(0),(12345678901234567890123456789012345678)");

    [Fact]
    public Task UInt128() =>
        RunTypeTest("val UInt128", "(0),(340282366920938463463374607431768211455)");

    [Fact]
    public Task UInt256() =>
        RunTypeTest("val UInt256", "(0),(12345678901234567890123456789012345678)");

    [Fact]
    public Task Float32() =>
        RunTypeTest("val Float32", "(1.5),(0),(-1.5)");

    [Fact]
    public Task Float64() =>
        RunTypeTest("val Float64", "(3.14159265358979),(0),(-2.71828)");

    [Fact]
    public Task Decimal32() =>
        RunTypeTest("val Decimal32(4)", "(99.9999),(0),(-99.9999)");

    [Fact]
    public Task Decimal64() =>
        RunTypeTest("val Decimal64(8)", "(123456.12345678),(0),(-999999.99999999)");

    [Fact]
    public Task Decimal128() =>
        RunTypeTest("val Decimal128(18)", "(12345678901234567.890123456789012345),(0)");

    [Fact]
    public Task Decimal256() =>
        RunTypeTest("val Decimal256(30)", "(123456789.012345678901234567890123456789),(0)");

    [Fact]
    public Task FixedString() =>
        RunTypeTest("val FixedString(8)", "('hello'),('test1234'),('')");

    [Fact]
    public async Task Enum8()
    {
        // Query with toString() to normalize enum representation across drivers
        await RunTypeTest(
            "val Enum8('a' = 1, 'b' = 2, 'c' = 3)",
            "('a'),('b'),('c')",
            selectExpr: "toString(val)");
    }

    [Fact]
    public async Task Enum16()
    {
        await RunTypeTest(
            "val Enum16('alpha' = 1000, 'beta' = 2000, 'gamma' = 3000)",
            "('alpha'),('beta'),('gamma')",
            selectExpr: "toString(val)");
    }

    [Fact]
    public Task IPv4() =>
        RunTypeTest("val IPv4", "('192.168.1.1'),('10.0.0.1'),('255.255.255.255')");

    [Fact]
    public Task IPv6() =>
        RunTypeTest("val IPv6", "('::1'),('2001:db8::1'),('fe80::1')");

    [Fact]
    public Task Date() =>
        RunTypeTest("val Date", "('2024-01-01'),('2000-06-15'),('1970-01-01')");

    [Fact]
    public Task Date32() =>
        RunTypeTest("val Date32", "('2024-01-01'),('1900-01-01'),('2299-12-31')");

    [Fact]
    public Task DateTime() =>
        RunTypeTest("val DateTime('UTC')", "('2024-01-01 12:30:45'),('1970-01-01 00:00:00'),('2106-02-07 06:28:15')");

    [Fact]
    public Task DateTime64() =>
        RunTypeTest("val DateTime64(3, 'UTC')", "('2024-01-01 12:30:45.123'),('1970-01-01 00:00:00.000')");

    [Fact]
    public Task String() =>
        RunTypeTest("val String", "('hello world'),(''),('test with spaces')");

    [Fact]
    public Task UUID() =>
        RunTypeTest("val UUID", "('550e8400-e29b-41d4-a716-446655440000'),('00000000-0000-0000-0000-000000000000')");

    [Fact]
    public Task NullableBool() =>
        RunTypeTest("val Nullable(Bool)", "(true),(false),(NULL)");

    [Fact]
    public Task NullableInt32() =>
        RunTypeTest("val Nullable(Int32)", "(42),(NULL),(-1)");

    [Fact]
    public Task NullableString() =>
        RunTypeTest("val Nullable(String)", "('hello'),(NULL),('')");

    [Fact]
    public Task NullableDecimal64() =>
        RunTypeTest("val Nullable(Decimal64(4))", "(123.4567),(NULL),(0)");
}
