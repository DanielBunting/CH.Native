using CH.Native.SmokeTests.Fixtures;
using CH.Native.SmokeTests.Helpers;
using Xunit;

namespace CH.Native.SmokeTests.Query;

[Collection("SmokeTest")]
public class BoundaryValueSmokeTests
{
    private readonly SmokeTestFixture _fixture;

    public BoundaryValueSmokeTests(SmokeTestFixture fixture)
    {
        _fixture = fixture;
    }

    private async Task RunBoundaryTest(string columnDef, string insertValues, string selectExpr = "*")
    {
        var table = $"smoke_boundary_{Guid.NewGuid():N}";
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
                $"SELECT {selectExpr} FROM {table}");

            var driver = await DriverQueryHelper.QueryAsync(
                _fixture.DriverConnectionString,
                $"SELECT {selectExpr} FROM {table}");

            ResultComparer.AssertResultsEqual(native, driver, $"Boundary: {columnDef}");
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task Float64_NaN()
    {
        // Use SELECT with literal since NaN can't be inserted directly via VALUES
        var native = await NativeQueryHelper.QueryAsync(
            _fixture.NativeConnectionString, "SELECT nan::Float64");
        var driver = await DriverQueryHelper.QueryAsync(
            _fixture.DriverConnectionString, "SELECT nan::Float64");
        ResultComparer.AssertResultsEqual(native, driver, "Float64 NaN");
    }

    [Fact]
    public async Task Float64_PositiveInfinity()
    {
        var native = await NativeQueryHelper.QueryAsync(
            _fixture.NativeConnectionString, "SELECT inf::Float64");
        var driver = await DriverQueryHelper.QueryAsync(
            _fixture.DriverConnectionString, "SELECT inf::Float64");
        ResultComparer.AssertResultsEqual(native, driver, "Float64 +Inf");
    }

    [Fact]
    public async Task Float64_NegativeInfinity()
    {
        var native = await NativeQueryHelper.QueryAsync(
            _fixture.NativeConnectionString, "SELECT -inf::Float64");
        var driver = await DriverQueryHelper.QueryAsync(
            _fixture.DriverConnectionString, "SELECT -inf::Float64");
        ResultComparer.AssertResultsEqual(native, driver, "Float64 -Inf");
    }

    [Fact]
    public async Task Float64_NegativeZero()
    {
        var native = await NativeQueryHelper.QueryAsync(
            _fixture.NativeConnectionString, "SELECT -0.0::Float64");
        var driver = await DriverQueryHelper.QueryAsync(
            _fixture.DriverConnectionString, "SELECT -0.0::Float64");
        ResultComparer.AssertResultsEqual(native, driver, "Float64 -0.0");
    }

    [Fact]
    public async Task Float64_Subnormal()
    {
        // Smallest positive subnormal Float64
        var native = await NativeQueryHelper.QueryAsync(
            _fixture.NativeConnectionString, "SELECT 5e-324::Float64");
        var driver = await DriverQueryHelper.QueryAsync(
            _fixture.DriverConnectionString, "SELECT 5e-324::Float64");
        ResultComparer.AssertResultsEqual(native, driver, "Float64 subnormal");
    }

    [Fact]
    public async Task Float64_MaxMin()
    {
        var native = await NativeQueryHelper.QueryAsync(
            _fixture.NativeConnectionString, "SELECT 1.7976931348623157e308::Float64, -1.7976931348623157e308::Float64");
        var driver = await DriverQueryHelper.QueryAsync(
            _fixture.DriverConnectionString, "SELECT 1.7976931348623157e308::Float64, -1.7976931348623157e308::Float64");
        ResultComparer.AssertResultsEqual(native, driver, "Float64 max/min");
    }

    [Fact]
    public async Task LargeString()
    {
        var table = $"smoke_boundary_{Guid.NewGuid():N}";
        try
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"CREATE TABLE {table} (val String) ENGINE = Memory");

            // 1MB string
            var largeStr = new string('A', 1_000_000);
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"INSERT INTO {table} VALUES ('{largeStr}')");

            // Compare lengths instead of full content for performance
            var nativeLen = await NativeQueryHelper.QueryAsync(
                _fixture.NativeConnectionString,
                $"SELECT length(val) FROM {table}");
            var driverLen = await DriverQueryHelper.QueryAsync(
                _fixture.DriverConnectionString,
                $"SELECT length(val) FROM {table}");

            ResultComparer.AssertResultsEqual(nativeLen, driverLen, "Large string length");

            // Also verify a hash to ensure content matches
            var nativeHash = await NativeQueryHelper.QueryAsync(
                _fixture.NativeConnectionString,
                $"SELECT sipHash64(val) FROM {table}");
            var driverHash = await DriverQueryHelper.QueryAsync(
                _fixture.DriverConnectionString,
                $"SELECT sipHash64(val) FROM {table}");

            ResultComparer.AssertResultsEqual(nativeHash, driverHash, "Large string hash");
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task StringWithNullBytes()
    {
        // ClickHouse strings can contain null bytes
        var native = await NativeQueryHelper.QueryAsync(
            _fixture.NativeConnectionString,
            "SELECT 'hello\\0world'");
        var driver = await DriverQueryHelper.QueryAsync(
            _fixture.DriverConnectionString,
            "SELECT 'hello\\0world'");
        ResultComparer.AssertResultsEqual(native, driver, "Null bytes in string");
    }

    [Fact]
    public async Task UnicodeStrings_CJK()
    {
        await RunBoundaryTest("val String",
            @"('你好世界'),('こんにちは'),('안녕하세요')");
    }

    [Fact]
    public async Task UnicodeStrings_Emoji()
    {
        await RunBoundaryTest("val String",
            @"('🎉🎊🎈'),('👨‍👩‍👧‍👦'),('🏳️‍🌈')");
    }

    [Fact]
    public Task DateTime_Epoch() =>
        RunBoundaryTest("val DateTime('UTC')", "('1970-01-01 00:00:00')");

    [Fact]
    public Task DateTime_MaxValue() =>
        RunBoundaryTest("val DateTime('UTC')", "('2106-02-07 06:28:15')");

    [Fact]
    public Task DateTime64_SubMillisecond() =>
        RunBoundaryTest("val DateTime64(6, 'UTC')", "('2024-01-15 12:30:45.123456')");

    [Fact]
    public Task DateTime64_Nanosecond() =>
        RunBoundaryTest("val DateTime64(9, 'UTC')", "('2024-01-15 12:30:45.123456789')");

    [Fact]
    public Task Date_MinValue() =>
        RunBoundaryTest("val Date", "('1970-01-01')");

    [Fact]
    public Task Date_MaxValue() =>
        RunBoundaryTest("val Date", "('2149-06-06')");

    [Fact]
    public Task Date32_ExtendedRange() =>
        RunBoundaryTest("val Date32", "('1900-01-01'),('2299-12-31')");

    [Fact]
    public Task UUID_AllZeros() =>
        RunBoundaryTest("val UUID", "('00000000-0000-0000-0000-000000000000')");

    [Fact]
    public Task UUID_AllOnes() =>
        RunBoundaryTest("val UUID", "('ffffffff-ffff-ffff-ffff-ffffffffffff')");
}
