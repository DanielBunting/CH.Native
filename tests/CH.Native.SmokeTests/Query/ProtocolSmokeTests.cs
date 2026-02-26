using CH.Native.Ado;
using CH.Native.Connection;
using CH.Native.SmokeTests.Fixtures;
using CH.Native.SmokeTests.Helpers;
using Xunit;

namespace CH.Native.SmokeTests.Query;

[Collection("SmokeTest")]
public class ProtocolSmokeTests
{
    private readonly SmokeTestFixture _fixture;

    public ProtocolSmokeTests(SmokeTestFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task MultiBlockResult_100KRows()
    {
        var table = $"smoke_protocol_{Guid.NewGuid():N}";
        try
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $@"CREATE TABLE {table} (
                    id UInt64,
                    value String
                ) ENGINE = Memory");

            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $@"INSERT INTO {table}
                   SELECT number, concat('row_', toString(number))
                   FROM numbers(100000)");

            // Compare counts
            var nativeCount = await NativeQueryHelper.ExecuteScalarAsync<ulong>(
                _fixture.NativeConnectionString,
                $"SELECT count() FROM {table}");
            var driverCount = await DriverQueryHelper.ExecuteScalarAsync(
                _fixture.DriverConnectionString,
                $"SELECT count() FROM {table}");

            Assert.Equal(100000UL, nativeCount);
            Assert.Equal(100000UL, Convert.ToUInt64(driverCount));

            // Compare checksum of all data
            var nativeChecksum = await NativeQueryHelper.QueryAsync(
                _fixture.NativeConnectionString,
                $"SELECT sum(sipHash64(id, value)) FROM {table}");
            var driverChecksum = await DriverQueryHelper.QueryAsync(
                _fixture.DriverConnectionString,
                $"SELECT sum(sipHash64(id, value)) FROM {table}");
            ResultComparer.AssertResultsEqual(nativeChecksum, driverChecksum, "100K rows checksum");

            // Spot-check first, middle, and last rows
            var native = await NativeQueryHelper.QueryAsync(
                _fixture.NativeConnectionString,
                $"SELECT id, value FROM {table} WHERE id IN (0, 50000, 99999) ORDER BY id");
            var driver = await DriverQueryHelper.QueryAsync(
                _fixture.DriverConnectionString,
                $"SELECT id, value FROM {table} WHERE id IN (0, 50000, 99999) ORDER BY id");

            ResultComparer.AssertResultsEqual(native, driver, "100K rows spot-check");
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task EmptyResultSet()
    {
        var table = $"smoke_protocol_{Guid.NewGuid():N}";
        try
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"CREATE TABLE {table} (id Int32, val String) ENGINE = Memory");

            var native = await NativeQueryHelper.QueryAsync(
                _fixture.NativeConnectionString,
                $"SELECT * FROM {table}");
            var driver = await DriverQueryHelper.QueryAsync(
                _fixture.DriverConnectionString,
                $"SELECT * FROM {table}");

            Assert.Empty(native);
            Assert.Empty(driver);
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task ServerVersion_IsNonEmpty()
    {
        // Native driver: use ADO.NET ServerVersion property
        await using var nativeConn = new ClickHouseDbConnection(_fixture.NativeConnectionString);
        await nativeConn.OpenAsync();
        var nativeVersion = nativeConn.ServerVersion;

        // Reference driver: use SELECT version() since ServerVersion property was removed
        var driverVersion = (string?)await DriverQueryHelper.ExecuteScalarAsync(
            _fixture.DriverConnectionString, "SELECT version()");

        Assert.NotNull(nativeVersion);
        Assert.NotEmpty(nativeVersion);
        Assert.NotNull(driverVersion);
        Assert.NotEmpty(driverVersion);

        // Both should contain version-like content (digits and dots)
        Assert.Contains(".", nativeVersion);
        Assert.Contains(".", driverVersion);
    }

    [Fact]
    public async Task LargeColumnCount()
    {
        // Generate a query with many columns
        var cols = string.Join(", ", Enumerable.Range(1, 100).Select(i => $"number + {i} AS col_{i}"));

        var native = await NativeQueryHelper.QueryAsync(
            _fixture.NativeConnectionString,
            $"SELECT {cols} FROM numbers(5)");
        var driver = await DriverQueryHelper.QueryAsync(
            _fixture.DriverConnectionString,
            $"SELECT {cols} FROM numbers(5)");

        ResultComparer.AssertResultsEqual(native, driver, "100 columns");
    }

    [Fact]
    public async Task SystemTables_Query()
    {
        // Query a real system table that both drivers must handle
        var native = await NativeQueryHelper.QueryAsync(
            _fixture.NativeConnectionString,
            "SELECT name, engine FROM system.databases ORDER BY name");
        var driver = await DriverQueryHelper.QueryAsync(
            _fixture.DriverConnectionString,
            "SELECT name, engine FROM system.databases ORDER BY name");

        ResultComparer.AssertResultsEqual(native, driver, "system.databases");
    }
}
