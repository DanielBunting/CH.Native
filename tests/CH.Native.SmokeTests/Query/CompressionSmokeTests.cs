using CH.Native.SmokeTests.Fixtures;
using CH.Native.SmokeTests.Helpers;
using Xunit;

namespace CH.Native.SmokeTests.Query;

[Collection("SmokeTest")]
public class CompressionSmokeTests
{
    private readonly SmokeTestFixture _fixture;

    public CompressionSmokeTests(SmokeTestFixture fixture)
    {
        _fixture = fixture;
    }

    [Theory]
    [InlineData("lz4")]
    [InlineData("zstd")]
    public async Task BasicTypes_WithCompression(string method)
    {
        var table = $"smoke_compress_{Guid.NewGuid():N}";
        try
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $@"CREATE TABLE {table} (
                    id Int32,
                    name String,
                    value Float64
                ) ENGINE = Memory");

            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"INSERT INTO {table} VALUES (1, 'hello', 3.14), (2, 'world', 2.71), (3, 'test', 1.41)");

            var connStr = _fixture.NativeConnectionStringWithCompression(method);
            var native = await NativeQueryHelper.QueryAsync(
                connStr,
                $"SELECT * FROM {table} ORDER BY id");

            var driver = await DriverQueryHelper.QueryAsync(
                _fixture.DriverConnectionString,
                $"SELECT * FROM {table} ORDER BY id");

            ResultComparer.AssertResultsEqual(native, driver, $"Compression {method}: basic types");
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Theory]
    [InlineData("lz4")]
    [InlineData("zstd")]
    public async Task LargeDataset_WithCompression(string method)
    {
        var table = $"smoke_compress_{Guid.NewGuid():N}";
        try
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $@"CREATE TABLE {table} (
                    id UInt32,
                    value String
                ) ENGINE = Memory");

            // Insert 10K rows via SELECT
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $@"INSERT INTO {table}
                   SELECT number, concat('value_', toString(number))
                   FROM numbers(10000)");

            var connStr = _fixture.NativeConnectionStringWithCompression(method);

            // Compare count
            var nativeCount = await NativeQueryHelper.ExecuteScalarAsync<ulong>(
                connStr, $"SELECT count() FROM {table}");
            var driverCount = await DriverQueryHelper.ExecuteScalarAsync(
                _fixture.DriverConnectionString, $"SELECT count() FROM {table}");
            Assert.Equal(10000UL, nativeCount);
            Assert.Equal(10000UL, Convert.ToUInt64(driverCount));

            // Compare a sample of rows
            var native = await NativeQueryHelper.QueryAsync(
                connStr,
                $"SELECT * FROM {table} WHERE id IN (0, 5000, 9999) ORDER BY id");

            var driver = await DriverQueryHelper.QueryAsync(
                _fixture.DriverConnectionString,
                $"SELECT * FROM {table} WHERE id IN (0, 5000, 9999) ORDER BY id");

            ResultComparer.AssertResultsEqual(native, driver, $"Compression {method}: large dataset");
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Theory]
    [InlineData("lz4")]
    [InlineData("zstd")]
    public async Task MixedColumnTypes_WithCompression(string method)
    {
        var table = $"smoke_compress_{Guid.NewGuid():N}";
        try
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $@"CREATE TABLE {table} (
                    id UInt32,
                    name LowCardinality(String),
                    tags Array(String),
                    metadata Map(String, String),
                    nullable_val Nullable(Int64),
                    dt DateTime('UTC')
                ) ENGINE = Memory");

            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $@"INSERT INTO {table} VALUES
                   (1, 'type_a', ['tag1', 'tag2'], {{'key1': 'val1'}}, 42, '2024-01-01 00:00:00'),
                   (2, 'type_b', [], {{}}, NULL, '2024-06-15 12:30:00'),
                   (3, 'type_a', ['tag3'], {{'key2': 'val2', 'key3': 'val3'}}, -1, '2024-12-31 23:59:59')");

            var connStr = _fixture.NativeConnectionStringWithCompression(method);
            var native = await NativeQueryHelper.QueryAsync(
                connStr,
                $"SELECT * FROM {table} ORDER BY id");

            var driver = await DriverQueryHelper.QueryAsync(
                _fixture.DriverConnectionString,
                $"SELECT * FROM {table} ORDER BY id");

            ResultComparer.AssertResultsEqual(native, driver, $"Compression {method}: mixed types");
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task NoCompression_MatchesDriver()
    {
        // Verify uncompressed CH.Native also matches
        var table = $"smoke_compress_{Guid.NewGuid():N}";
        try
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"CREATE TABLE {table} (id Int32, val String) ENGINE = Memory");

            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"INSERT INTO {table} VALUES (1, 'no compression'), (2, 'test')");

            // Default connection string (no compression)
            var native = await NativeQueryHelper.QueryAsync(
                _fixture.NativeConnectionString,
                $"SELECT * FROM {table} ORDER BY id");

            var driver = await DriverQueryHelper.QueryAsync(
                _fixture.DriverConnectionString,
                $"SELECT * FROM {table} ORDER BY id");

            ResultComparer.AssertResultsEqual(native, driver, "No compression");
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }
}
