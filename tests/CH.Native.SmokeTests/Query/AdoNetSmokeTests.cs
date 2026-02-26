using System.Data;
using System.Data.Common;
using CH.Native.Ado;
using CH.Native.SmokeTests.Fixtures;
using CH.Native.SmokeTests.Helpers;
using Dapper;
using Xunit;
using DriverConnection = ClickHouse.Driver.ADO.ClickHouseConnection;

namespace CH.Native.SmokeTests.Query;

[Collection("SmokeTest")]
public class AdoNetSmokeTests
{
    private readonly SmokeTestFixture _fixture;

    public AdoNetSmokeTests(SmokeTestFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task ExecuteScalar_ReturnsMatchingResult()
    {
        await using var nativeConn = new ClickHouseDbConnection(_fixture.NativeConnectionString);
        await nativeConn.OpenAsync();
        using var nativeCmd = nativeConn.CreateCommand();
        nativeCmd.CommandText = "SELECT 42";
        var nativeResult = await nativeCmd.ExecuteScalarAsync();

        using var driverConn = new DriverConnection(_fixture.DriverConnectionString);
        await driverConn.OpenAsync();
        using var driverCmd = driverConn.CreateCommand();
        driverCmd.CommandText = "SELECT 42";
        var driverResult = await driverCmd.ExecuteScalarAsync();

        ResultComparer.AssertValuesEqual(nativeResult, driverResult, "ExecuteScalar SELECT 42");
    }

    [Fact]
    public async Task ExecuteScalar_StringResult()
    {
        await using var nativeConn = new ClickHouseDbConnection(_fixture.NativeConnectionString);
        await nativeConn.OpenAsync();
        using var nativeCmd = nativeConn.CreateCommand();
        nativeCmd.CommandText = "SELECT 'hello world'";
        var nativeResult = await nativeCmd.ExecuteScalarAsync();

        using var driverConn = new DriverConnection(_fixture.DriverConnectionString);
        await driverConn.OpenAsync();
        using var driverCmd = driverConn.CreateCommand();
        driverCmd.CommandText = "SELECT 'hello world'";
        var driverResult = await driverCmd.ExecuteScalarAsync();

        ResultComparer.AssertValuesEqual(nativeResult, driverResult, "ExecuteScalar string");
    }

    [Fact]
    public async Task DbDataReader_DBNullHandling()
    {
        var table = $"smoke_ado_{Guid.NewGuid():N}";
        try
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"CREATE TABLE {table} (id Int32, val Nullable(String)) ENGINE = Memory");

            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"INSERT INTO {table} VALUES (1, 'hello'), (2, NULL), (3, 'world')");

            // Read via native ADO.NET
            var nativeResults = new List<(int id, bool isNull, string? val)>();
            await using var nativeConn = new ClickHouseDbConnection(_fixture.NativeConnectionString);
            await nativeConn.OpenAsync();
            using var nativeCmd = nativeConn.CreateCommand();
            nativeCmd.CommandText = $"SELECT id, val FROM {table} ORDER BY id";
            using var nativeReader = await nativeCmd.ExecuteReaderAsync();
            while (await nativeReader.ReadAsync())
            {
                var id = nativeReader.GetInt32(0);
                var isNull = nativeReader.IsDBNull(1);
                var val = isNull ? null : nativeReader.GetString(1);
                nativeResults.Add((id, isNull, val));
            }

            // Read via driver ADO.NET
            var driverResults = new List<(int id, bool isNull, string? val)>();
            using var driverConn = new DriverConnection(_fixture.DriverConnectionString);
            await driverConn.OpenAsync();
            using var driverCmd = driverConn.CreateCommand();
            driverCmd.CommandText = $"SELECT id, val FROM {table} ORDER BY id";
            using var driverReader = await driverCmd.ExecuteReaderAsync();
            while (await driverReader.ReadAsync())
            {
                var id = driverReader.GetInt32(0);
                var isNull = driverReader.IsDBNull(1);
                var val = isNull ? null : driverReader.GetString(1);
                driverResults.Add((id, isNull, val));
            }

            Assert.Equal(driverResults.Count, nativeResults.Count);
            for (int i = 0; i < nativeResults.Count; i++)
            {
                Assert.Equal(driverResults[i].id, nativeResults[i].id);
                Assert.Equal(driverResults[i].isNull, nativeResults[i].isNull);
                Assert.Equal(driverResults[i].val, nativeResults[i].val);
            }
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task GetSchemaTable_ReturnsColumnMetadata()
    {
        var table = $"smoke_ado_{Guid.NewGuid():N}";
        try
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"CREATE TABLE {table} (id Int32, name String, value Float64) ENGINE = Memory");

            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"INSERT INTO {table} VALUES (1, 'test', 3.14)");

            // Get schema from native
            await using var nativeConn = new ClickHouseDbConnection(_fixture.NativeConnectionString);
            await nativeConn.OpenAsync();
            using var nativeCmd = nativeConn.CreateCommand();
            nativeCmd.CommandText = $"SELECT * FROM {table}";
            using var nativeReader = await nativeCmd.ExecuteReaderAsync();
            var nativeSchema = nativeReader.GetSchemaTable();

            // Get schema from driver
            using var driverConn = new DriverConnection(_fixture.DriverConnectionString);
            await driverConn.OpenAsync();
            using var driverCmd = driverConn.CreateCommand();
            driverCmd.CommandText = $"SELECT * FROM {table}";
            using var driverReader = await driverCmd.ExecuteReaderAsync();
            var driverSchema = driverReader.GetSchemaTable();

            // Compare column names (both should have id, name, value)
            if (nativeSchema != null && driverSchema != null)
            {
                var nativeNames = nativeSchema.Rows.Cast<DataRow>()
                    .Select(r => r["ColumnName"]?.ToString())
                    .OrderBy(n => n)
                    .ToList();
                var driverNames = driverSchema.Rows.Cast<DataRow>()
                    .Select(r => r["ColumnName"]?.ToString())
                    .OrderBy(n => n)
                    .ToList();

                Assert.Equal(driverNames, nativeNames);
            }
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task Dapper_QueryAsync_MatchesResults()
    {
        var table = $"smoke_ado_{Guid.NewGuid():N}";
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
                $"INSERT INTO {table} VALUES (1, 'alice', 100.5), (2, 'bob', 200.25), (3, 'charlie', 300.75)");

            // Dapper via CH.Native ADO.NET
            await using var nativeConn = new ClickHouseDbConnection(_fixture.NativeConnectionString);
            await nativeConn.OpenAsync();
            var nativeRows = (await nativeConn.QueryAsync<dynamic>(
                $"SELECT id, name, value FROM {table} ORDER BY id")).ToList();

            // Dapper via ClickHouse.Driver ADO.NET
            using var driverConn = new DriverConnection(_fixture.DriverConnectionString);
            await driverConn.OpenAsync();
            var driverRows = (await driverConn.QueryAsync<dynamic>(
                $"SELECT id, name, value FROM {table} ORDER BY id")).ToList();

            Assert.Equal(driverRows.Count, nativeRows.Count);
            for (int i = 0; i < nativeRows.Count; i++)
            {
                var nativeDict = (IDictionary<string, object>)nativeRows[i];
                var driverDict = (IDictionary<string, object>)driverRows[i];

                Assert.Equal(driverDict.Keys.OrderBy(k => k), nativeDict.Keys.OrderBy(k => k));
                foreach (var key in nativeDict.Keys)
                {
                    ResultComparer.AssertValuesEqual(nativeDict[key], driverDict[key], $"Dapper row {i}, col {key}");
                }
            }
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task ConnectionState_MatchesBehavior()
    {
        await using var nativeConn = new ClickHouseDbConnection(_fixture.NativeConnectionString);
        Assert.Equal(ConnectionState.Closed, nativeConn.State);
        await nativeConn.OpenAsync();
        Assert.Equal(ConnectionState.Open, nativeConn.State);

        using var driverConn = new DriverConnection(_fixture.DriverConnectionString);
        Assert.Equal(ConnectionState.Closed, driverConn.State);
        await driverConn.OpenAsync();
        Assert.Equal(ConnectionState.Open, driverConn.State);
    }

    [Fact]
    public async Task MultipleQueries_OnSameConnection()
    {
        var table = $"smoke_ado_{Guid.NewGuid():N}";
        try
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"CREATE TABLE {table} (id Int32) ENGINE = Memory");

            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"INSERT INTO {table} VALUES (1), (2), (3)");

            // Multiple queries on same native connection
            await using var nativeConn = new ClickHouseDbConnection(_fixture.NativeConnectionString);
            await nativeConn.OpenAsync();

            using var cmd1 = nativeConn.CreateCommand();
            cmd1.CommandText = $"SELECT count() FROM {table}";
            var nativeCount = await cmd1.ExecuteScalarAsync();

            using var cmd2 = nativeConn.CreateCommand();
            cmd2.CommandText = $"SELECT max(id) FROM {table}";
            var nativeMax = await cmd2.ExecuteScalarAsync();

            // Multiple queries on same driver connection
            using var driverConn = new DriverConnection(_fixture.DriverConnectionString);
            await driverConn.OpenAsync();

            using var dcmd1 = driverConn.CreateCommand();
            dcmd1.CommandText = $"SELECT count() FROM {table}";
            var driverCount = await dcmd1.ExecuteScalarAsync();

            using var dcmd2 = driverConn.CreateCommand();
            dcmd2.CommandText = $"SELECT max(id) FROM {table}";
            var driverMax = await dcmd2.ExecuteScalarAsync();

            ResultComparer.AssertValuesEqual(nativeCount, driverCount, "Multiple queries: count");
            ResultComparer.AssertValuesEqual(nativeMax, driverMax, "Multiple queries: max");
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }
}
