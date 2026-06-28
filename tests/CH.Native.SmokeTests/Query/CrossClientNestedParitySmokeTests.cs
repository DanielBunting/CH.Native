using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SmokeTests.Fixtures;
using CH.Native.SmokeTests.Helpers;
using Xunit;

namespace CH.Native.SmokeTests.Query;

/// <summary>
/// Cross-driver parity for the ClickHouse <c>Nested(...)</c> type. The generic parity
/// matrix (<see cref="CrossClientInsertReadParitySmokeTests"/>) covers Array and Map but
/// not Nested, whose whole-column wire form (shared offsets, <c>flatten_nested=0</c>) was
/// the subject of a recent reader/writer/skipper fix. These pin that CH.Native and the
/// official <c>ClickHouse.Driver</c> agree on Nested data — proving CH.Native emits
/// standard wire bytes, not merely bytes self-consistent with its own reader.
/// </summary>
[Collection("SmokeTest")]
public class CrossClientNestedParitySmokeTests
{
    private readonly SmokeTestFixture _fixture;

    public CrossClientNestedParitySmokeTests(SmokeTestFixture fixture)
    {
        _fixture = fixture;
    }

    private sealed class NestedRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        // Whole Nested column: object[] of per-field arrays, here [ string[] keys, int[] values ].
        [ClickHouseColumn(Name = "n", Order = 1)] public object[] N { get; set; } = Array.Empty<object>();
    }

    // flatten_nested=0 must be set in the same session as CREATE so `n` is stored as a
    // single Nested column; the native protocol runs one statement per query.
    private async Task<ClickHouseConnection> OpenUnflattenedAsync()
    {
        var conn = new ClickHouseConnection(_fixture.NativeConnectionString);
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync("SET flatten_nested = 0");
        return conn;
    }

    [Fact]
    public async Task Nested_NativeWholeColumnWrite_BothClientsReadSubColumnsIdentically()
    {
        var table = $"smoke_nested_{Guid.NewGuid():N}";
        await using var conn = await OpenUnflattenedAsync();
        try
        {
            await conn.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id Int32, n Nested(key String, value Int32)) ENGINE = Memory");

            // Writer: CH.Native bulk insert of the whole Nested column (the fixed writer).
            await using (var ins = conn.CreateBulkInserter<NestedRow>(table))
            {
                await ins.InitAsync();
                await ins.AddAsync(new NestedRow { Id = 1, N = new object[] { new[] { "a", "b" }, new[] { 10, 20 } } });
                await ins.AddAsync(new NestedRow { Id = 2, N = new object[] { Array.Empty<string>(), Array.Empty<int>() } });
                await ins.AddAsync(new NestedRow { Id = 3, N = new object[] { new[] { "only" }, new[] { 99 } } });
                await ins.CompleteAsync();
            }

            var sql = $"SELECT n.key, n.value FROM {table} ORDER BY id";

            // Reader 1: CH.Native. Reader 2: official ClickHouse.Driver. Must match.
            var native = await NativeQueryHelper.QueryStreamAsync(_fixture.NativeConnectionString, sql);
            var driver = await DriverQueryHelper.QueryStreamAsync(_fixture.DriverConnectionString, sql);

            ResultComparer.AssertResultsEqual(native, driver, "Nested sub-column cross-driver parity");

            // Anchor the actual values too (not just "both clients agree").
            Assert.Equal(3, native.Count);
            Assert.Equal(new[] { "a", "b" }, (string[])native[0][0]!);
            Assert.Equal(new[] { 10, 20 }, (int[])native[0][1]!);
            Assert.Empty((string[])native[1][0]!);
            Assert.Equal(new[] { "only" }, (string[])native[2][0]!);
            Assert.Equal(new[] { 99 }, (int[])native[2][1]!);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task Nested_CliWrite_NativeWholeColumnRead_MatchesDriverSubColumns()
    {
        var table = $"smoke_nested_{Guid.NewGuid():N}";
        await using var conn = await OpenUnflattenedAsync();
        try
        {
            await conn.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id Int32, n Nested(key String, value Int32)) ENGINE = Memory");

            // Writer: clickhouse-client, array-of-tuples literal for the Nested column.
            await CliQueryHelper.ExecuteNonQueryAsync(_fixture,
                $"INSERT INTO {table} VALUES (1, [('a',10),('b',20)]), (2, [])");

            // CH.Native reads the WHOLE column (our reader); the driver reads sub-columns.
            var nativeWhole = new List<(string[] keys, int[] values)>();
            await foreach (var row in conn.QueryStreamAsync($"SELECT n FROM {table} ORDER BY id"))
            {
                var n = (object[])row.GetFieldValue<object>("n");
                nativeWhole.Add(((string[])n[0], (int[])n[1]));
            }

            var driverSub = await DriverQueryHelper.QueryStreamAsync(
                _fixture.DriverConnectionString, $"SELECT n.key, n.value FROM {table} ORDER BY id");

            Assert.Equal(2, nativeWhole.Count);
            Assert.Equal(2, driverSub.Count);
            // Row 1
            Assert.Equal(new[] { "a", "b" }, nativeWhole[0].keys);
            Assert.Equal(new[] { "a", "b" }, (string[])driverSub[0][0]!);
            Assert.Equal(new[] { 10, 20 }, nativeWhole[0].values);
            Assert.Equal(new[] { 10, 20 }, (int[])driverSub[0][1]!);
            // Row 2 (empty)
            Assert.Empty(nativeWhole[1].keys);
            Assert.Empty((string[])driverSub[1][0]!);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }
}
