using CH.Native.Connection;
using CH.Native.Data.AggregateState;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.VersionMatrix;

/// <summary>
/// MV-shaped scenarios + <c>SimpleAggregateFunction</c> variants + schema variations
/// across <see cref="SupportedImages.All"/>. Mirrors realistic usage patterns —
/// multi-column MVs, empty MVs, mixed aggregate/simple-aggregate columns, and
/// various inner numeric widths — that single-function matrix tests don't catch.
/// </summary>
[Collection("VersionMatrix")]
[Trait(Categories.Name, Categories.VersionMatrix)]
public class AggregateFunctionScenarioTests
{
    private readonly VersionedNodeCache _cache;

    public AggregateFunctionScenarioTests(VersionedNodeCache cache) => _cache = cache;

    private async Task<ClickHouseConnection> OpenAsync(string image)
    {
        var settings = await _cache.GetSettingsAsync(image);
        var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();
        return conn;
    }

    // --- AggregateFunction MV scenarios ---------------------------------------

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task MultiColumnMv_AllTier1Functions_SelectStar_Succeeds(string image)
    {
        await using var conn = await OpenAsync(image);
        var src = $"agg_multi_src_{Guid.NewGuid():N}";
        var mv = $"agg_multi_mv_{Guid.NewGuid():N}";

        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {src} (id Int32, v Int32) ENGINE=MergeTree ORDER BY id");
        await conn.ExecuteNonQueryAsync(
            $"CREATE MATERIALIZED VIEW {mv} ENGINE=AggregatingMergeTree ORDER BY id AS " +
            $"SELECT id, " +
            $"  countState() AS c_state, " +
            $"  sumState(v) AS s_state, " +
            $"  minState(v) AS mn_state, " +
            $"  maxState(v) AS mx_state, " +
            $"  anyState(v) AS a_state, " +
            $"  anyLastState(v) AS al_state " +
            $"FROM {src} GROUP BY id");

        try
        {
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {src} VALUES (1, 10), (1, 20), (1, 5), (2, 100)");

            int rows = 0;
            await foreach (var r in conn.QueryAsync(
                $"SELECT id, c_state, s_state, mn_state, mx_state, a_state, al_state FROM {mv} ORDER BY id"))
            {
                Assert.Equal("count", r.GetFieldValue<ClickHouseAggregateState>("c_state").FunctionName);
                Assert.Equal("sum", r.GetFieldValue<ClickHouseAggregateState>("s_state").FunctionName);
                Assert.Equal("min", r.GetFieldValue<ClickHouseAggregateState>("mn_state").FunctionName);
                Assert.Equal("max", r.GetFieldValue<ClickHouseAggregateState>("mx_state").FunctionName);
                Assert.Equal("any", r.GetFieldValue<ClickHouseAggregateState>("a_state").FunctionName);
                Assert.Equal("anyLast", r.GetFieldValue<ClickHouseAggregateState>("al_state").FunctionName);
                rows++;
            }
            Assert.True(rows >= 1);

            // finalizeAggregation cross-check on a subset.
            var totals = new Dictionary<int, (long c, long s, int mn, int mx)>();
            await foreach (var r in conn.QueryAsync(
                $"SELECT id, " +
                $"  toInt64(finalizeAggregation(c_state)) AS c, " +
                $"  toInt64(finalizeAggregation(s_state)) AS s, " +
                $"  finalizeAggregation(mn_state) AS mn, " +
                $"  finalizeAggregation(mx_state) AS mx " +
                $"FROM {mv} ORDER BY id"))
            {
                totals[r.GetFieldValue<int>("id")] = (
                    r.GetFieldValue<long>("c"),
                    r.GetFieldValue<long>("s"),
                    r.GetFieldValue<int>("mn"),
                    r.GetFieldValue<int>("mx"));
            }
            Assert.Equal((3L, 35L, 5, 20), totals[1]);
            Assert.Equal((1L, 100L, 100, 100), totals[2]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {mv}");
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {src}");
        }
    }

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task EmptyMv_NoRows_NoException(string image)
    {
        await using var conn = await OpenAsync(image);
        var src = $"agg_empty_src_{Guid.NewGuid():N}";
        var mv = $"agg_empty_mv_{Guid.NewGuid():N}";

        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {src} (id Int32, v Int32) ENGINE=MergeTree ORDER BY id");
        await conn.ExecuteNonQueryAsync(
            $"CREATE MATERIALIZED VIEW {mv} ENGINE=AggregatingMergeTree ORDER BY id AS " +
            $"SELECT id, sumState(v) AS s FROM {src} GROUP BY id");

        try
        {
            // No INSERTs — MV stays empty.
            int rows = 0;
            await foreach (var _ in conn.QueryAsync($"SELECT id, s FROM {mv} ORDER BY id"))
                rows++;
            Assert.Equal(0, rows);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {mv}");
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {src}");
        }
    }

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task SumState_AllSupportedInnerNumericWidths_RoundTrip(string image)
    {
        // Same payload, different inner widths. All should land as expected scalars
        // regardless of CH version — sum() widens consistently within each width class.
        await using var conn = await OpenAsync(image);

        foreach (var inner in new[] { "Int8", "Int16", "Int32", "Int64", "Float32", "Float64" })
        {
            var src = $"agg_w_src_{Guid.NewGuid():N}";
            var mv = $"agg_w_mv_{Guid.NewGuid():N}";

            await conn.ExecuteNonQueryAsync(
                $"CREATE TABLE {src} (id Int32, v {inner}) ENGINE=MergeTree ORDER BY id");
            await conn.ExecuteNonQueryAsync(
                $"CREATE MATERIALIZED VIEW {mv} ENGINE=AggregatingMergeTree ORDER BY id AS " +
                $"SELECT id, sumState(v) AS s FROM {src} GROUP BY id");

            try
            {
                await conn.ExecuteNonQueryAsync($"INSERT INTO {src} VALUES (1, 3), (1, 4), (2, 7)");

                var states = new List<ClickHouseAggregateState>();
                await foreach (var r in conn.QueryAsync($"SELECT s FROM {mv} ORDER BY id"))
                    states.Add(r.GetFieldValue<ClickHouseAggregateState>("s"));

                Assert.NotEmpty(states);
                Assert.All(states, s => Assert.Equal("sum", s.FunctionName));
                Assert.All(states, s => Assert.Equal(8, s.State.Length));   // all widen to 8 bytes
            }
            finally
            {
                await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {mv}");
                await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {src}");
            }
        }
    }

    // --- SimpleAggregateFunction variants -------------------------------------

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task SimpleAggregateFunction_Sum_Int64_ReadsAsLong(string image)
    {
        await using var conn = await OpenAsync(image);
        var table = $"sagg_sum_{Guid.NewGuid():N}";

        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, total SimpleAggregateFunction(sum, Int64)) " +
            $"ENGINE=AggregatingMergeTree ORDER BY id");

        try
        {
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES (1, 100), (1, 200), (2, 5)");
            await conn.ExecuteNonQueryAsync($"OPTIMIZE TABLE {table} FINAL");

            var got = new Dictionary<int, long>();
            await foreach (var r in conn.QueryAsync($"SELECT id, total FROM {table} ORDER BY id"))
                got[r.GetFieldValue<int>("id")] = r.GetFieldValue<long>("total");

            Assert.Equal(300L, got[1]); // 100 + 200 merged
            Assert.Equal(5L, got[2]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task SimpleAggregateFunction_Min_String_ReadsAsString(string image)
    {
        await using var conn = await OpenAsync(image);
        var table = $"sagg_min_{Guid.NewGuid():N}";

        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, smallest SimpleAggregateFunction(min, String)) " +
            $"ENGINE=AggregatingMergeTree ORDER BY id");

        try
        {
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES (1, 'banana'), (1, 'apple'), (1, 'cherry'), (2, 'zebra')");
            await conn.ExecuteNonQueryAsync($"OPTIMIZE TABLE {table} FINAL");

            var got = new Dictionary<int, string>();
            await foreach (var r in conn.QueryAsync($"SELECT id, smallest FROM {table} ORDER BY id"))
                got[r.GetFieldValue<int>("id")] = r.GetFieldValue<string>("smallest");

            Assert.Equal("apple", got[1]);
            Assert.Equal("zebra", got[2]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task MixedTable_AggregateFunction_And_SimpleAggregateFunction(string image)
    {
        // One table with both a SimpleAggregateFunction column (transparent inner)
        // and an AggregateFunction column (opaque state). SELECT * must read both.
        await using var conn = await OpenAsync(image);
        var table = $"agg_mixed_{Guid.NewGuid():N}";

        await conn.ExecuteNonQueryAsync($"""
            CREATE TABLE {table} (
                id Int32,
                running_max SimpleAggregateFunction(max, Int32),
                sum_state AggregateFunction(sum, Int32)
            ) ENGINE=AggregatingMergeTree ORDER BY id
            """);

        try
        {
            // Seed via SELECT … State() so the AggregateFunction column gets real states.
            await conn.ExecuteNonQueryAsync($"""
                INSERT INTO {table}
                SELECT 1, 50, sumState(toInt32(number)) FROM numbers(11) GROUP BY 1
                """);
            await conn.ExecuteNonQueryAsync($"OPTIMIZE TABLE {table} FINAL");

            await foreach (var r in conn.QueryAsync($"SELECT id, running_max, sum_state FROM {table}"))
            {
                Assert.Equal(1, r.GetFieldValue<int>("id"));
                Assert.Equal(50, r.GetFieldValue<int>("running_max"));
                var state = r.GetFieldValue<ClickHouseAggregateState>("sum_state");
                Assert.Equal("sum", state.FunctionName);
                Assert.Equal(8, state.State.Length);
            }

            // Finalize the AggregateFunction column server-side to verify the bytes are right.
            var total = await conn.ExecuteScalarAsync<long>(
                $"SELECT toInt64(finalizeAggregation(sum_state)) FROM {table}");
            Assert.Equal(55L, total); // 0..10 sum
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }
}
