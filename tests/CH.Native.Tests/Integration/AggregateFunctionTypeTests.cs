using CH.Native.Connection;
using CH.Native.Data.AggregateState;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Headline integration test for <c>AggregateFunction</c> support — proves
/// <c>SELECT *</c> from an <c>AggregatingMergeTree</c> MV with tier-1 aggregates
/// no longer throws, plus a finalize-aggregation cross-check that the bytes
/// we read are the same ones ClickHouse produces.
/// </summary>
[Collection("ClickHouse")]
public class AggregateFunctionTypeTests
{
    private readonly ClickHouseFixture _fixture;

    public AggregateFunctionTypeTests(ClickHouseFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task SelectStar_FromAggregatingMv_Succeeds_ForSumState()
    {
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();
        var src = $"agg_src_{Guid.NewGuid():N}";
        var mv = $"agg_mv_{Guid.NewGuid():N}";

        await conn.ExecuteNonQueryAsync($"CREATE TABLE {src} (id Int32, v Int32) ENGINE=MergeTree ORDER BY id");
        await conn.ExecuteNonQueryAsync(
            $"CREATE MATERIALIZED VIEW {mv} ENGINE=AggregatingMergeTree ORDER BY id AS " +
            $"SELECT id, sumState(v) AS v_state FROM {src} GROUP BY id");

        try
        {
            await conn.ExecuteNonQueryAsync($"INSERT INTO {src} VALUES (1, 10), (1, 20), (2, 5)");

            // The crucial assertion: SELECT * does not throw on the state column.
            var rows = new List<(int id, ClickHouseAggregateState s)>();
            await foreach (var r in conn.QueryAsync($"SELECT id, v_state FROM {mv} ORDER BY id"))
            {
                rows.Add((r.GetFieldValue<int>("id"), r.GetFieldValue<ClickHouseAggregateState>("v_state")));
            }

            Assert.True(rows.Count >= 1, "expected at least one materialized row");
            Assert.All(rows, t => Assert.Equal("sum", t.s.FunctionName));
            Assert.All(rows, t => Assert.Equal(8, t.s.State.Length)); // sumState(Int32) → 8 bytes

            // Cross-check: finalizeAggregation gives the expected totals.
            var totals = new Dictionary<int, long>();
            await foreach (var r in conn.QueryAsync(
                $"SELECT id, toInt64(finalizeAggregation(v_state)) AS total FROM {mv} ORDER BY id"))
            {
                totals[r.GetFieldValue<int>("id")] = r.GetFieldValue<long>("total");
            }
            Assert.Equal(30L, totals[1]); // 10 + 20
            Assert.Equal(5L, totals[2]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {mv}");
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {src}");
        }
    }

    [Fact]
    public async Task SelectStar_FromAggregatingMv_Succeeds_ForCountAndMinMaxStates()
    {
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();
        var src = $"agg_multi_src_{Guid.NewGuid():N}";
        var mv = $"agg_multi_mv_{Guid.NewGuid():N}";

        await conn.ExecuteNonQueryAsync($"CREATE TABLE {src} (id Int32, v Int32) ENGINE=MergeTree ORDER BY id");
        await conn.ExecuteNonQueryAsync(
            $"CREATE MATERIALIZED VIEW {mv} ENGINE=AggregatingMergeTree ORDER BY id AS " +
            $"SELECT id, countState() AS c_state, minState(v) AS min_state, maxState(v) AS max_state " +
            $"FROM {src} GROUP BY id");

        try
        {
            await conn.ExecuteNonQueryAsync($"INSERT INTO {src} VALUES (1, 10), (1, 20), (1, 5), (2, 100)");

            var rows = new List<(int id, ClickHouseAggregateState c, ClickHouseAggregateState mn, ClickHouseAggregateState mx)>();
            await foreach (var r in conn.QueryAsync($"SELECT id, c_state, min_state, max_state FROM {mv} ORDER BY id"))
            {
                rows.Add((
                    r.GetFieldValue<int>("id"),
                    r.GetFieldValue<ClickHouseAggregateState>("c_state"),
                    r.GetFieldValue<ClickHouseAggregateState>("min_state"),
                    r.GetFieldValue<ClickHouseAggregateState>("max_state")));
            }

            Assert.True(rows.Count >= 1);
            Assert.All(rows, t => Assert.Equal("count", t.c.FunctionName));
            Assert.All(rows, t => Assert.Equal("min", t.mn.FunctionName));
            Assert.All(rows, t => Assert.Equal("max", t.mx.FunctionName));

            // count state is a varuint; min/max(Int32) is 1-byte flag + 4 data bytes.
            Assert.All(rows, t => Assert.NotEmpty(t.c.State));
            Assert.All(rows, t => Assert.Equal(5, t.mn.State.Length));
            Assert.All(rows, t => Assert.Equal(5, t.mx.State.Length));

            // Server-side cross-checks against finalizeAggregation.
            var checks = new Dictionary<int, (long count, int min, int max)>();
            await foreach (var r in conn.QueryAsync(
                $"SELECT id, toInt64(finalizeAggregation(c_state)) AS c, " +
                $"finalizeAggregation(min_state) AS mn, finalizeAggregation(max_state) AS mx " +
                $"FROM {mv} ORDER BY id"))
            {
                checks[r.GetFieldValue<int>("id")] = (
                    r.GetFieldValue<long>("c"),
                    r.GetFieldValue<int>("mn"),
                    r.GetFieldValue<int>("mx"));
            }
            Assert.Equal((3L, 5, 20), checks[1]);
            Assert.Equal((1L, 100, 100), checks[2]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {mv}");
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {src}");
        }
    }

    [Fact]
    public async Task SelectStar_FromAggregatingMv_Succeeds_ForSimpleAggregateFunction()
    {
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();
        var table = $"agg_simple_{Guid.NewGuid():N}";

        // SimpleAggregateFunction stores the inner type's value directly; the function
        // name is a server-side merge hint that the driver ignores.
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, total SimpleAggregateFunction(sum, Int64)) " +
            $"ENGINE=AggregatingMergeTree ORDER BY id");

        try
        {
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES (1, 100), (1, 200), (2, 5)");
            await conn.ExecuteNonQueryAsync($"OPTIMIZE TABLE {table} FINAL");

            var rows = new List<(int id, long total)>();
            await foreach (var r in conn.QueryAsync($"SELECT id, total FROM {table} ORDER BY id"))
            {
                rows.Add((r.GetFieldValue<int>("id"), r.GetFieldValue<long>("total")));
            }

            Assert.True(rows.Count >= 2);
            // SimpleAggregateFunction reads as the inner CLR type directly.
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }
}
