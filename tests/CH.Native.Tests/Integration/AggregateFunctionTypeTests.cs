using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// AggregatingMergeTree behaviour for a push-and-query client. CH.Native does not
/// decode raw <c>AggregateFunction(...)</c> state columns (opaque, server-internal
/// blobs); the supported path is to query the finalized value with
/// <c>finalizeAggregation()</c>, and <c>SimpleAggregateFunction(fn, T)</c> reads as its
/// inner type <c>T</c>. These pin that contract: the finalize path returns correct
/// values, SimpleAggregateFunction round-trips, and a raw state column fails clean with
/// actionable guidance (connection isolated).
/// </summary>
[Collection("ClickHouse")]
public class AggregateFunctionTypeTests
{
    private readonly ClickHouseFixture _fixture;

    public AggregateFunctionTypeTests(ClickHouseFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task FinalizeAggregation_FromAggregatingMv_ReturnsCorrectValues()
    {
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();
        var src = $"agg_src_{Guid.NewGuid():N}";
        var mv = $"agg_mv_{Guid.NewGuid():N}";

        await conn.ExecuteNonQueryAsync($"CREATE TABLE {src} (id Int32, v Int32) ENGINE=MergeTree ORDER BY id");
        await conn.ExecuteNonQueryAsync(
            $"CREATE MATERIALIZED VIEW {mv} ENGINE=AggregatingMergeTree ORDER BY id AS " +
            $"SELECT id, countState() AS c, sumState(v) AS s, minState(v) AS mn, maxState(v) AS mx " +
            $"FROM {src} GROUP BY id");

        try
        {
            await conn.ExecuteNonQueryAsync($"INSERT INTO {src} VALUES (1, 10), (1, 20), (1, 5), (2, 100)");

            var byId = new Dictionary<int, (long count, long sum, int min, int max)>();
            await foreach (var r in conn.QueryStreamAsync(
                $"SELECT id, toInt64(finalizeAggregation(c)) AS c, toInt64(finalizeAggregation(s)) AS s, " +
                $"finalizeAggregation(mn) AS mn, finalizeAggregation(mx) AS mx FROM {mv} ORDER BY id"))
            {
                byId[r.GetFieldValue<int>("id")] = (
                    r.GetFieldValue<long>("c"),
                    r.GetFieldValue<long>("s"),
                    r.GetFieldValue<int>("mn"),
                    r.GetFieldValue<int>("mx"));
            }

            Assert.Equal((3L, 35L, 5, 20), byId[1]);
            Assert.Equal((1L, 100L, 100, 100), byId[2]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {mv}");
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {src}");
        }
    }

    [Fact]
    public async Task SimpleAggregateFunction_ReadsAsInnerType()
    {
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();
        var table = $"agg_simple_{Guid.NewGuid():N}";

        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, total SimpleAggregateFunction(sum, Int64)) " +
            $"ENGINE=AggregatingMergeTree ORDER BY id");

        try
        {
            await conn.ExecuteNonQueryAsync($"INSERT INTO {table} VALUES (1, 100), (1, 200), (2, 5)");
            await conn.ExecuteNonQueryAsync($"OPTIMIZE TABLE {table} FINAL");

            var byId = new Dictionary<int, long>();
            await foreach (var r in conn.QueryStreamAsync($"SELECT id, total FROM {table} ORDER BY id"))
                byId[r.GetFieldValue<int>("id")] = r.GetFieldValue<long>("total"); // reads as long, no wrapper

            Assert.Equal(300L, byId[1]); // 100 + 200 merged
            Assert.Equal(5L, byId[2]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task RawAggregateFunctionStateColumn_FailsClean_WithGuidance()
    {
        // Reading a raw AggregateFunction state column is not supported. It must throw a
        // clear NotSupportedException naming the finalizeAggregation/hex workarounds, with
        // the broken connection isolated (a fresh connection still works, and the
        // documented workarounds succeed).
        var src = $"agg_raw_src_{Guid.NewGuid():N}";
        var mv = $"agg_raw_mv_{Guid.NewGuid():N}";

        await using (var setup = new ClickHouseConnection(_fixture.ConnectionString))
        {
            await setup.OpenAsync();
            await setup.ExecuteNonQueryAsync($"CREATE TABLE {src} (id Int32, v Int32) ENGINE=MergeTree ORDER BY id");
            await setup.ExecuteNonQueryAsync(
                $"CREATE MATERIALIZED VIEW {mv} ENGINE=AggregatingMergeTree ORDER BY id AS " +
                $"SELECT id, sumState(v) AS s FROM {src} GROUP BY id");
            await setup.ExecuteNonQueryAsync($"INSERT INTO {src} VALUES (1, 10), (1, 20)");
        }

        try
        {
            var failing = new ClickHouseConnection(_fixture.ConnectionString);
            await failing.OpenAsync();
            var ex = await Assert.ThrowsAsync<NotSupportedException>(async () =>
            {
                await foreach (var _ in failing.QueryStreamAsync($"SELECT s FROM {mv}"))
                {
                }
            });
            Assert.Contains("not supported", ex.Message);
            Assert.Contains("finalizeAggregation", ex.Message);
            Assert.Contains("hex(", ex.Message);
            await failing.DisposeAsync();

            // Fresh connection is unaffected; the documented workarounds both work.
            await using var ok = new ClickHouseConnection(_fixture.ConnectionString);
            await ok.OpenAsync();

            var finalized = await ok.ExecuteScalarAsync<long>(
                $"SELECT toInt64(finalizeAggregation(s)) FROM {mv} WHERE id = 1");
            Assert.Equal(30L, finalized);

            var hex = await ok.ExecuteScalarAsync<string>($"SELECT hex(s) FROM {mv} WHERE id = 1");
            Assert.False(string.IsNullOrEmpty(hex)); // raw bytes transfer as a String
        }
        finally
        {
            await using var cleanup = new ClickHouseConnection(_fixture.ConnectionString);
            await cleanup.OpenAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {mv}");
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {src}");
        }
    }
}
