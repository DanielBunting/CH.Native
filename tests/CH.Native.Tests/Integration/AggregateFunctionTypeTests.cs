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
            await foreach (var r in conn.QueryStreamAsync($"SELECT id, v_state FROM {mv} ORDER BY id"))
            {
                rows.Add((r.GetFieldValue<int>("id"), r.GetFieldValue<ClickHouseAggregateState>("v_state")));
            }

            Assert.True(rows.Count >= 1, "expected at least one materialized row");
            Assert.All(rows, t => Assert.Equal("sum", t.s.FunctionName));
            Assert.All(rows, t => Assert.Equal(8, t.s.State.Length)); // sumState(Int32) → 8 bytes

            // Cross-check: finalizeAggregation gives the expected totals.
            var totals = new Dictionary<int, long>();
            await foreach (var r in conn.QueryStreamAsync(
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
            await foreach (var r in conn.QueryStreamAsync($"SELECT id, c_state, min_state, max_state FROM {mv} ORDER BY id"))
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
            await foreach (var r in conn.QueryStreamAsync(
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
            await foreach (var r in conn.QueryStreamAsync($"SELECT id, total FROM {table} ORDER BY id"))
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

    // ------------------------------------------------------------------
    // Wide / exotic inner-type state-format coverage. The state-format registry's
    // byte sizes for these widths were exercised only by synthetic in-process bytes;
    // server-validated coverage stopped at Int32 widths (sum→8, min/max→5). These
    // read a REAL AggregateFunction state column produced by the server for each
    // wide inner type and assert the exact on-wire state length CH.Native expects.
    //
    // Because column data on the wire is NOT length-prefixed, a wrong state size
    // silently over/under-reads and corrupts the next column — so a trailing
    // sentinel column reading back correctly is itself proof the size is right
    // against this server version. A finalizeAggregation cross-check confirms the
    // bytes are semantically the ones ClickHouse produced.
    // ------------------------------------------------------------------

    [Theory]
    // innerType, stateFn, fnName, expectedStateBytes, sampleLiteral, expectedFinalizedToString
    [InlineData("Int128", "sumState", "sum", 16, "toInt128(5)", "5")]
    [InlineData("Int256", "sumState", "sum", 32, "toInt256(5)", "5")]
    [InlineData("UInt256", "sumState", "sum", 32, "toUInt256(5)", "5")]
    [InlineData("Float64", "sumState", "sum", 8, "toFloat64(1.5)", "1.5")]
    [InlineData("Int64", "minState", "min", 9, "toInt64(42)", "42")]
    [InlineData("UUID", "maxState", "max", 17, "toUUID('00000000-0000-0000-0000-000000000001')", "00000000-0000-0000-0000-000000000001")]
    [InlineData("Date", "maxState", "max", 3, "toDate('2024-01-01')", "2024-01-01")]
    // Decimal inner types (the fix). The server emits these as canonical Decimal(P, S);
    // sum PROMOTES (Decimal32/64/128 → 16-byte state, Decimal256 → 32), while min/max
    // store 1 flag + native width (Decimal32 → 5, Decimal128 → 17, Decimal256 → 33).
    [InlineData("Decimal32(4)", "sumState", "sum", 16, "toDecimal32(1.2345, 4)", "1.2345")]
    [InlineData("Decimal128(4)", "sumState", "sum", 16, "toDecimal128(1.2345, 4)", "1.2345")]
    [InlineData("Decimal256(4)", "sumState", "sum", 32, "toDecimal256(1.2345, 4)", "1.2345")]
    [InlineData("Decimal32(4)", "minState", "min", 5, "toDecimal32(1.2345, 4)", "1.2345")]
    [InlineData("Decimal128(4)", "minState", "min", 17, "toDecimal128(1.2345, 4)", "1.2345")]
    [InlineData("Decimal256(4)", "maxState", "max", 33, "toDecimal256(1.2345, 4)", "1.2345")]
    // Higher scale (10 dp) — state size is unchanged (it follows precision, not scale),
    // and the finalized value preserves all 10 decimal places. The literal is passed as
    // a STRING so the server parses the decimal exactly (a numeric literal would round-
    // trip through Float64 and lose the last digits).
    [InlineData("Decimal128(10)", "sumState", "sum", 16, "toDecimal128('1.2345678901', 10)", "1.2345678901")]
    [InlineData("Decimal64(10)", "minState", "min", 9, "toDecimal64('1.2345678901', 10)", "1.2345678901")]
    public async Task AggregateState_WideInnerType_DecodesExactSize_AndStaysAligned(
        string innerType, string stateFn, string fnName, int expectedStateBytes,
        string sampleLiteral, string expectedFinalizedToString)
    {
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();
        var src = $"agg_wide_src_{Guid.NewGuid():N}";
        var mv = $"agg_wide_mv_{Guid.NewGuid():N}";

        await conn.ExecuteNonQueryAsync($"CREATE TABLE {src} (id Int32, v {innerType}) ENGINE=MergeTree ORDER BY id");
        await conn.ExecuteNonQueryAsync(
            $"CREATE MATERIALIZED VIEW {mv} ENGINE=AggregatingMergeTree ORDER BY id AS " +
            $"SELECT id, {stateFn}(v) AS s FROM {src} GROUP BY id");

        try
        {
            await conn.ExecuteNonQueryAsync($"INSERT INTO {src} VALUES (1, {sampleLiteral})");

            // Read the state column followed by a trailing sentinel: if the state
            // size is wrong the sentinel reads garbage (or the connection poisons).
            ClickHouseAggregateState? state = null;
            int sentinel = 0;
            await foreach (var r in conn.QueryStreamAsync(
                $"SELECT s, toInt32(987654) AS sentinel FROM {mv} ORDER BY id"))
            {
                state = r.GetFieldValue<ClickHouseAggregateState>("s");
                sentinel = r.GetFieldValue<int>("sentinel");
            }

            Assert.NotNull(state);
            Assert.Equal(fnName, state!.FunctionName);
            Assert.Equal(expectedStateBytes, state.State.Length);
            Assert.Equal(987654, sentinel); // alignment held → size decode is correct

            // The connection must remain usable for a subsequent query.
            var ping = await conn.ExecuteScalarAsync<int>("SELECT 123");
            Assert.Equal(123, ping);

            // Semantic cross-check: the server finalizes the same state to the value.
            var finalized = await conn.ExecuteScalarAsync<string>(
                $"SELECT toString(finalizeAggregation(s)) FROM {mv} ORDER BY id LIMIT 1");
            Assert.Equal(expectedFinalizedToString, finalized);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {mv}");
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {src}");
        }
    }

    /// <summary>
    /// A genuinely-unsupported aggregate state (<c>uniqExact</c> — a variable-size HLL/hash
    /// state outside CH.Native's tier-1 set) must still fail CLEAN: a clear
    /// <see cref="NotSupportedException"/> naming the <c>finalizeAggregation</c>/<c>hex</c>
    /// workarounds, with the broken connection isolated (a fresh connection works and the
    /// documented workarounds succeed). This guards the unsupported-type hardening path
    /// now that the Decimal inner types it used to demonstrate have been fixed and moved
    /// into the matrix above.
    /// </summary>
    [Fact]
    public async Task AggregateState_UnsupportedFunction_FailsClean_WithWorkaround()
    {
        var src = $"agg_unsup_src_{Guid.NewGuid():N}";
        var mv = $"agg_unsup_mv_{Guid.NewGuid():N}";

        await using (var setup = new ClickHouseConnection(_fixture.ConnectionString))
        {
            await setup.OpenAsync();
            await setup.ExecuteNonQueryAsync($"CREATE TABLE {src} (id Int32, v UInt64) ENGINE=MergeTree ORDER BY id");
            await setup.ExecuteNonQueryAsync(
                $"CREATE MATERIALIZED VIEW {mv} ENGINE=AggregatingMergeTree ORDER BY id AS " +
                $"SELECT id, uniqExactState(v) AS s FROM {src} GROUP BY id");
            await setup.ExecuteNonQueryAsync($"INSERT INTO {src} VALUES (1, 10), (1, 20), (1, 10), (2, 99)");
        }

        try
        {
            // Reading the opaque uniqExact state throws clean (and closes that connection).
            var failing = new ClickHouseConnection(_fixture.ConnectionString);
            await failing.OpenAsync();
            var ex = await Assert.ThrowsAsync<NotSupportedException>(async () =>
            {
                await foreach (var _ in failing.QueryStreamAsync($"SELECT s FROM {mv}"))
                {
                }
            });
            Assert.Contains("not supported", ex.Message);
            Assert.Contains("uniqExact", ex.Message);
            Assert.Contains("finalizeAggregation", ex.Message); // the documented workaround
            await failing.DisposeAsync();

            // A fresh connection is unaffected, and the documented workarounds work.
            await using var ok = new ClickHouseConnection(_fixture.ConnectionString);
            await ok.OpenAsync();

            var distinctForGroup1 = await ok.ExecuteScalarAsync<ulong>(
                $"SELECT toUInt64(finalizeAggregation(s)) FROM {mv} WHERE id = 1");
            Assert.Equal(2UL, distinctForGroup1); // {10, 20} → 2 distinct

            // hex(state) ships the opaque bytes as a String — also documented.
            var hexLen = await ok.ExecuteScalarAsync<ulong>($"SELECT length(hex(s)) / 2 FROM {mv} WHERE id = 1");
            Assert.True(hexLen > 0);
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
