using CH.Native.Connection;
using CH.Native.Data.AggregateState;
using CH.Native.Linq;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.VersionMatrix;

/// <summary>
/// Edge cases around the aggregate-function feature: unsupported-function error
/// contract, server-side workaround recipes (<c>finalizeAggregation</c>, <c>hex</c>/<c>unhex</c>),
/// and round-trip via <see cref="ClickHouseQueryableExtensions.InsertAsync"/>.
///
/// The single-image error-contract test runs on every image
/// (cheap — no MV setup) so the always-on lane gets a smoke check that the
/// driver's "we don't decode this" message stays accurate and actionable.
/// </summary>
[Collection("VersionMatrix")]
[Trait(Categories.Name, Categories.VersionMatrix)]
public class AggregateFunctionEdgeCaseTests
{
    private readonly VersionedNodeCache _cache;

    public AggregateFunctionEdgeCaseTests(VersionedNodeCache cache) => _cache = cache;

    private async Task<ClickHouseConnection> OpenAsync(string image)
    {
        var settings = await _cache.GetSettingsAsync(image);
        var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();
        return conn;
    }

    // --- Unsupported-function error contract ---------------------------------

    [Theory]
    [InlineData("quantilesTDigestState(0.5)", "toFloat64(1.5)", "quantilesTDigest")]
    [InlineData("uniqExactState", "toUInt64(1)", "uniqExact")]
    [InlineData("groupArrayState", "toInt32(1)", "groupArray")]
    [InlineData("topKState(10)", "'foo'", "topK")]
    public async Task UnsupportedFunction_ThrowsActionableError_OnLatestImage(
        string aggCall, string argExpr, string bareFnName)
    {
        // Each inline case is (aggregate-call-expression, value-expression-for-inner-type,
        // bare-function-name-for-error-message-match). The query emits a single state
        // value of the unsupported type so the driver triggers reader-factory
        // resolution and throws NotSupportedException.
        await using var conn = await OpenAsync(SupportedImages.Latest);

        var ex = await Assert.ThrowsAsync<NotSupportedException>(async () =>
        {
            _ = await conn.ExecuteScalarAsync<ClickHouseAggregateState>(
                $"SELECT {aggCall}({argExpr})");
        });

        Assert.Contains("not supported", ex.Message);
        Assert.Contains("finalizeAggregation", ex.Message);
        Assert.Contains("hex(", ex.Message);
        Assert.Contains(bareFnName, ex.Message);
    }

    // --- Server-side workaround recipes --------------------------------------

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task UniqExact_FinalizeAggregationWorkaround_Works(string image)
    {
        // The error message tells users to project through finalizeAggregation().
        // Prove that recipe actually works on every supported image.
        await using var conn = await OpenAsync(image);
        var cardinality = await conn.ExecuteScalarAsync<ulong>(
            "SELECT finalizeAggregation(uniqExactState(toUInt64(number % 50))) FROM numbers(1000)");
        Assert.Equal(50UL, cardinality);
    }

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task UniqExact_HexUnhexWorkaround_Works(string image)
    {
        // The other workaround the error message recommends: transfer the state
        // as a hex String, finalize on demand.
        await using var conn = await OpenAsync(image);
        var stateHex = await conn.ExecuteScalarAsync<string>(
            "SELECT hex(uniqExactState(toUInt64(number % 50))) FROM numbers(1000)");
        Assert.False(string.IsNullOrEmpty(stateHex));

        // Round-trip through unhex back to the same scalar.
        var table = $"hex_recipe_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (s AggregateFunction(uniqExact, UInt64)) ENGINE = Memory");
        try
        {
            await conn.ExecuteNonQueryAsync($"INSERT INTO {table} VALUES (unhex('{stateHex}'))");
            var rehydrated = await conn.ExecuteScalarAsync<ulong>(
                $"SELECT finalizeAggregation(s) FROM {table}");
            Assert.Equal(50UL, rehydrated);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    // --- BulkInsert round-trip -----------------------------------------------

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task BulkInsert_CapturedSumState_RoundTripsAndFinalizes(string image)
    {
        // 1. Produce a sumState() on the server and pull its bytes via the new
        //    ClickHouseAggregateState reader.
        // 2. BulkInsert those states into a fresh table.
        // 3. finalizeAggregation in the new table must return the same scalar.
        await using var conn = await OpenAsync(image);
        var srcTable = $"bulk_agg_src_{Guid.NewGuid():N}";
        var dstTable = $"bulk_agg_dst_{Guid.NewGuid():N}";

        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {srcTable} (id Int32, s AggregateFunction(sum, Int32)) ENGINE = Memory");
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {dstTable} (id Int32, s AggregateFunction(sum, Int32)) ENGINE = Memory");
        try
        {
            // Seed src with a known state — sum(0..99) = 4950 — using the server's own producer.
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {srcTable} SELECT 1, sumState(toInt32(number)) FROM numbers(100)");

            // Read the state via the new reader.
            var rows = new List<AggregateRow>();
            await foreach (var r in conn.QueryAsync<AggregateRow>($"SELECT id, s FROM {srcTable}"))
                rows.Add(r);
            Assert.Single(rows);
            Assert.Equal("sum", rows[0].S.FunctionName);
            Assert.Equal(8, rows[0].S.State.Length);

            // Bulk-insert into dst — exercises AggregateFunctionColumnWriter end-to-end.
            await conn.Table<AggregateRow>(dstTable).InsertAsync(rows);

            // Finalize on dst — must equal 4950.
            var total = await conn.ExecuteScalarAsync<long>(
                $"SELECT toInt64(finalizeAggregation(s)) FROM {dstTable}");
            Assert.Equal(4950L, total);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {dstTable}");
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {srcTable}");
        }
    }

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task BulkInsert_SimpleAggregateFunction_RoundTrips(string image)
    {
        // SimpleAggregateFunction columns work transparently for bulk insert —
        // the property type is just the inner CLR type.
        await using var conn = await OpenAsync(image);
        var table = $"bulk_sagg_{Guid.NewGuid():N}";

        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, total SimpleAggregateFunction(sum, Int64)) " +
            $"ENGINE = AggregatingMergeTree ORDER BY id");
        try
        {
            var seed = new[]
            {
                new SimpleAggRow { Id = 1, Total = 100 },
                new SimpleAggRow { Id = 1, Total = 200 },
                new SimpleAggRow { Id = 2, Total = 5 },
            };
            await conn.Table<SimpleAggRow>(table).InsertAsync(seed);
            await conn.ExecuteNonQueryAsync($"OPTIMIZE TABLE {table} FINAL");

            var got = new Dictionary<int, long>();
            await foreach (var r in conn.QueryAsync<SimpleAggRow>(
                $"SELECT id, total FROM {table} ORDER BY id"))
            {
                got[r.Id] = r.Total;
            }
            Assert.Equal(300L, got[1]); // merged
            Assert.Equal(5L, got[2]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task BulkInsert_MalformedState_FailsWithLengthError(string image)
    {
        // A state byte[] of the wrong length for the function's wire format must
        // fail with a clear length-mismatch error. The failure point is the
        // registry's per-row validation in FixedSizeStateFormat.WriteOneState;
        // the message names the expected/actual byte count.
        await using var conn = await OpenAsync(image);
        var table = $"bulk_bad_state_{Guid.NewGuid():N}";

        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, s AggregateFunction(sum, Int32)) ENGINE = Memory");
        try
        {
            var bad = new[]
            {
                new AggregateRow
                {
                    Id = 1,
                    // sum(Int32) expects 8 bytes; pass 3 to force the validator's throw.
                    S = new ClickHouseAggregateState(new byte[] { 1, 2, 3 }, "sum"),
                },
            };

            var ex = await Assert.ThrowsAnyAsync<Exception>(
                () => conn.Table<AggregateRow>(table).InsertAsync(bad));

            // ArgumentException from the registry, possibly wrapped by the
            // BulkInserter's flush path. Either form should mention the expected
            // size and the actual size in the message chain.
            var combined = ex.Message + (ex.InnerException?.Message ?? string.Empty);
            Assert.Contains("8 bytes", combined);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }
}

internal sealed class AggregateRow
{
    [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
    [ClickHouseColumn(Name = "s", Order = 1)] public ClickHouseAggregateState S { get; set; } = ClickHouseAggregateState.Empty;
}

internal sealed class SimpleAggRow
{
    [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
    [ClickHouseColumn(Name = "total", Order = 1)] public long Total { get; set; }
}
