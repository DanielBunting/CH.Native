using CH.Native.Connection;
using CH.Native.Data.AggregateState;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.VersionMatrix;

/// <summary>
/// Per-function round-trip coverage for the tier-1 <c>AggregateFunction</c> set
/// across <see cref="SupportedImages.All"/>. ClickHouse offers no compatibility
/// guarantee for the per-function state binary format across versions; these tests
/// are the safety net against silent drift when a new image is pinned.
///
/// Each test creates a source table + <c>AggregatingMergeTree</c> MV, inserts a
/// small fixed payload, then asserts:
/// <list type="bullet">
/// <item>The state column reads as <see cref="ClickHouseAggregateState"/> with the
/// expected byte length and bare function name (no <c>State</c> suffix).</item>
/// <item><c>finalizeAggregation()</c> on the same column returns the expected
/// scalar — proves the bytes we read are the bytes ClickHouse produced.</item>
/// </list>
/// One <c>[Theory]</c> per supported function rather than a parameterised
/// mega-test, so a regression in <c>min</c> doesn't mask a regression in <c>max</c>.
/// </summary>
[Collection("VersionMatrix")]
[Trait(Categories.Name, Categories.VersionMatrix)]
public class AggregateFunctionMatrixTests
{
    private readonly VersionedNodeCache _cache;

    public AggregateFunctionMatrixTests(VersionedNodeCache cache) => _cache = cache;

    private async Task<ClickHouseConnection> OpenAsync(string image)
    {
        var settings = await _cache.GetSettingsAsync(image);
        var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();
        return conn;
    }

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public Task SumState_Int32_RoundTrips(string image)
        => RunRoundTrip(image, "sum", "Int32", expectedStateBytes: 8,
            inserts: new[] { (1, 10), (1, 20), (2, 5) },
            expectedTotals: new Dictionary<int, long> { { 1, 30 }, { 2, 5 } });

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public Task SumState_Int64_RoundTrips(string image)
        => RunRoundTrip(image, "sum", "Int64", expectedStateBytes: 8,
            inserts: new[] { (1, 1000), (1, 2000), (2, 500) },
            expectedTotals: new Dictionary<int, long> { { 1, 3000 }, { 2, 500 } });

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public Task SumState_Float64_RoundTrips(string image)
        => RunRoundTrip(image, "sum", "Float64", expectedStateBytes: 8,
            inserts: new[] { (1, 10), (1, 20), (2, 5) },
            expectedTotals: new Dictionary<int, long> { { 1, 30 }, { 2, 5 } });

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public Task MinState_Int32_RoundTrips(string image)
        => RunRoundTrip(image, "min", "Int32", expectedStateBytes: 5,
            inserts: new[] { (1, 20), (1, 5), (1, 10), (2, 99) },
            expectedTotals: new Dictionary<int, long> { { 1, 5 }, { 2, 99 } });

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public Task MaxState_Int32_RoundTrips(string image)
        => RunRoundTrip(image, "max", "Int32", expectedStateBytes: 5,
            inserts: new[] { (1, 5), (1, 20), (1, 10), (2, 7) },
            expectedTotals: new Dictionary<int, long> { { 1, 20 }, { 2, 7 } });

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public Task AnyState_Int32_RoundTrips(string image)
        => RunRoundTrip(image, "any", "Int32",
            expectedStateBytes: 5,
            inserts: new[] { (1, 10), (2, 99) },
            // any() picks an arbitrary value — assert the state isn't empty
            // rather than a specific number.
            expectedTotals: null);

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public Task AnyLastState_Int32_RoundTrips(string image)
        => RunRoundTrip(image, "anyLast", "Int32",
            expectedStateBytes: 5,
            inserts: new[] { (1, 10), (2, 99) },
            expectedTotals: null);

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task CountState_NoArg_RoundTrips(string image)
    {
        await using var conn = await OpenAsync(image);
        var src = $"agg_count_src_{Guid.NewGuid():N}";
        var mv = $"agg_count_mv_{Guid.NewGuid():N}";

        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {src} (id Int32, v Int32) ENGINE=MergeTree ORDER BY id");
        await conn.ExecuteNonQueryAsync(
            $"CREATE MATERIALIZED VIEW {mv} ENGINE=AggregatingMergeTree ORDER BY id AS " +
            $"SELECT id, countState() AS c FROM {src} GROUP BY id");

        try
        {
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {src} VALUES (1, 10), (1, 20), (1, 30), (2, 5)");

            var states = new Dictionary<int, ClickHouseAggregateState>();
            await foreach (var r in conn.StreamAsync($"SELECT id, c FROM {mv} ORDER BY id"))
            {
                states[r.GetFieldValue<int>("id")] = r.GetFieldValue<ClickHouseAggregateState>("c");
            }

            Assert.Equal(2, states.Count);
            Assert.All(states.Values, s => Assert.Equal("count", s.FunctionName));
            Assert.All(states.Values, s => Assert.NotEmpty(s.State));

            var totals = new Dictionary<int, long>();
            await foreach (var r in conn.StreamAsync(
                $"SELECT id, toInt64(finalizeAggregation(c)) AS t FROM {mv} ORDER BY id"))
            {
                totals[r.GetFieldValue<int>("id")] = r.GetFieldValue<long>("t");
            }
            Assert.Equal(3L, totals[1]);
            Assert.Equal(1L, totals[2]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {mv}");
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {src}");
        }
    }

    /// <summary>
    /// Shared round-trip helper used by <c>sum</c>/<c>min</c>/<c>max</c>/<c>any</c>/<c>anyLast</c>.
    /// Each test instantiates source + MV, inserts the payload, then validates the
    /// state column shape and (optionally) the server-side finalized scalar.
    /// </summary>
    private async Task RunRoundTrip(
        string image,
        string aggregateFn,
        string innerType,
        int expectedStateBytes,
        (int id, int v)[] inserts,
        Dictionary<int, long>? expectedTotals)
    {
        await using var conn = await OpenAsync(image);
        var src = $"agg_src_{Guid.NewGuid():N}";
        var mv = $"agg_mv_{Guid.NewGuid():N}";

        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {src} (id Int32, v {innerType}) ENGINE=MergeTree ORDER BY id");
        await conn.ExecuteNonQueryAsync(
            $"CREATE MATERIALIZED VIEW {mv} ENGINE=AggregatingMergeTree ORDER BY id AS " +
            $"SELECT id, {aggregateFn}State(v) AS s FROM {src} GROUP BY id");

        try
        {
            // Single batched INSERT so the MV's GROUP BY id collapses all rows
            // for the same id into one partial state. Separate INSERTs would
            // leave one unmerged row per insert in the AggregatingMergeTree,
            // and per-row finalizeAggregation on those would not match the
            // expected aggregate (it would return the value of one arbitrary
            // partial state, not the merged total). The CountState test in
            // this file uses the same batched-INSERT pattern.
            var values = string.Join(", ", inserts.Select(t => $"({t.id}, {t.v})"));
            await conn.ExecuteNonQueryAsync($"INSERT INTO {src} VALUES {values}");

            var states = new Dictionary<int, ClickHouseAggregateState>();
            await foreach (var r in conn.StreamAsync($"SELECT id, s FROM {mv} ORDER BY id"))
            {
                states[r.GetFieldValue<int>("id")] = r.GetFieldValue<ClickHouseAggregateState>("s");
            }

            Assert.NotEmpty(states);
            Assert.All(states.Values, s => Assert.Equal(aggregateFn, s.FunctionName));
            Assert.All(states.Values, s => Assert.Equal(expectedStateBytes, s.State.Length));

            if (expectedTotals is not null)
            {
                var totals = new Dictionary<int, long>();
                await foreach (var r in conn.StreamAsync(
                    $"SELECT id, toInt64(finalizeAggregation(s)) AS t FROM {mv} ORDER BY id"))
                {
                    totals[r.GetFieldValue<int>("id")] = r.GetFieldValue<long>("t");
                }
                foreach (var (id, expected) in expectedTotals)
                    Assert.Equal(expected, totals[id]);
            }
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {mv}");
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {src}");
        }
    }
}
