using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Stress;

/// <summary>
/// LowCardinality dictionary-encoding boundary: ClickHouse switches index width
/// at 256 (UInt8 → UInt16) and 65 536 (UInt16 → UInt32). Existing tests cover
/// small dictionaries; the boundary transitions are precisely where off-by-one
/// reader/writer bugs hide.
///
/// <para>The 65 536 cases push 130 000+ rows per case and add ~30s to the suite.
/// They are tagged <see cref="Categories.LongBoundary"/> so CI excludes them by
/// default (<c>--filter "Category!=LongBoundary"</c>) and opts them into the
/// nightly run explicitly.</para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Stress)]
public class LowCardinalityBoundaryTests
{
    private readonly SingleNodeFixture _fixture;
    private readonly ITestOutputHelper _output;

    public LowCardinalityBoundaryTests(SingleNodeFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Theory]
    [InlineData(255)]
    [InlineData(256)]
    [InlineData(257)]
    public Task Uint8Boundary_RoundTripPreservesAllValues(int distinct)
        => RoundTripAsync(distinct);

    [Theory]
    [InlineData(65_535)]
    [InlineData(65_536)]
    [InlineData(65_537)]
    [Trait(Categories.Name, Categories.LongBoundary)]
    public Task Uint16Boundary_RoundTripPreservesAllValues(int distinct)
        => RoundTripAsync(distinct);

    private async Task RoundTripAsync(int distinct)
    {
        // 2× distinct rows so every dictionary entry is referenced at least twice
        // (catches reader bugs that mis-resolve a duplicate index).
        var rowCount = distinct * 2;
        var table = $"lc_boundary_{distinct}_{Guid.NewGuid():N}";

        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int64, val LowCardinality(String)) ENGINE = MergeTree ORDER BY id");

        try
        {
            var rows = new List<LowCardRow>(rowCount);
            for (int i = 0; i < rowCount; i++)
                rows.Add(new LowCardRow { Id = i, Val = $"v_{i % distinct}" });

            await using (var inserter = conn.CreateBulkInserter<LowCardRow>(table))
            {
                await inserter.InitAsync();
                await inserter.AddRangeAsync(rows);
                await inserter.CompleteAsync();
            }

            // Server-side cardinality matches request — no silent dedup or expansion.
            var distinctOnServer = await conn.ExecuteScalarAsync<ulong>(
                $"SELECT uniqExact(val) FROM {table}");
            Assert.Equal((ulong)distinct, distinctOnServer);

            // Per-row equality — i.e., reader resolves the index correctly across
            // the dictionary-width transition.
            int verified = 0;
            await foreach (var row in conn.QueryAsync<LowCardRow>($"SELECT id, val FROM {table} ORDER BY id"))
            {
                Assert.Equal($"v_{row.Id % distinct}", row.Val);
                verified++;
            }
            Assert.Equal(rowCount, verified);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    public sealed class LowCardRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public long Id { get; set; }

        [ClickHouseColumn(Name = "val", Order = 1)]
        public string Val { get; set; } = string.Empty;
    }
}
