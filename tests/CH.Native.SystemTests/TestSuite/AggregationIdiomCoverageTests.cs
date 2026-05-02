using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.TestSuite;

/// <summary>
/// Pins .NET-side type mapping for the five most-common ClickHouse
/// aggregation idioms (use-cases §4.3). Each idiom round-trips as plain
/// SQL but the library-specific mapping (e.g. <c>quantiles(...)</c> →
/// <c>Array(Float64)</c> → <c>double[]</c>) is what would surface as a
/// runtime cast error if the mapping regressed.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class AggregationIdiomCoverageTests : IAsyncLifetime
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;
    private readonly string _table = $"agg_data_{Guid.NewGuid():N}";

    public AggregationIdiomCoverageTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    public async Task InitializeAsync()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {_table} (level String, country String, latency Float64, user_id Int32) " +
            "ENGINE = MergeTree ORDER BY level");

        var values = new List<string>();
        var rng = new Random(42);
        for (int i = 0; i < 1000; i++)
        {
            var level = i % 50 == 0 ? "ERROR" : "INFO";
            var country = i % 3 == 0 ? "US" : (i % 3 == 1 ? "UK" : "DE");
            var latency = 50 + rng.NextDouble() * 950; // 50..1000ms
            values.Add($"('{level}', '{country}', {latency:F2}, {i})");
        }
        await conn.ExecuteNonQueryAsync($"INSERT INTO {_table} VALUES {string.Join(",", values)}");
    }

    public async Task DisposeAsync()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {_table}");
    }

    [Fact]
    public async Task CountIf_MapsToInt64()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        var errorCount = await conn.ExecuteScalarAsync<ulong>(
            $"SELECT countIf(level = 'ERROR') FROM {_table}");

        _output.WriteLine($"countIf: {errorCount}");
        // 1000 / 50 = 20 ERRORs.
        Assert.Equal(20UL, errorCount);
    }

    [Fact]
    public async Task QuantileSingle_MapsToDouble()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        var p99 = await conn.ExecuteScalarAsync<double>(
            $"SELECT quantile(0.99)(latency) FROM {_table}");

        _output.WriteLine($"p99: {p99}");
        Assert.InRange(p99, 50, 1000);
    }

    [Fact]
    public async Task QuantilesArray_MapsToDoubleArray()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        var rows = new List<QuantilesRow>();
        await foreach (var row in conn.QueryAsync<QuantilesRow>(
            $"SELECT quantiles(0.5, 0.95, 0.99)(latency) AS qs FROM {_table}"))
        {
            rows.Add(row);
        }

        Assert.Single(rows);
        Assert.Equal(3, rows[0].Qs.Length);
        _output.WriteLine($"quantiles: [{string.Join(", ", rows[0].Qs)}]");
        Assert.True(rows[0].Qs[0] <= rows[0].Qs[1]);
        Assert.True(rows[0].Qs[1] <= rows[0].Qs[2]);
    }

    [Fact]
    public async Task UniqHll12_MapsToUInt64()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        var distinct = await conn.ExecuteScalarAsync<ulong>(
            $"SELECT uniqHLL12(user_id) FROM {_table}");

        _output.WriteLine($"uniqHLL12: {distinct}");
        // HLL is approximate — bound the answer by ±10% of the true value (1000).
        Assert.InRange((long)distinct, 900, 1100);
    }

    [Fact]
    public async Task TopK_MapsToStringArray()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        var rows = new List<TopKRow>();
        await foreach (var row in conn.QueryAsync<TopKRow>(
            $"SELECT topK(3)(country) AS countries FROM {_table}"))
        {
            rows.Add(row);
        }

        Assert.Single(rows);
        var topThree = rows[0].Countries;
        _output.WriteLine($"topK: [{string.Join(", ", topThree)}]");
        Assert.True(topThree.Length <= 3);
        // All three of US/UK/DE should appear.
        Assert.Contains("US", topThree);
    }

    [Fact]
    public async Task GroupArray_MapsToStringArray_ForPrimitiveElement()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        var rows = new List<GroupArrayRow>();
        await foreach (var row in conn.QueryAsync<GroupArrayRow>(
            $"SELECT groupArray(country) AS all_countries FROM (SELECT DISTINCT country FROM {_table})"))
        {
            rows.Add(row);
        }

        Assert.Single(rows);
        Assert.Contains("US", rows[0].AllCountries);
        Assert.Contains("UK", rows[0].AllCountries);
        Assert.Contains("DE", rows[0].AllCountries);
    }

    internal sealed class QuantilesRow
    {
        [ClickHouseColumn(Name = "qs")] public double[] Qs { get; set; } = Array.Empty<double>();
    }

    internal sealed class TopKRow
    {
        [ClickHouseColumn(Name = "countries")] public string[] Countries { get; set; } = Array.Empty<string>();
    }

    internal sealed class GroupArrayRow
    {
        [ClickHouseColumn(Name = "all_countries")] public string[] AllCountries { get; set; } = Array.Empty<string>();
    }
}
