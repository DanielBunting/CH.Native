using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Linq;

/// <summary>
/// Smoke test that the library reads materialised views the same way as
/// regular MergeTree tables. Use-cases §8.9 documents MVs as the canonical
/// way to keep dashboard reads fast; the library has no MV-specific code,
/// but a regression in the read path that broke MV consumption would be
/// expensive to ship.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Linq)]
public class MaterializedViewConsumerTests : IAsyncLifetime
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;
    private readonly string _source = $"events_src_{Guid.NewGuid():N}";
    private readonly string _target = $"events_hourly_{Guid.NewGuid():N}";
    private readonly string _mv = $"events_hourly_mv_{Guid.NewGuid():N}";

    public MaterializedViewConsumerTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    public async Task InitializeAsync()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {_source} (ts DateTime, level String, message String) " +
            "ENGINE = MergeTree ORDER BY ts");

        // SummingMergeTree as the MV target so groupings actually fold.
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {_target} (hour DateTime, level String, count UInt64) " +
            "ENGINE = SummingMergeTree(count) ORDER BY (hour, level)");

        await conn.ExecuteNonQueryAsync(
            $"CREATE MATERIALIZED VIEW {_mv} TO {_target} AS " +
            $"SELECT toStartOfHour(ts) AS hour, level, count() AS count FROM {_source} GROUP BY hour, level");

        // Populate source — MV pipes into target as a side effect.
        var values = new List<string>();
        var t0 = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        for (int i = 0; i < 60; i++)
        {
            var ts = t0.AddMinutes(i);
            var level = i % 3 == 0 ? "ERROR" : "INFO";
            values.Add($"('{ts:yyyy-MM-dd HH:mm:ss}', '{level}', 'm{i}')");
        }
        await conn.ExecuteNonQueryAsync(
            $"INSERT INTO {_source} VALUES {string.Join(",", values)}");
    }

    public async Task DisposeAsync()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();
        // Drop in dependency order: MV first, then target, then source.
        await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {_mv}");
        await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {_target}");
        await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {_source}");
    }

    [Fact]
    public async Task LinqQueryAgainstMVTargetTable_ReturnsRolledUpRows()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        var rows = await conn.Table<HourlyRow>(_target)
            .OrderBy(r => r.Hour).ThenBy(r => r.Level)
            .ToListAsync();

        _output.WriteLine($"MV-backed rows: {rows.Count}");
        // 60 minutes ÷ hour = 1 hour bucket. Two levels (INFO/ERROR) → 2 rows.
        Assert.True(rows.Count >= 1);
        Assert.Contains(rows, r => r.Level == "ERROR" && r.Count > 0);
        Assert.Contains(rows, r => r.Level == "INFO" && r.Count > 0);

        var total = rows.Sum(r => (long)r.Count);
        Assert.Equal(60, total);
    }

    [Fact]
    public async Task RawQueryAgainstMVTargetTable_ReturnsSameShapeAsMergeTreeRead()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        var total = await conn.ExecuteScalarAsync<ulong>($"SELECT sum(count) FROM {_target}");
        _output.WriteLine($"Raw query total: {total}");
        Assert.Equal(60UL, total);

        // Engine sanity: confirm the test setup actually built an MV+target.
        var engine = await conn.ExecuteScalarAsync<string>(
            $"SELECT engine FROM system.tables WHERE name = '{_mv}'");
        Assert.Equal("MaterializedView", engine);
    }

    internal sealed class HourlyRow
    {
        [ClickHouseColumn(Name = "hour")] public DateTime Hour { get; set; }
        [ClickHouseColumn(Name = "level")] public string Level { get; set; } = "";
        [ClickHouseColumn(Name = "count")] public ulong Count { get; set; }
    }
}
