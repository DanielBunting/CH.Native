using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using CH.Native.SystemTests.Linq.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Linq;

[Collection("LinqFacts")]
[Trait(Categories.Name, Categories.Linq)]
public class ClickHouseExtensionsTests : IAsyncLifetime
{
    private readonly SingleNodeFixture _node;
    private readonly LinqFactTableFixture _facts;
    private ClickHouseConnection _conn = null!;
    private string _replacingTable = $"linq_repl_{Guid.NewGuid():N}";
    private string _sampledTable = $"linq_sample_{Guid.NewGuid():N}";

    public ClickHouseExtensionsTests(SingleNodeFixture node, LinqFactTableFixture facts)
    {
        _node = node;
        _facts = facts;
    }

    public async Task InitializeAsync()
    {
        _conn = new ClickHouseConnection(_node.BuildSettings());
        await _conn.OpenAsync();
    }

    public async Task DisposeAsync()
    {
        try
        {
            await _conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {_replacingTable}");
            await _conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {_sampledTable}");
        }
        catch
        {
            // Best-effort cleanup; container will dispose anyway.
        }
        await _conn.DisposeAsync();
    }

    [Fact]
    public async Task Final_OnReplacingMergeTree_DeDuplicates()
    {
        // ReplacingMergeTree keyed on Id; insert two versions of id=1, id=2.
        await _conn.ExecuteNonQueryAsync($@"
            CREATE TABLE {_replacingTable} (
                id Int64,
                version Int64,
                payload String
            ) ENGINE = ReplacingMergeTree(version) ORDER BY id");

        var rows = new List<ReplRow>
        {
            new() { Id = 1, Version = 1, Payload = "old" },
            new() { Id = 1, Version = 2, Payload = "new" },
            new() { Id = 2, Version = 5, Payload = "v5" },
            new() { Id = 2, Version = 9, Payload = "v9" },
        };

        await using (var inserter = _conn.CreateBulkInserter<ReplRow>(_replacingTable))
        {
            await inserter.InitAsync();
            await inserter.AddRangeAsync(rows);
            await inserter.CompleteAsync();
        }

        // OPTIMIZE FINAL forces the merge so FINAL has something concrete to dedupe.
        await _conn.ExecuteNonQueryAsync($"OPTIMIZE TABLE {_replacingTable} FINAL");

        var deduped = await _conn.Table<ReplRow>(_replacingTable)
            .Final()
            .OrderBy(r => r.Id)
            .ToListAsync();

        Assert.Equal(2, deduped.Count);
        Assert.Equal("new", deduped[0].Payload);
        Assert.Equal("v9", deduped[1].Payload);
    }

    [Fact]
    public async Task Sample_Ratio_ReducesRowCount_Roughly()
    {
        // Build a sampled MergeTree with 100k rows. Use intHash32 so sampling has
        // the spread it needs.
        await _conn.ExecuteNonQueryAsync($@"
            CREATE TABLE {_sampledTable} (
                id Int64,
                payload String
            ) ENGINE = MergeTree()
            ORDER BY (intHash32(id), id)
            SAMPLE BY intHash32(id)");

        // Bulk insert 100k rows.
        await using (var inserter = _conn.CreateBulkInserter<SampleRow>(_sampledTable))
        {
            await inserter.InitAsync();
            for (int i = 0; i < 100_000; i++)
            {
                await inserter.AddAsync(new SampleRow { Id = i, Payload = "x" });
            }
            await inserter.CompleteAsync();
        }

        int sampledCount = await _conn.Table<SampleRow>(_sampledTable)
            .Sample(0.1)
            .CountAsync();

        // Sampling is approximate: allow generous tolerance (5k..20k around 10k).
        Assert.InRange(sampledCount, 5_000, 20_000);
    }

    [Fact]
    public async Task Final_Combined_With_Where()
    {
        // Same setup as the previous test, but queried alongside a WHERE.
        await _conn.ExecuteNonQueryAsync($@"
            CREATE TABLE {_replacingTable}_w (
                id Int64,
                version Int64,
                payload String
            ) ENGINE = ReplacingMergeTree(version) ORDER BY id");

        var rows = new List<ReplRow>
        {
            new() { Id = 1, Version = 1, Payload = "old" },
            new() { Id = 1, Version = 2, Payload = "new" },
            new() { Id = 2, Version = 9, Payload = "keep" },
            new() { Id = 3, Version = 7, Payload = "drop-via-where" },
        };

        try
        {
            await using (var inserter = _conn.CreateBulkInserter<ReplRow>($"{_replacingTable}_w"))
            {
                await inserter.InitAsync();
                await inserter.AddRangeAsync(rows);
                await inserter.CompleteAsync();
            }
            await _conn.ExecuteNonQueryAsync($"OPTIMIZE TABLE {_replacingTable}_w FINAL");

            var result = await _conn.Table<ReplRow>($"{_replacingTable}_w")
                .Final()
                .Where(r => r.Id <= 2)
                .OrderBy(r => r.Id)
                .ToListAsync();

            Assert.Equal(2, result.Count);
            Assert.Equal("new", result[0].Payload);
            Assert.Equal("keep", result[1].Payload);
        }
        finally
        {
            await _conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {_replacingTable}_w");
        }
    }

    public class ReplRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public long Id { get; set; }
        [ClickHouseColumn(Name = "version", Order = 1)] public long Version { get; set; }
        [ClickHouseColumn(Name = "payload", Order = 2)] public string Payload { get; set; } = string.Empty;
    }

    public class SampleRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public long Id { get; set; }
        [ClickHouseColumn(Name = "payload", Order = 1)] public string Payload { get; set; } = string.Empty;
    }
}
