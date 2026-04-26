using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Linq.Fixtures;

/// <summary>
/// Provisions a single shared 1000-row fact table for the LINQ system-test suite.
/// Reuses <see cref="SingleNodeFixture"/>'s container so we don't pay a second startup.
/// </summary>
public class LinqFactTableFixture : IAsyncLifetime
{
    private const int RowCount = 1000;
    private static readonly string[] Countries = ["US", "UK", "DE", "FR", "JP"];

    private readonly SingleNodeFixture _node;

    public LinqFactTableFixture(SingleNodeFixture node)
    {
        _node = node;
    }

    /// <summary>
    /// Globally unique table name for this run. Use it via
    /// <c>connection.Table&lt;LinqFactRow&gt;(fixture.TableName)</c>.
    /// </summary>
    public string TableName { get; } = $"linq_facts_{Guid.NewGuid():N}";

    /// <summary>
    /// In-memory copy of the seeded rows so tests can compute oracle values without a round-trip.
    /// </summary>
    public IReadOnlyList<LinqFactRow> Rows { get; private set; } = Array.Empty<LinqFactRow>();

    public ClickHouseConnectionSettings BuildSettings(
        Action<ClickHouseConnectionSettingsBuilder>? configure = null)
        => _node.BuildSettings(configure);

    public async Task InitializeAsync()
    {
        await using var conn = new ClickHouseConnection(_node.BuildSettings());
        await conn.OpenAsync();

        await conn.ExecuteNonQueryAsync($@"
            CREATE TABLE {TableName} (
                id Int64,
                country LowCardinality(String),
                amount Float64,
                quantity Int32,
                optional_code Nullable(Int32),
                name String,
                created_at DateTime,
                active UInt8
            ) ENGINE = MergeTree() ORDER BY id");

        var rows = BuildRows();
        Rows = rows;

        await using var inserter = conn.CreateBulkInserter<LinqFactRow>(TableName);
        await inserter.InitAsync();
        await inserter.AddRangeAsync(rows);
        await inserter.CompleteAsync();
    }

    public async Task DisposeAsync()
    {
        try
        {
            await using var conn = new ClickHouseConnection(_node.BuildSettings());
            await conn.OpenAsync();
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {TableName}");
        }
        catch
        {
            // Container may already be tearing down.
        }
    }

    private static List<LinqFactRow> BuildRows()
    {
        var rows = new List<LinqFactRow>(RowCount);
        var rand = new Random(42); // deterministic seed
        var baseDate = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        var nameBank = new[]
        {
            "foobar", "afoo", "barfoo", "Café", "alpha", "beta",
            "gamma", "delta", "epsilon", "zeta",
        };

        for (int i = 1; i <= RowCount; i++)
        {
            // Deterministic but varied amount roughly in [-200, 1000].
            double amount = Math.Round((rand.NextDouble() * 1200.0) - 200.0, 2);

            // Every 7th row has a NULL optional_code; otherwise 0..99.
            int? optional = i % 7 == 0 ? null : rand.Next(0, 100);

            string name = i switch
            {
                1 => "foobar",
                2 => "afoo",
                3 => "barfoo",
                4 => "Café",
                5 => "O'Brien", // single-quote round-trip
                _ => nameBank[i % nameBank.Length],
            };

            rows.Add(new LinqFactRow
            {
                Id = i,
                Country = Countries[(i - 1) % Countries.Length],
                Amount = amount,
                Quantity = ((i - 1) % 50) + 1, // 1..50 cycle
                OptionalCode = optional,
                Name = name,
                CreatedAt = baseDate.AddMinutes(i),
                Active = (byte)(i % 2),
            });
        }

        return rows;
    }
}

/// <summary>
/// Couples the ClickHouse container fixture and the seeded fact table fixture
/// into one xUnit collection so both are shared across every test class in the
/// LINQ system-test suite.
/// </summary>
[CollectionDefinition("LinqFacts")]
public class LinqFactTableCollection
    : ICollectionFixture<SingleNodeFixture>,
      ICollectionFixture<LinqFactTableFixture>
{
}
