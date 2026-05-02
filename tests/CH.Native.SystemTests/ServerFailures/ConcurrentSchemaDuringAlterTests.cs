using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.ServerFailures;

/// <summary>
/// Concurrent schema reads (via <c>system.columns</c>) while another connection
/// runs <c>ALTER TABLE</c>. Real tooling (Dapper schema reflection, query builders,
/// migration tools) issues these in parallel, and the contract here is that
/// schema-reader connections never throw and never observe a half-applied
/// schema — only complete pre-ALTER or complete post-ALTER snapshots.
///
/// <para>The native ADO.NET <c>GetSchema("Columns")</c> override is not yet
/// implemented in <c>ClickHouseDbConnection</c>, so this test queries
/// <c>system.columns</c> directly. The concurrency story is unaffected by
/// where the schema metadata is fetched from.</para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.ServerFailures)]
public class ConcurrentSchemaDuringAlterTests
{
    private readonly SingleNodeFixture _fixture;
    private readonly ITestOutputHelper _output;

    public ConcurrentSchemaDuringAlterTests(SingleNodeFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public async Task ConcurrentSchemaReadsDuringAlter_NeverThrow_AndConverge()
    {
        var table = $"alter_concurrency_{Guid.NewGuid():N}";

        await using var setup = new ClickHouseConnection(_fixture.BuildSettings());
        await setup.OpenAsync();
        await setup.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int64, name String) ENGINE = MergeTree ORDER BY id");

        try
        {
            // Sets that are valid at any point during the alter cycle. The reader
            // must always observe one of these — never a partial set, never throw.
            var originalCols = new HashSet<string> { "id", "name" };
            var afterAddCols = new HashSet<string> { "id", "name", "extra" };
            var validShapes = new[] { originalCols, afterAddCols };

            using var stop = new CancellationTokenSource();
            int readCount = 0;
            int observedAdd = 0;
            int observedDrop = 0;

            await using var connA = new ClickHouseConnection(_fixture.BuildSettings());
            await connA.OpenAsync();

            var readerLoop = Task.Run(async () =>
            {
                while (!stop.IsCancellationRequested)
                {
                    var cols = await ReadColumnNamesAsync(connA, table);
                    Interlocked.Increment(ref readCount);

                    // Every snapshot must match one of the well-defined shapes —
                    // never partial / corrupted.
                    var matched = validShapes.Any(s => s.SetEquals(cols));
                    Assert.True(matched,
                        $"Unexpected column set during ALTER: [{string.Join(",", cols)}]");

                    if (cols.Contains("extra")) Interlocked.Increment(ref observedAdd);
                    else if (cols.SetEquals(originalCols) && readCount > 1) Interlocked.Increment(ref observedDrop);
                }
            });

            // Connection B drives ALTER ADD then ALTER DROP, repeated a few times so
            // the reader has multiple chances to land mid-transition.
            await using var connB = new ClickHouseConnection(_fixture.BuildSettings());
            await connB.OpenAsync();

            for (int i = 0; i < 5; i++)
            {
                await connB.ExecuteNonQueryAsync(
                    $"ALTER TABLE {table} ADD COLUMN extra String DEFAULT ''");
                await Task.Delay(150);
                await connB.ExecuteNonQueryAsync(
                    $"ALTER TABLE {table} DROP COLUMN extra");
                await Task.Delay(150);
            }

            // Final settling delay so the reader's last observation is post-DROP.
            await Task.Delay(500);
            stop.Cancel();
            await readerLoop;

            _output.WriteLine($"Reads: {readCount}, observedAdd: {observedAdd}, observedDrop: {observedDrop}");

            // Convergence: A's last read must show the original (post-DROP) schema.
            var finalCols = await ReadColumnNamesAsync(connA, table);
            Assert.True(finalCols.SetEquals(originalCols),
                $"Final schema didn't converge to original: [{string.Join(",", finalCols)}]");

            // Sanity: the reader actually ran the loop multiple times during the
            // alter window — without this, the test reduces to "read once, no throw"
            // which doesn't cover the concurrency contract.
            Assert.True(readCount >= 5,
                $"Expected ≥ 5 reads during the alter cycle; saw {readCount}.");
        }
        finally
        {
            await setup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    private static async Task<HashSet<string>> ReadColumnNamesAsync(
        ClickHouseConnection conn, string table)
    {
        var cols = new HashSet<string>(StringComparer.Ordinal);
        await foreach (var row in conn.QueryAsync(
            $"SELECT name FROM system.columns WHERE database = currentDatabase() AND table = '{table}' ORDER BY position"))
        {
            var name = row[0] as string;
            Assert.False(string.IsNullOrEmpty(name),
                "system.columns returned a row with null/empty name — partial schema observed.");
            cols.Add(name!);
        }
        return cols;
    }
}
