using System.Diagnostics;
using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Stress;

/// <summary>
/// Concurrent inserters + readers + periodic DDL on the same table — closer to a
/// production multi-tenant workload than the existing single-axis stress tests.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Stress)]
public class MixedWorkloadContentionTests
{
    private readonly SingleNodeFixture _fixture;
    private readonly ITestOutputHelper _output;

    public MixedWorkloadContentionTests(SingleNodeFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public async Task WritersReadersDdl_NoDeadlocks_LatencyBounded()
    {
        var table = $"mix_{Guid.NewGuid():N}";

        await using var setup = new ClickHouseConnection(_fixture.BuildSettings());
        await setup.OpenAsync();
        await setup.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int64, k String, v Float64, t DateTime) " +
            "ENGINE = MergeTree ORDER BY (k, id) TTL t + INTERVAL 1 YEAR");

        var duration = TimeSpan.FromSeconds(30);
        var deadline = DateTime.UtcNow + duration;
        var insertCount = 0L;
        var queryCount = 0L;
        var ddlCount = 0L;
        var failures = new List<Exception>();

        try
        {
            var inserters = Enumerable.Range(0, 4).Select(workerId => Task.Run(async () =>
            {
                try
                {
                    await using var c = new ClickHouseConnection(_fixture.BuildSettings());
                    await c.OpenAsync();
                    long nextId = workerId * 1_000_000;
                    while (DateTime.UtcNow < deadline)
                    {
                        await using var inserter = c.CreateBulkInserter<Row>(table,
                            new BulkInsertOptions { BatchSize = 2000 });
                        await inserter.InitAsync();
                        for (int j = 0; j < 2000 && DateTime.UtcNow < deadline; j++)
                            await inserter.AddAsync(new Row
                            {
                                Id = nextId++,
                                K = $"k{workerId}_{j % 10}",
                                V = j,
                                T = DateTime.UtcNow
                            });
                        await inserter.CompleteAsync();
                        Interlocked.Add(ref insertCount, 2000);
                    }
                }
                catch (Exception ex) { lock (failures) failures.Add(ex); }
            })).ToArray();

            var readerLatencies = new List<long>();
            var readers = Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
            {
                try
                {
                    await using var c = new ClickHouseConnection(_fixture.BuildSettings());
                    await c.OpenAsync();
                    while (DateTime.UtcNow < deadline)
                    {
                        var sw = Stopwatch.StartNew();
                        ulong? count = await c.ExecuteScalarAsync<ulong>(
                            $"SELECT count() FROM {table} WHERE v > 0");
                        sw.Stop();
                        lock (readerLatencies) readerLatencies.Add(sw.ElapsedMilliseconds);
                        Interlocked.Increment(ref queryCount);
                        if (count is null) { /* should not happen */ }
                    }
                }
                catch (Exception ex) { lock (failures) failures.Add(ex); }
            })).ToArray();

            var ddl = Task.Run(async () =>
            {
                try
                {
                    await using var c = new ClickHouseConnection(_fixture.BuildSettings());
                    await c.OpenAsync();
                    while (DateTime.UtcNow < deadline)
                    {
                        await c.ExecuteNonQueryAsync($"OPTIMIZE TABLE {table}");
                        Interlocked.Increment(ref ddlCount);
                        await Task.Delay(5000);
                    }
                }
                catch (Exception ex) { lock (failures) failures.Add(ex); }
            });

            await Task.WhenAll(inserters.Concat(readers).Append(ddl));

            _output.WriteLine($"Mixed workload over {duration}");
            _output.WriteLine($"  Inserts: {insertCount:N0}");
            _output.WriteLine($"  Queries: {queryCount:N0}");
            _output.WriteLine($"  OPTIMIZE: {ddlCount}");
            _output.WriteLine($"  Failures: {failures.Count}");

            // We expect transient errors (e.g. "too many parts" code 252) under heavy
            // OPTIMIZE — but no exceptions specific to client-side state corruption.
            var corruptionLikelyMessages = failures
                .Select(f => f.Message)
                .Where(m =>
                    m.Contains("ObjectDisposed", StringComparison.OrdinalIgnoreCase)
                    || m.Contains("NullReference", StringComparison.OrdinalIgnoreCase)
                    || m.Contains("InvalidOperation", StringComparison.OrdinalIgnoreCase))
                .ToList();
            Assert.Empty(corruptionLikelyMessages);

            // Workload must have made progress.
            Assert.True(insertCount > 0, "No inserts completed during the run.");
            Assert.True(queryCount > 0, "No queries completed during the run.");

            // Latency budget: even with contention, p99 should stay within 10s.
            // Failure here means OPTIMIZE is starving readers or the pool is starved.
            if (readerLatencies.Count > 0)
            {
                readerLatencies.Sort();
                var p99 = readerLatencies[(int)(readerLatencies.Count * 0.99)];
                _output.WriteLine($"  Reader p99 latency: {p99}ms (n={readerLatencies.Count})");
                Assert.True(p99 < 10_000,
                    $"Reader p99 latency {p99}ms exceeds 10s budget — contention is unbounded.");
            }

            // Failure rate must be a small fraction of total operations.
            var totalOps = insertCount + queryCount;
            Assert.True(failures.Count < totalOps * 0.05,
                $"Failure rate {failures.Count}/{totalOps} > 5%; first failure: {failures.FirstOrDefault()?.Message}");
        }
        finally
        {
            await setup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    private class Row
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public long Id { get; set; }
        [ClickHouseColumn(Name = "k", Order = 1)] public string K { get; set; } = "";
        [ClickHouseColumn(Name = "v", Order = 2)] public double V { get; set; }
        [ClickHouseColumn(Name = "t", Order = 3)] public DateTime T { get; set; }
    }
}
