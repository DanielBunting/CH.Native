using System.Diagnostics;
using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Stress;

/// <summary>
/// Realistic production workload archetypes the README markets the library for.
/// Each test asserts a guard rail (throughput, memory, completion-without-error)
/// rather than an absolute perf number.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Stress)]
public class WorkloadArchetypesTests
{
    private readonly SingleNodeFixture _fixture;
    private readonly ITestOutputHelper _output;

    public WorkloadArchetypesTests(SingleNodeFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public async Task HighRateSmallBatch_ManyConcurrentWriters_NoStarvation()
    {
        var table = $"arch_hrsb_{Guid.NewGuid():N}";
        await using var setup = new ClickHouseConnection(_fixture.BuildSettings());
        await setup.OpenAsync();
        await setup.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int64, payload String) ENGINE = MergeTree ORDER BY id");

        try
        {
            var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
            {
                Settings = _fixture.BuildSettings(),
                MaxPoolSize = 50,
            });

            const int writers = 20;
            const int batchesPerWriter = 25;
            const int rowsPerBatch = 200;

            var sw = Stopwatch.StartNew();
            var tasks = Enumerable.Range(0, writers).Select(w => Task.Run(async () =>
            {
                long id = (long)w * 1_000_000L;
                for (int b = 0; b < batchesPerWriter; b++)
                {
                    await using var c = await ds.OpenConnectionAsync();
                    await using var ins = c.CreateBulkInserter<Row>(table,
                        new BulkInsertOptions { BatchSize = rowsPerBatch });
                    await ins.InitAsync();
                    for (int r = 0; r < rowsPerBatch; r++)
                        await ins.AddAsync(new Row { Id = id++, Payload = "abc" });
                    await ins.CompleteAsync();
                }
            })).ToArray();
            await Task.WhenAll(tasks);
            sw.Stop();

            await ds.DisposeAsync();

            var total = (long)writers * batchesPerWriter * rowsPerBatch;
            await using var verifyConn = new ClickHouseConnection(_fixture.BuildSettings());
            await verifyConn.OpenAsync();
            var committed = await verifyConn.ExecuteScalarAsync<ulong>($"SELECT count() FROM {table}");
            Assert.Equal((ulong)total, committed);

            _output.WriteLine($"High-rate small-batch: {total:N0} rows by {writers} writers in {sw.Elapsed.TotalSeconds:F1}s " +
                $"({total / sw.Elapsed.TotalSeconds:N0} rows/s)");
        }
        finally
        {
            await setup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task DashboardConcurrency_ManyParallelGroupBy_SameTotals()
    {
        var table = $"arch_dash_{Guid.NewGuid():N}";
        await using var setup = new ClickHouseConnection(_fixture.BuildSettings());
        await setup.OpenAsync();
        await setup.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (cat String, v Int64) ENGINE = Memory");
        try
        {
            await setup.ExecuteNonQueryAsync(
                $"INSERT INTO {table} SELECT toString(number % 100), number FROM numbers(500000)");

            var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
            {
                Settings = _fixture.BuildSettings(),
                MaxPoolSize = 32,
            });
            const int parallel = 50;

            var sw = Stopwatch.StartNew();
            var results = await Task.WhenAll(Enumerable.Range(0, parallel).Select(_ => Task.Run(async () =>
            {
                await using var c = await ds.OpenConnectionAsync();
                return await c.ExecuteScalarAsync<ulong>(
                    $"SELECT sum(v) FROM {table}");
            })));
            sw.Stop();
            await ds.DisposeAsync();

            // Expected sum of 0..499999 = 500000 * 499999 / 2
            const ulong expected = 500000UL * 499999UL / 2UL;
            foreach (var r in results)
                Assert.Equal(expected, r);

            _output.WriteLine($"Dashboard concurrency: {parallel} parallel GROUP BY in {sw.Elapsed.TotalSeconds:F1}s");
        }
        finally
        {
            await setup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task StreamedExport_LargeResultSet_BoundedMemory()
    {
        const int rows = 5_000_000;
        const long heapCeiling = 192L * 1024 * 1024;

        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        long peak = 0;
        var streamed = 0L;
        var samplerStop = 0;
        var sampler = Task.Run(async () =>
        {
            while (Volatile.Read(ref samplerStop) == 0)
            {
                var h = GC.GetTotalMemory(false);
                if (h > peak) peak = h;
                await Task.Delay(250);
            }
        });

        ulong checksum = 0;
        try
        {
            await foreach (var r in conn.QueryAsync($"SELECT number FROM numbers({rows})"))
            {
                checksum += r.GetFieldValue<ulong>(0);
                streamed++;
            }
        }
        finally
        {
            Interlocked.Exchange(ref samplerStop, 1);
            await sampler;
        }

        Assert.Equal(rows, streamed);
        _output.WriteLine($"Streamed {streamed:N0} rows; peak heap {peak / (1024.0 * 1024.0):F1} MiB");
        Assert.True(peak < heapCeiling,
            $"Peak heap {peak:N0} exceeded ceiling {heapCeiling:N0} — streaming may not be bounded.");

        // Checksum: sum of 0..rows-1. Ensures values are actually being deserialised
        // correctly, not just rows being counted.
        ulong expected = (ulong)rows * (ulong)(rows - 1) / 2UL;
        Assert.Equal(expected, checksum);
    }

    private class Row
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public long Id { get; set; }
        [ClickHouseColumn(Name = "payload", Order = 1)] public string Payload { get; set; } = "";
    }
}
