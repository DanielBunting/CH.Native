using CH.Native.Connection;
using CH.Native.Data;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Observability;

/// <summary>
/// Validates the protocol-level observability hooks: <c>IProgress&lt;QueryProgress&gt;</c>
/// receives multiple progress reports for slow queries, and <c>WITH TOTALS</c> /
/// <c>WITH EXTREMES</c> queries don't crash the reader.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Observability)]
public class ProgressAndTotalsTests
{
    private readonly SingleNodeFixture _fixture;

    public ProgressAndTotalsTests(SingleNodeFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task LongQuery_FiresMultipleProgressReports()
    {
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        var reports = new List<QueryProgress>();
        // Progress<T> dispatches via threadpool which can reorder reports; use a direct
        // synchronous implementation so RowsRead monotonicity is preserved.
        var progress = new SynchronousProgress<QueryProgress>(p =>
        {
            lock (reports) reports.Add(p);
        });

        // Force a multi-second single-threaded scan so the server has time to emit
        // multiple Progress packets at its default interactive_delay.
        _ = await conn.ExecuteScalarAsync<ulong>(
            "SELECT count() FROM numbers(2000000000) " +
            "SETTINGS max_threads = 1, max_block_size = 65536",
            progress: progress);

        // Progress<T> dispatches callbacks via SynchronizationContext / threadpool —
        // give them a moment to land before asserting.
        await Task.Delay(200);

        // Test name says "Multiple" — pin at least 3 progress packets for a query of
        // this size. CH's default interactive_delay emits one every ~100ms; a multi-
        // second single-threaded scan must produce several.
        Assert.True(reports.Count >= 3,
            $"Expected ≥ 3 progress reports, saw {reports.Count}");

        // At least one report should carry a non-trivial RowsRead.
        Assert.True(reports.Any(p => p.RowsRead > 0),
            "All progress reports had RowsRead == 0; instrumentation looks broken.");

        // Across the run the total work observed should grow — last report's RowsRead
        // (snapshot) or sum of deltas should exceed the first.
        var maxRowsRead = reports.Max(p => p.RowsRead);
        var minRowsRead = reports.Min(p => p.RowsRead);
        Assert.True(maxRowsRead > minRowsRead,
            $"Progress reports never grew: min={minRowsRead}, max={maxRowsRead}.");
    }

    [Fact]
    public async Task QueryWithTotals_StreamsWithoutError()
    {
        var table = $"totals_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (k String, v Int32) ENGINE = Memory");

        try
        {
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES ('a', 1), ('a', 2), ('b', 3), ('b', 4), ('c', 5)");

            var observedKeys = new List<string>();
            var observedSums = new List<long>();
            await foreach (var r in conn.QueryAsync(
                $"SELECT k, sum(v) FROM {table} GROUP BY k WITH TOTALS"))
            {
                observedKeys.Add(r.GetFieldValue<string>(0));
                observedSums.Add(r.GetFieldValue<long>(1));
            }
            // Server emits 3 grouped rows. The totals block may or may not be surfaced
            // by the iterator; pin both modes (3 = grouped only, 4 = grouped + totals).
            Assert.True(observedKeys.Count == 3 || observedKeys.Count == 4,
                $"Expected exactly 3 (groups) or 4 (groups+totals) rows, saw {observedKeys.Count}.");
            // Verify the grouped sums for the keys we know.
            for (int i = 0; i < observedKeys.Count; i++)
            {
                if (observedKeys[i] == "a") Assert.Equal(3L, observedSums[i]);
                if (observedKeys[i] == "b") Assert.Equal(7L, observedSums[i]);
                if (observedKeys[i] == "c") Assert.Equal(5L, observedSums[i]);
            }
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task QueryWithExtremes_StreamsWithoutError()
    {
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        var seen = new List<ulong>();
        await foreach (var r in conn.QueryAsync(
            "SELECT number AS x FROM numbers(10) SETTINGS extremes = 1"))
        {
            seen.Add(r.GetFieldValue<ulong>(0));
        }
        // Pin both modes: 10 (data only) or 12 (data + 2-row extremes block).
        Assert.True(seen.Count == 10 || seen.Count == 12,
            $"Expected exactly 10 (data) or 12 (data+extremes) rows, saw {seen.Count}.");
        // The 10 known data values are 0..9 — they must all appear among the first 10.
        for (ulong x = 0; x < 10; x++)
            Assert.Contains(x, seen);
    }

    private sealed class SynchronousProgress<T> : IProgress<T>
    {
        private readonly Action<T> _handler;
        public SynchronousProgress(Action<T> handler) => _handler = handler;
        public void Report(T value) => _handler(value);
    }
}
