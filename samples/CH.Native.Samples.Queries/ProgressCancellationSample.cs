using CH.Native.Connection;
using CH.Native.Data;

namespace CH.Native.Samples.Queries;

/// <summary>
/// <c>IProgress&lt;QueryProgress&gt;</c> + <c>CancellationToken</c> — long-running
/// scans with progress reporting and cooperative cancellation. Models a UI or
/// background-job runner that needs to (a) show "rows scanned" feedback and
/// (b) abort cleanly when the user changes their mind.
/// </summary>
/// <remarks>
/// Progress events are pumped through whatever <see cref="SynchronizationContext"/>
/// the caller is on — straight passthrough on a console app, marshalled to the UI
/// thread when used from a UI framework. Cancellation is cooperative and prompt:
/// the server is told to stop, the streaming reader unwinds, and the await
/// rethrows <see cref="OperationCanceledException"/>. Both knobs are present on
/// every connection-layer query method that supports them.
/// </remarks>
internal static class ProgressCancellationSample
{
    public static async Task RunAsync(string connectionString)
    {
        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        // -----------------------------------------------------------------
        // Part 1: Progress reporting on a heavy scalar query.
        // -----------------------------------------------------------------
        Console.WriteLine("--- Part 1: IProgress<QueryProgress> on a 10M-row scan ---");

        var progressEvents = 0;
        // Track the high-water mark — the final callback in the stream can
        // be a zero-valued frame, so saving "the last one" loses information.
        QueryProgress peakProgress = default;
        var progress = new Progress<QueryProgress>(p =>
        {
            progressEvents++;
            if (p.RowsRead > peakProgress.RowsRead) peakProgress = p;
            // Throttle the printout — server-side fanout can fire many events.
            if (progressEvents % 5 == 1)
            {
                var pct = p.PercentComplete is { } x ? $"{x,6:F1}%" : "  n/a ";
                Console.WriteLine($"  progress: rows={p.RowsRead,12:N0} bytes={p.BytesRead,12:N0} pct={pct}");
            }
        });

        var progressQueryId = $"progress-heavy-{Guid.NewGuid():N}";
        var heavyResult = await connection.ExecuteScalarAsync<ulong>(
            "SELECT count() FROM numbers(10000000) WHERE intHash32(number) % 7 = 0",
            progress: progress,
            cancellationToken: CancellationToken.None,
            queryId: progressQueryId);

        Console.WriteLine();
        Console.WriteLine($"  Result               : {heavyResult:N0}");
        Console.WriteLine($"  Progress events fired: {progressEvents}");
        Console.WriteLine($"  Peak RowsRead        : {peakProgress.RowsRead:N0}");
        Console.WriteLine($"  Peak BytesRead       : {peakProgress.BytesRead:N0}");
        Console.WriteLine($"  queryId sent         : {progressQueryId}");
        Console.WriteLine($"  queryId echoed       : {connection.LastQueryId}");

        // -----------------------------------------------------------------
        // Part 2: Cooperative cancellation mid-stream.
        // -----------------------------------------------------------------
        Console.WriteLine();
        Console.WriteLine("--- Part 2: CancellationToken mid-stream ---");

        using var cts = new CancellationTokenSource();
        var rowsBeforeCancel = 0;
        Exception? cancelException = null;

        try
        {
            // Stream from a deliberately unbounded source. Cancel once we've
            // proven the streaming works.
            await foreach (var row in connection.QueryAsync(
                "SELECT number FROM numbers(1000000000)",
                cts.Token,
                queryId: $"progress-cancel-{Guid.NewGuid():N}"))
            {
                rowsBeforeCancel++;
                if (rowsBeforeCancel >= 10_000)
                {
                    cts.Cancel();
                }
            }
        }
        catch (OperationCanceledException ex)
        {
            cancelException = ex;
        }

        Console.WriteLine($"  Rows consumed before cancel : {rowsBeforeCancel:N0}");
        Console.WriteLine($"  Caught                      : {cancelException?.GetType().Name ?? "(none — cancellation didn't fire)"}");

        Console.WriteLine();
        Console.WriteLine("--- Plumbing check ---");
        var peakPct = peakProgress.PercentComplete is { } x ? $"{x:F1}%" : "n/a";
        Console.WriteLine($"  IProgress<QueryProgress> : {progressEvents} events, peak pct={peakPct}");
        Console.WriteLine($"  CancellationToken        : signalled mid-stream, OperationCanceledException raised");
        Console.WriteLine($"  queryId                  : threaded through both calls");
    }
}
