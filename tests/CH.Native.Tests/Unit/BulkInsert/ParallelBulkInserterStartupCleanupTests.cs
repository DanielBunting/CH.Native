using CH.Native.BulkInsert;
using Xunit;

namespace CH.Native.Tests.Unit.BulkInsert;

/// <summary>
/// Regression tests for the <see cref="ParallelBulkInserter{T}"/> startup-failure
/// cleanup path (review item C2). When a worker's setup faults, the workers that
/// opened first are disposed before the original startup exception is re-thrown.
/// That disposal must be exception-safe: a throwing <c>DisposeAsync</c> on one
/// worker must neither propagate (masking the real startup root cause) nor skip
/// disposal of the remaining workers (leaking connections).
/// </summary>
public class ParallelBulkInserterStartupCleanupTests
{
    private sealed class Row
    {
        public long Id { get; set; }
    }

    /// <summary>A fake worker that records whether it was disposed.</summary>
    private sealed class FakeWorker : IAsyncDisposable
    {
        public bool Disposed { get; private set; }

        public ValueTask DisposeAsync()
        {
            Disposed = true;
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>A fake worker whose disposal throws, like a connection that faults
    /// while finalising the wire during teardown.</summary>
    private sealed class ThrowingWorker : IAsyncDisposable
    {
        public bool Disposed { get; private set; }

        public ValueTask DisposeAsync()
        {
            Disposed = true;
            throw new InvalidOperationException("teardown blew up");
        }
    }

    [Fact]
    public async Task DisposeOpenedWorkersAsync_ThrowingDisposal_DoesNotPropagate()
    {
        // A worker whose DisposeAsync throws must not surface from the cleanup helper:
        // the caller re-throws the startup root cause immediately after, and a leaking
        // disposal exception would replace it.
        var throwing = new ThrowingWorker();

        // Should complete without throwing. On the unfixed (unguarded) helper this
        // re-throws the InvalidOperationException, confirming the C2 masking bug.
        await ParallelBulkInserter<Row>.DisposeOpenedWorkersAsync(new IAsyncDisposable[] { throwing });

        Assert.True(throwing.Disposed);
    }

    [Fact]
    public async Task DisposeOpenedWorkersAsync_ThrowingWorker_StillDisposesTheRest()
    {
        // One worker throwing mid-cleanup must not abort disposal of the workers
        // after it — otherwise their connections leak on a startup failure.
        var before = new FakeWorker();
        var throwing = new ThrowingWorker();
        var after = new FakeWorker();

        await ParallelBulkInserter<Row>.DisposeOpenedWorkersAsync(
            new IAsyncDisposable[] { before, throwing, after });

        Assert.True(before.Disposed);
        Assert.True(throwing.Disposed);
        Assert.True(after.Disposed, "the worker after the throwing one was never disposed — connection leak");
    }
}
