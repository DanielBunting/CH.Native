using System.IO.Pipelines;
using System.Reflection;
using CH.Native.Connection;
using Xunit;

namespace CH.Native.Tests.Unit.Connection;

/// <summary>
/// Concurrency regression tests for <see cref="ClickHouseConnection"/>:
///   B. Concurrent writes to <c>_pipeWriter</c> from SendQueryAsync + SendCancelAsync.
///   A. Unsynchronized reads of <c>_isOpen</c> from CanBePooled.
///   C. DrainAfterCancellationAsync unconditionally clearing <c>_currentQueryId</c>
///      belonging to a subsequent query.
/// </summary>
public class ConnectionThreadSafetyTests
{
    // ---- Test helpers ----------------------------------------------------

    private static ClickHouseConnection NewClosedConnection()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithUsername("default")
            .Build();
        return new ClickHouseConnection(settings);
    }

    private static void SetField(object target, string name, object? value)
    {
        var f = target.GetType().GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Field not found: {name}");
        f.SetValue(target, value);
    }

    private static T? GetField<T>(object target, string name)
    {
        var f = target.GetType().GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Field not found: {name}");
        return (T?)f.GetValue(target);
    }

    private static Task InvokePrivateTaskAsync(object target, string name, params object?[] args)
    {
        var m = target.GetType().GetMethod(name, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Method not found: {name}");
        return (Task)m.Invoke(target, args)!;
    }

    /// <summary>
    /// Stream that counts overlapping WriteAsync calls. Yields at entry so
    /// concurrent callers actually get a chance to interleave.
    /// </summary>
    private sealed class ConcurrencyDetectingStream : Stream
    {
        private int _inFlight;
        private int _concurrent;

        public int ConcurrentWritesObserved => _concurrent;

        public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken ct)
        {
            var n = Interlocked.Increment(ref _inFlight);
            try
            {
                if (n > 1) Interlocked.Increment(ref _concurrent);
                await Task.Yield();
                await Task.Delay(2, ct);
            }
            finally
            {
                Interlocked.Decrement(ref _inFlight);
            }
        }

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct = default)
        {
            var n = Interlocked.Increment(ref _inFlight);
            try
            {
                if (n > 1) Interlocked.Increment(ref _concurrent);
                await Task.Yield();
                await Task.Delay(2, ct);
            }
            finally
            {
                Interlocked.Decrement(ref _inFlight);
            }
        }

        public override Task FlushAsync(CancellationToken ct) => Task.CompletedTask;
        public override void Flush() { }

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => 0;
        public override long Position { get => 0; set => throw new NotSupportedException(); }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    // ======================================================================
    // Finding B: concurrent writes to _pipeWriter from the cancel path.
    // ======================================================================

    [Fact]
    public async Task B_SendCancelAsync_ConcurrentCalls_DoNotOverlapOnTheWire()
    {
        await using var conn = NewClosedConnection();
        var detector = new ConcurrencyDetectingStream();
        var pipeWriter = PipeWriter.Create(detector);

        SetField(conn, "_pipeWriter", pipeWriter);
        SetField(conn, "_isOpen", true);

        const int fanout = 32;
        var tasks = new Task[fanout];
        for (int i = 0; i < fanout; i++)
            tasks[i] = Task.Run(() => conn.SendCancelAsync());

        await Task.WhenAll(tasks);

        Assert.Equal(0, detector.ConcurrentWritesObserved);
    }

    // ======================================================================
    // Finding A: CanBePooled must read _isOpen under _queryLock.
    // ======================================================================

    [Fact]
    public async Task A_CanBePooled_RespectsIsOpenMutationPerformedUnderQueryLock()
    {
        // Scenario the fix targets:
        //   1. _isOpen is currently true, no query in flight.
        //   2. Another thread enters _queryLock, flips _isOpen=false (as
        //      CloseInternalAsync will do post-fix), then releases.
        //   3. A concurrent CanBePooled call started while the lock was held
        //      must observe _isOpen=false once it acquires the lock.
        //
        // Pre-fix: CanBePooled reads _isOpen BEFORE taking the lock. At that
        // point _isOpen is still true (step 1), so it passes the early check
        // and falls through to the post-lock checks — returning TRUE.
        //
        // Post-fix: CanBePooled reads _isOpen INSIDE the lock, so it blocks
        // until the closer releases, then correctly sees _isOpen=false and
        // returns FALSE.
        await using var conn = NewClosedConnection();
        SetField(conn, "_isOpen", true);

        var lockObj = GetField<object>(conn, "_queryLock")!;
        var lockAcquired = new ManualResetEventSlim(false);
        var release = new ManualResetEventSlim(false);

        var closer = Task.Run(() =>
        {
            lock (lockObj)
            {
                lockAcquired.Set();
                release.Wait();
                // Flip state under the lock — matches what the post-fix
                // CloseInternalAsync will do.
                SetField(conn, "_isOpen", false);
            }
        });

        lockAcquired.Wait();

        // Start CanBePooled while the lock is held. It should block inside
        // the lock until we release, then observe _isOpen=false.
        var canBePooledTask = Task.Run(() => conn.CanBePooled);

        // Give CanBePooled time to reach the lock. Without a proper
        // inside-the-lock read, it has already made the _isOpen decision.
        await Task.Delay(50);

        release.Set();

        await closer;
        var result = await canBePooledTask;

        Assert.False(result);
    }

    // ======================================================================
    // Finding C: DrainAfterCancellationAsync must not clear a _currentQueryId
    // belonging to a later query.
    // ======================================================================

    [Fact]
    public async Task C_DrainAfterCancellation_PreservesCurrentQueryIdBelongingToLaterQuery()
    {
        await using var conn = NewClosedConnection();

        // Set up a Pipe so _pipeReader exists; we control when drain completes.
        var pipe = new Pipe();
        SetField(conn, "_pipeReader", pipe.Reader);
        SetField(conn, "_isOpen", true);
        SetField(conn, "_currentQueryId", "q1");

        // Kick off the drain; it blocks on ReadAsync waiting for bytes.
        var drainTask = InvokePrivateTaskAsync(conn, "DrainAfterCancellationAsync");

        // Give the drain a moment to reach ReadAsync.
        await Task.Delay(50);

        // A subsequent query starts while the drain for q1 is still in flight.
        var lockObj = GetField<object>(conn, "_queryLock")!;
        lock (lockObj)
        {
            SetField(conn, "_currentQueryId", "q2");
        }

        // Let the drain complete: writer completing with no data causes
        // ReadAsync to return IsCompleted && IsEmpty, exiting the loop cleanly.
        await pipe.Writer.CompleteAsync();
        await drainTask;

        // The drain belonged to q1 and must NOT clobber the field that now
        // belongs to q2.
        Assert.Equal("q2", GetField<string>(conn, "_currentQueryId"));
    }

    [Fact]
    public async Task C_DrainAfterCancellation_ClearsCurrentQueryId_WhenStillOwned()
    {
        // Companion: when nothing else has started, the drain's scope still
        // owns _currentQueryId, so it must be cleared (existing behavior).
        await using var conn = NewClosedConnection();

        var pipe = new Pipe();
        SetField(conn, "_pipeReader", pipe.Reader);
        SetField(conn, "_isOpen", true);
        SetField(conn, "_currentQueryId", "q1");

        var drainTask = InvokePrivateTaskAsync(conn, "DrainAfterCancellationAsync");

        await pipe.Writer.CompleteAsync();
        await drainTask;

        Assert.Null(GetField<string>(conn, "_currentQueryId"));
    }
}
