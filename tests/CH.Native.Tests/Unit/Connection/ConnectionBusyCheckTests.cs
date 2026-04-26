using System.IO.Pipelines;
using System.Reflection;
using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Exceptions;
using Xunit;

namespace CH.Native.Tests.Unit.Connection;

/// <summary>
/// Tier 1 coverage for the connection-busy gate (Gap 2 — concurrent queries on
/// a single ClickHouseConnection). The gate is implemented as
/// <c>EnterBusy</c>/<c>ExitBusy</c> on the connection, with each public Execute
/// path claiming the slot synchronously at entry. These tests reflect into the
/// connection's private state to simulate "another caller is already in flight"
/// without needing a real server or pipe — the contract under test is purely
/// the entry gate.
/// </summary>
public class ConnectionBusyCheckTests
{
    private static ClickHouseConnection NewOpenLikeConnection(string? inFlightQueryId = "in-flight-q1")
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithUsername("default")
            .Build();
        var conn = new ClickHouseConnection(settings);
        SetField(conn, "_isOpen", true);
        SetField(conn, "_busy", true);
        // _busyOwnerQueryId is the source of truth for the busy exception's
        // InFlightQueryId. _currentQueryId is set in parallel only because some
        // existing tests reflect against it; the busy gate itself doesn't read it.
        SetField(conn, "_busyOwnerQueryId", inFlightQueryId);
        SetField(conn, "_currentQueryId", inFlightQueryId);
        return conn;
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

    // ======================================================================
    // D1. Each public Execute path throws ClickHouseConnectionBusyException
    //     synchronously when the connection is already busy.
    // ======================================================================

    [Fact]
    public async Task D_ExecuteScalarAsync_WhenBusy_ThrowsBusy()
    {
        await using var conn = NewOpenLikeConnection();

        var task = conn.ExecuteScalarAsync<long>("SELECT 1");
        Assert.True(task.IsFaulted, "EnterBusy must throw synchronously, before any await.");

        var ex = await Assert.ThrowsAsync<ClickHouseConnectionBusyException>(() => task);
        Assert.Equal("in-flight-q1", ex.InFlightQueryId);
    }

    [Fact]
    public async Task D_ExecuteNonQueryAsync_WhenBusy_ThrowsBusy()
    {
        await using var conn = NewOpenLikeConnection();

        var task = conn.ExecuteNonQueryAsync("INSERT INTO t VALUES (1)");
        Assert.True(task.IsFaulted);

        var ex = await Assert.ThrowsAsync<ClickHouseConnectionBusyException>(() => task);
        Assert.Equal("in-flight-q1", ex.InFlightQueryId);
    }

    [Fact]
    public async Task D_ExecuteReaderAsync_WhenBusy_ThrowsBusy()
    {
        await using var conn = NewOpenLikeConnection();

        var task = conn.ExecuteReaderAsync("SELECT * FROM t");
        Assert.True(task.IsFaulted);

        await Assert.ThrowsAsync<ClickHouseConnectionBusyException>(() => task);
    }

    [Fact]
    public async Task D_QueryTypedAsync_WhenBusy_ThrowsBusy()
    {
        await using var conn = NewOpenLikeConnection();

        // QueryTypedAsync is an iterator — it does the busy check on first
        // MoveNextAsync, not at call site. The throw still happens before any
        // wire I/O, which is the contract.
        var enumerator = conn.QueryTypedAsync<DummyRow>("SELECT 1").GetAsyncEnumerator();
        await Assert.ThrowsAsync<ClickHouseConnectionBusyException>(async () => await enumerator.MoveNextAsync());
        await enumerator.DisposeAsync();
    }

    // ======================================================================
    // D2. ClickHouseConnectionBusyException is catchable as InvalidOperationException
    //     so existing ADO.NET-style catches keep working.
    // ======================================================================

    [Fact]
    public async Task D_BusyException_IsCatchableAsInvalidOperationException()
    {
        await using var conn = NewOpenLikeConnection();

        var caughtAsBase = false;
        try
        {
            await conn.ExecuteScalarAsync<long>("SELECT 1");
        }
        catch (InvalidOperationException ex)
        {
            caughtAsBase = true;
            Assert.IsType<ClickHouseConnectionBusyException>(ex);
        }

        Assert.True(caughtAsBase);
    }

    // ======================================================================
    // D3. Bulk insert init claims the slot — concurrent queries throw busy.
    // ======================================================================

    [Fact]
    public async Task D_QueryAsync_WhileBulkInsertHoldsSlot_ThrowsBusy()
    {
        // Simulate "BulkInserter has called EnterBusyForBulkInsert and is mid-stream":
        // the slot is held; the inserter's id (or a sentinel) is in _currentQueryId.
        await using var conn = NewOpenLikeConnection("bulk-insert-q1");

        var ex = await Assert.ThrowsAsync<ClickHouseConnectionBusyException>(
            () => conn.ExecuteScalarAsync<long>("SELECT 1"));
        Assert.Equal("bulk-insert-q1", ex.InFlightQueryId);
    }

    [Fact]
    public async Task D_BulkInserterInit_WhenBusy_ThrowsBusy()
    {
        await using var conn = NewOpenLikeConnection();

        var inserter = new BulkInserter<DummyRow>(conn, "events");

        // InitAsync checks token + ObjectDisposed before EnterBusy, so the
        // throw is on the InitAsync task once it starts executing the body.
        var ex = await Assert.ThrowsAsync<ClickHouseConnectionBusyException>(() => inserter.InitAsync());
        Assert.Equal("in-flight-q1", ex.InFlightQueryId);

        // Slot wasn't claimed by the failed init — the original holder still
        // owns it; CanBePooled stays false because of the simulated holder, not
        // because the failed init leaked a slot.
        Assert.True(GetField<bool>(conn, "_busy"));
        Assert.False(GetField<bool>(inserter, "_slotClaimed"));
    }

    // ======================================================================
    // D4. Handshake sentinel — a concurrent call during OpenAsync handshake
    //     surfaces the "<handshake>" id.
    // ======================================================================

    [Fact]
    public async Task D_QueryAsync_DuringHandshakeWindow_SurfacesHandshakeSentinel()
    {
        // Simulates the state OpenAsync establishes between EnterBusy and the
        // _isOpen flip. _isOpen is set to true here so the busy check (rather
        // than "Connection is not open") is what fires.
        await using var conn = NewOpenLikeConnection(ClickHouseConnectionBusyException.HandshakeSentinel);

        var ex = await Assert.ThrowsAsync<ClickHouseConnectionBusyException>(
            () => conn.ExecuteScalarAsync<long>("SELECT 1"));
        Assert.Equal(ClickHouseConnectionBusyException.HandshakeSentinel, ex.InFlightQueryId);
    }

    // ======================================================================
    // D8. Role-sync invariant — the busy exception always names the outer
    //     caller's id, even if the inner SET ROLE recursion has temporarily
    //     clobbered or cleared _currentQueryId. _busyOwnerQueryId is the
    //     stable source of truth for the busy window.
    // ======================================================================

    [Fact]
    public async Task D_BusyException_RoleSyncWindow_ReportsOuterCallerId()
    {
        // Simulate the state inside EnsureRolesResolvedAsync: outer caller
        // has claimed the slot with id "outer-q1"; the inner SET ROLE has
        // already finished and ReadServerMessagesAsync.finally has cleared
        // _currentQueryId. _busyOwnerQueryId still holds "outer-q1".
        await using var conn = NewOpenLikeConnection("outer-q1");
        SetField(conn, "_currentQueryId", null); // mid role-sync gap

        var ex = await Assert.ThrowsAsync<ClickHouseConnectionBusyException>(
            () => conn.ExecuteScalarAsync<long>("SELECT 1"));
        // Must be the outer caller's id, not <handshake> and not null/empty.
        Assert.Equal("outer-q1", ex.InFlightQueryId);
    }

    [Fact]
    public async Task D_BusyException_RoleSyncInnerOverride_StillReportsOuterId()
    {
        // Simulate the moment inside SET ROLE execution where line 1700 of
        // SendQueryAsync has set _currentQueryId to the inner SET ROLE id.
        // _busyOwnerQueryId still names the outer caller.
        await using var conn = NewOpenLikeConnection("outer-q1");
        SetField(conn, "_currentQueryId", "set-role-id");

        var ex = await Assert.ThrowsAsync<ClickHouseConnectionBusyException>(
            () => conn.ExecuteScalarAsync<long>("SELECT 1"));
        Assert.Equal("outer-q1", ex.InFlightQueryId);
        Assert.NotEqual("set-role-id", ex.InFlightQueryId);
    }

    // ======================================================================
    // D5. Idle connection: busy check passes, EnterBusy claims, ExitBusy frees.
    //     This is the "no false-positive" sanity check.
    // ======================================================================

    [Fact]
    public async Task D_EnterBusy_OnIdleConnection_ClaimsThenReleases()
    {
        await using var conn = NewOpenLikeConnection();
        // Reset to idle state.
        SetField(conn, "_busy", false);
        SetField(conn, "_busyOwnerQueryId", null);
        SetField(conn, "_currentQueryId", null);

        var enterBusy = conn.GetType().GetMethod("EnterBusy", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var exitBusy = conn.GetType().GetMethod("ExitBusy", BindingFlags.Instance | BindingFlags.NonPublic)!;

        enterBusy.Invoke(conn, new object?[] { "owner-q" });
        Assert.True(GetField<bool>(conn, "_busy"));
        Assert.Equal("owner-q", GetField<string>(conn, "_busyOwnerQueryId"));

        // Second EnterBusy throws with the OWNER's id, not the new caller's.
        var tie = Assert.Throws<TargetInvocationException>(() => enterBusy.Invoke(conn, new object?[] { "intruder-q" }));
        var busyEx = Assert.IsType<ClickHouseConnectionBusyException>(tie.InnerException);
        Assert.Equal("owner-q", busyEx.InFlightQueryId);

        exitBusy.Invoke(conn, null);
        Assert.False(GetField<bool>(conn, "_busy"));
        Assert.Null(GetField<string>(conn, "_busyOwnerQueryId"));

        // After release, EnterBusy succeeds again.
        enterBusy.Invoke(conn, new object?[] { "next-q" });
        exitBusy.Invoke(conn, null);

        await Task.CompletedTask;
    }

    // ======================================================================
    // D6. CanBePooled rejects a busy connection.
    // ======================================================================

    [Fact]
    public async Task D_CanBePooled_FalseWhileBusy()
    {
        await using var conn = NewOpenLikeConnection();
        var canBePooled = (bool)conn.GetType()
            .GetProperty("CanBePooled", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(conn)!;
        Assert.False(canBePooled);
    }

    [Fact]
    public async Task D_CanBePooled_TrueWhenIdle()
    {
        await using var conn = NewOpenLikeConnection();
        SetField(conn, "_busy", false);
        SetField(conn, "_currentQueryId", null);

        var canBePooled = (bool)conn.GetType()
            .GetProperty("CanBePooled", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(conn)!;
        Assert.True(canBePooled);
    }

    // ======================================================================
    // D7. Stress: 32 racing callers — at most one passes the gate at any
    //     instant. The other 31 throw ClickHouseConnectionBusyException. The
    //     existing _writeLock keeps bytes from interleaving, but this test
    //     proves the new gate keeps the public API serialised on top of it.
    // ======================================================================

    [Fact]
    public async Task D_BusyCheck_UnderConcurrentLoad_NoOverlap()
    {
        await using var conn = NewOpenLikeConnection();
        // Reset to idle so the gate has to mediate.
        SetField(conn, "_busy", false);
        SetField(conn, "_busyOwnerQueryId", null);
        SetField(conn, "_currentQueryId", null);

        var enterBusy = conn.GetType().GetMethod("EnterBusy", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var exitBusy = conn.GetType().GetMethod("ExitBusy", BindingFlags.Instance | BindingFlags.NonPublic)!;

        // Deterministic contention: all 32 tasks block on a barrier and
        // attempt EnterBusy in lockstep. Exactly one wins; the other 31 must
        // throw ClickHouseConnectionBusyException. Repeating the round
        // 50 times exercises the gate under realistic churn.
        const int fanout = 32;
        const int rounds = 50;

        var maxObserved = 0;
        var busyExceptions = 0;
        var successes = 0;

        for (int round = 0; round < rounds; round++)
        {
            using var barrier = new Barrier(fanout);
            var tasks = new Task[fanout];
            var inFlight = 0;

            for (int i = 0; i < fanout; i++)
            {
                var threadId = i;
                tasks[i] = Task.Run(() =>
                {
                    barrier.SignalAndWait();
                    try
                    {
                        enterBusy.Invoke(conn, new object?[] { $"q-{round}-{threadId}" });
                    }
                    catch (TargetInvocationException tie) when (tie.InnerException is ClickHouseConnectionBusyException)
                    {
                        Interlocked.Increment(ref busyExceptions);
                        return;
                    }

                    try
                    {
                        var n = Interlocked.Increment(ref inFlight);
                        var prev = Volatile.Read(ref maxObserved);
                        while (n > prev)
                        {
                            if (Interlocked.CompareExchange(ref maxObserved, n, prev) == prev) break;
                            prev = Volatile.Read(ref maxObserved);
                        }
                        Thread.SpinWait(1000);
                        Interlocked.Decrement(ref inFlight);
                        Interlocked.Increment(ref successes);
                    }
                    finally
                    {
                        exitBusy.Invoke(conn, null);
                    }
                });
            }

            await Task.WhenAll(tasks);
        }

        // Core invariant: never more than one task in the critical section at
        // any instant. Successes and busy-throws will sum to the total number
        // of attempts but their split is non-deterministic — fast iterations
        // let many tasks slip through the gate sequentially within one barrier
        // round, while slow iterations leave most stuck behind a single
        // winner.
        Assert.Equal(1, maxObserved);
        Assert.Equal(rounds * fanout, successes + busyExceptions);
        Assert.True(busyExceptions > 0, "Some round must have produced contention; barrier should guarantee it.");
        Assert.False(GetField<bool>(conn, "_busy"));
    }

    private sealed class DummyRow
    {
        public long Value { get; set; }
    }
}
