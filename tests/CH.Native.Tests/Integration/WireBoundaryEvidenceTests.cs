using System.Diagnostics;
using CH.Native.Commands;
using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Pins the wire-conversation evidence contracts introduced by the state-machine
/// work (see the wire-state-machine plan): boundary proof survives a stray Cancel
/// packet, pre-cancelled tokens leave no evidence and fail fast, and a
/// CommandTimeout on the reader path leaves the connection genuinely reusable.
/// Uses InternalsVisibleTo to read <c>ConversationWrote</c>/<c>BoundaryProven</c>
/// and to drive <c>SendCancelAsync</c> directly.
/// </summary>
[Collection("ClickHouse")]
public class WireBoundaryEvidenceTests
{
    private readonly ClickHouseFixture _fixture;

    public WireBoundaryEvidenceTests(ClickHouseFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task CancelPacketAfterCompletedQuery_DoesNotUnproveBoundary_NextQueryCorrect()
    {
        // F2 pin: a Cancel packet is excluded from conversation evidence. The
        // detached cancellation callback can land its Cancel write AFTER the
        // response terminator was consumed; if that write cleared _boundaryProven,
        // the (future) pessimistic resolve would spuriously poison a healthy
        // connection. Also empirically pins the protocol assumption that the
        // server ignores a Cancel received while idle.
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        Assert.Equal(4242, await conn.ExecuteScalarAsync<int>("SELECT 4242"));
        Assert.True(conn.BoundaryProven, "Completed query must leave the boundary proven.");

        // Force a Cancel packet onto the idle wire (what the detached callback
        // does when it loses the race with completion).
        await conn.SendCancelAsync();

        Assert.True(conn.BoundaryProven,
            "A Cancel packet must not clear boundary proof — it is excluded from evidence.");

        // And the server must treat the idle Cancel as a no-op: next query works
        // and returns ITS OWN result (not a stale or error response).
        Assert.Equal(42, await conn.ExecuteScalarAsync<int>("SELECT 42"));
    }

    [Fact]
    public async Task PreCancelledToken_FailsFast_NoEvidence_ConnectionReusable()
    {
        // F3 pin: a token that is already cancelled before the query is sent must
        // (a) throw promptly — never enter the 30s cancellation drain, because
        // nothing was written and there is no response to drain; (b) leave
        // ConversationWrote false so the (future) drain gate `when
        // (_conversationWrote)` correctly skips; (c) leave the connection reusable.
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var sw = Stopwatch.StartNew();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => conn.ExecuteScalarAsync<int>("SELECT sleep(1)", cancellationToken: cts.Token));
        sw.Stop();

        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(5),
            $"Pre-cancelled token took {sw.Elapsed} — it must fail fast, not drain.");
        Assert.False(conn.ConversationWrote,
            "Nothing was written; the conversation must carry no write evidence.");
        Assert.Equal(4242, await conn.ExecuteScalarAsync<int>("SELECT 4242"));
    }

    [Fact]
    public async Task TypedStream_EarlyBreak_ConnectionReusableWithCorrectResult()
    {
        // F8: abandoning a typed stream (break out of await foreach) runs only
        // finallys — no catch ever sees it. QueryTypedAsync's finally must
        // Cancel+drain so the un-consumed response can't corrupt the next query.
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        var seen = 0;
        await foreach (var _ in conn.QueryTypedAsync<ulong>(
            "SELECT number FROM system.numbers LIMIT 1000000"))
        {
            if (++seen >= 10) break; // abandon mid-stream
        }

        // The follow-up must return ITS OWN result, not a leftover data block.
        Assert.Equal(4242, await conn.ExecuteScalarAsync<int>("SELECT 4242"));
        Assert.True(conn.BoundaryProven, "Early-break must leave the wire at a proven boundary (drained).");
    }

    [Fact]
    public async Task CommandTimeout_OnReaderPath_ConnectionReusableWithCorrectResult()
    {
        // F7: the reader's dispose-drain is dead when CommandTimeout fires — the
        // pump enumerator carries the timeout token, finishes on the OCE, and the
        // dispose drain loop no-ops on a finished iterator. Today the sleep
        // query's response bytes are left unread, and the NEXT query on the same
        // connection reads THEM as its own response — silent corruption (this
        // assertion returns sleep's 0 instead of 42) or a protocol error.
        // After the step-2 fix (reader-dispose falls back to
        // DrainAfterCancellationAsync) the connection must be genuinely reusable.
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        await using (var cmd = new ClickHouseCommand(conn, "SELECT sleep(2)"))
        {
            cmd.CommandTimeout = 1;
            await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync()) { }
            });
        }

        // The follow-up query must return its OWN result. Pre-fix this reads the
        // abandoned sleep-response bytes (wrong value or protocol error).
        Assert.Equal(42, await conn.ExecuteScalarAsync<int>("SELECT 42"));
    }
}
