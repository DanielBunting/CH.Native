using System.Diagnostics;
using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Pins the <em>cost</em> of <c>QueryTypedAsync</c>'s wire-realignment finally.
///
/// <para>
/// <see cref="WireBoundaryEvidenceTests.TypedStream_EarlyBreak_ConnectionReusableWithCorrectResult"/>
/// pins that abandoning a typed stream realigns the wire — the correctness half.
/// This file pins the other half: what that realignment costs the caller.
/// </para>
///
/// <para>
/// The realignment runs <c>SendCancelAsync</c> + <c>DrainAfterCancellationAsync</c>
/// from a <c>finally</c>, and neither observes the caller's token —
/// <c>DrainAfterCancellationAsync</c> is bounded only by its own internal 30-second
/// <c>CancellationTokenSource</c>. So <c>break</c> out of an <c>await foreach</c> —
/// an ordinary, hot usage shape — became a server round trip that cannot be
/// cancelled or shortened, with a 30-second worst case against a wedged server.
/// </para>
///
/// <para>
/// These tests do not assert the 30-second bound (a healthy container always
/// drains in milliseconds). They pin that the common paths stay in round-trip
/// territory, so a regression that pushes abandonment toward the drain timeout —
/// or that makes an already-cancelled caller pay it — fails loudly here rather
/// than showing up as a latency mystery in production.
/// </para>
/// </summary>
[Collection("ClickHouse")]
public class TypedStreamAbandonmentCostTests
{
    /// <summary>
    /// Generous vs. a healthy drain (milliseconds), far below the drain's own
    /// 30 s cap — so this fails on a real regression, not on CI jitter.
    /// </summary>
    private static readonly TimeSpan AbandonmentBudget = TimeSpan.FromSeconds(10);

    private readonly ClickHouseFixture _fixture;

    public TypedStreamAbandonmentCostTests(ClickHouseFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task EarlyBreak_RealignmentStaysWithinRoundTripBudget()
    {
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        // A large result set so the break lands with plenty of response still in
        // flight — the realignment has real work to do (Cancel, then drain the
        // in-flight blocks), not a trivially-empty wire.
        var sw = Stopwatch.StartNew();
        var seen = 0;
        await foreach (var _ in conn.QueryTypedAsync<ulong>(
            "SELECT number FROM system.numbers LIMIT 5000000"))
        {
            if (++seen >= 10) break;
        }
        sw.Stop();

        Assert.True(sw.Elapsed < AbandonmentBudget,
            $"Abandoning a typed stream took {sw.Elapsed.TotalSeconds:F1}s. The realignment " +
            "finally is uncancellable and bounded only by the drain's internal 30s timeout — " +
            "anything beyond a round trip here means the drain is not converging.");

        Assert.True(conn.BoundaryProven, "Realignment must leave the wire at a proven boundary.");
        Assert.Equal(4242, await conn.ExecuteScalarAsync<int>("SELECT 4242"));
    }

    [Fact]
    public async Task EarlyBreak_WithAlreadyCancelledToken_StillBounded()
    {
        // The case where the uncancellable drain is most exposed: the caller's
        // token is cancelled and THEN the enumerator is disposed. Iterator
        // disposal runs the same realignment, but the caller's cancellation
        // cannot shorten it — a caller that cancelled to get control back
        // promptly still waits for the round trip.
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        using var cts = new CancellationTokenSource();

        var sw = Stopwatch.StartNew();
        var seen = 0;
        try
        {
            await foreach (var _ in conn.QueryTypedAsync<ulong>(
                "SELECT number FROM system.numbers LIMIT 5000000",
                cancellationToken: cts.Token))
            {
                if (++seen >= 10)
                {
                    cts.Cancel();
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Acceptable: the pump may observe the token before the break lands.
        }
        sw.Stop();

        Assert.True(sw.Elapsed < AbandonmentBudget,
            $"Cancel-then-abandon took {sw.Elapsed.TotalSeconds:F1}s — the realignment must not " +
            "approach the drain's 30s cap on a healthy server.");

        // Whatever path it took, the connection must be classified, never left
        // claiming poolability with an unresolved wire.
        if (conn.CanBePooled)
            Assert.Equal(4242, await conn.ExecuteScalarAsync<int>("SELECT 4242"));
    }

    [Fact]
    public async Task FullyConsumedStream_SkipsRealignmentEntirely()
    {
        // The control: draining the enumerator to completion proves the boundary
        // inside the loop, so the finally's `!success && WireIndeterminate` gate
        // is false and NO Cancel/drain round trip happens. This is what keeps the
        // realignment cost scoped to abandonment — if this test starts paying the
        // round trip, the gate has regressed and every query got slower.
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        var count = 0;
        var sw = Stopwatch.StartNew();
        await foreach (var _ in conn.QueryTypedAsync<ulong>(
            "SELECT number FROM system.numbers LIMIT 1000"))
        {
            count++;
        }
        sw.Stop();

        Assert.Equal(1000, count);
        Assert.True(conn.BoundaryProven,
            "A fully-consumed stream proves the boundary in the loop, not via the drain.");
        Assert.True(sw.Elapsed < AbandonmentBudget,
            $"Full consumption took {sw.Elapsed.TotalSeconds:F1}s — it must not be paying a realignment round trip.");
    }
}
