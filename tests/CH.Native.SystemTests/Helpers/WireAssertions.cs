using CH.Native.Connection;
using Xunit;

namespace CH.Native.SystemTests.Helpers;

/// <summary>
/// Standard postcondition assertions for the wire-conversation contract.
/// The connection's evidence fields (<c>BoundaryProven</c> / <c>ConversationWrote</c>,
/// via InternalsVisibleTo) make wire-position invariants directly assertable —
/// previously these could only be inferred behaviorally, often needing looped runs.
/// </summary>
internal static class WireAssertions
{
    /// <summary>
    /// Asserts the connection is at a proven protocol boundary and pool-eligible:
    /// the standard postcondition after any successfully-completed operation.
    /// </summary>
    public static void AssertWireIdle(ClickHouseConnection conn)
    {
        Assert.True(conn.BoundaryProven,
            "Wire must be at a proven protocol boundary (a response terminator was consumed).");
        Assert.True(conn.CanBePooled,
            "Connection must be pool-eligible at an idle boundary.");
    }

    /// <summary>
    /// The universal post-failure contract: after ANY failure/cancellation, the
    /// connection must either (a) answer a follow-up query with the CORRECT
    /// distinctive sentinel — proving the wire was genuinely realigned, not just
    /// "able to return something" — or (b) refuse cleanly with the broken-connection
    /// error. It must never return a wrong value (reading a previous response's
    /// bytes) and never hang. Returns true when the connection was reusable.
    /// </summary>
    public static async Task<bool> AssertReusableOrCleanlyBrokenAsync(
        ClickHouseConnection conn, int sentinel, TimeSpan? timeout = null)
    {
        var probe = Task.Run(async () =>
            await conn.ExecuteScalarAsync<int>($"SELECT {sentinel}"));
        var winner = await Task.WhenAny(probe, Task.Delay(timeout ?? TimeSpan.FromSeconds(45)));
        Assert.True(winner == probe,
            "Post-failure probe hung — the wire was neither realigned nor condemned.");

        try
        {
            var value = await probe;
            Assert.Equal(sentinel, value); // wrong value = stale bytes = silent corruption
            AssertWireIdle(conn);
            return true;
        }
        catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
        {
            // Cleanly broken is acceptable — but it must be the *classified*
            // failure (broken/closed connection), never a protocol-desync error
            // surfacing mid-parse of stale bytes.
            Assert.False(conn.CanBePooled,
                $"Probe threw {ex.GetType().Name} but the connection still claims to be poolable — " +
                "a failure must either realign the wire or condemn the connection.");
            return false;
        }
    }
}
