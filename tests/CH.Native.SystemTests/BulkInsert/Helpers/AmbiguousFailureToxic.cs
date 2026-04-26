using CH.Native.SystemTests.Fixtures;

namespace CH.Native.SystemTests.BulkInsertFailures.Helpers;

/// <summary>
/// Toxiproxy choreography for the "ack-loss" failure mode: client→server bytes flow
/// freely until the data block lands on the server, then server→client is severed.
/// This forces the client into the ambiguous state where the block may or may not
/// have committed — the failure mode the typed
/// <c>ClickHouseAmbiguousInsertException</c> (Tier B) is meant to surface.
/// </summary>
/// <remarks>
/// The choreography is two-phase: kick off a delayed downstream reset_peer so the
/// upstream payload has time to reach the server, then let the inserter run. Tune
/// <paramref name="ackBlockDelay"/> against the workload size — too short, the
/// reset lands before any data does; too long, the insert completes successfully.
/// </remarks>
internal static class AmbiguousFailureToxic
{
    public static Task SeverAckAsync(
        ToxiproxyClient client,
        TimeSpan ackBlockDelay,
        CancellationToken cancellationToken = default)
    {
        return Task.Run(async () =>
        {
            await Task.Delay(ackBlockDelay, cancellationToken);
            await client.AddToxicAsync(
                ToxiproxyFixture.ProxyName,
                "reset_peer",
                "downstream",
                new Dictionary<string, object> { ["timeout"] = 0 });
        }, cancellationToken);
    }
}
