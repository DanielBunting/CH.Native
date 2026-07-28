using System.Diagnostics;
using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Cancellation;

/// <summary>
/// Repro for wire-state-machine review finding #2: the post-OCE drain gates are
/// <c>when (_conversationWrote)</c>, and <c>WriteAndFlushAsync</c> stamps
/// <c>_conversationWrote</c> BEFORE the write. A cancellation landing while the
/// query message itself is still being written means the server never received a
/// complete query and owes no response — yet the gate passes and
/// <c>DrainAfterCancellationAsync</c> reads against a silent server until its
/// 30 s cap before the caller's <c>OperationCanceledException</c> surfaces.
/// (The pre-refactor gate, <c>_cancellationRequested</c>, skipped the drain here.)
/// </summary>
[Collection("Toxiproxy")]
[Trait(Categories.Name, Categories.Cancellation)]
[Trait(Categories.Name, Categories.RaceSensitive)]
public sealed class CancelDuringQueryWriteTests : IAsyncLifetime
{
    private readonly ToxiproxyFixture _proxy;
    private readonly ITestOutputHelper _output;

    public CancelDuringQueryWriteTests(ToxiproxyFixture proxy, ITestOutputHelper output)
    {
        _proxy = proxy;
        _output = output;
    }

    public Task InitializeAsync() => _proxy.ResetProxyAsync();
    public Task DisposeAsync() => _proxy.ResetProxyAsync();

    [Fact]
    public async Task CancelWhileQueryMessageStillWriting_SurfacesPromptly()
    {
        await using var conn = new ClickHouseConnection(_proxy.BuildSettings());
        await conn.OpenAsync();

        // Throttle client->server to a crawl so the 8 MB query message is
        // guaranteed to still be mid-write when the token fires.
        await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "bandwidth", "upstream",
            new() { ["rate"] = 16 }); // KB/s

        var hugeSql = "SELECT 1 /* " + new string('x', 8_000_000) + " */";
        using var cts = new CancellationTokenSource(500);

        var sw = Stopwatch.StartNew();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            conn.ExecuteNonQueryAsync(hugeSql, cancellationToken: cts.Token));
        sw.Stop();

        _output.WriteLine($"cancellation surfaced after {sw.Elapsed.TotalSeconds:F1}s");

        // The server never received a complete query, so there is no response
        // to drain: the OCE must surface promptly, not after the 30 s drain cap.
        // (Condemning the connection is fine — bytes were half-written — but
        // stalling the CALLER for the full drain timeout is the regression.)
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(10),
            $"Cancellation took {sw.Elapsed.TotalSeconds:F1}s to surface — the post-OCE " +
            "drain ran against a server that owes no response (gate lacks boundary/partial-write awareness).");
    }
}
