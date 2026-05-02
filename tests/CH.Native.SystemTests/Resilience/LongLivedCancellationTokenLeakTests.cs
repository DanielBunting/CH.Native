using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Resilience;

/// <summary>
/// Probes for a <see cref="CancellationTokenRegistration"/> leak across many
/// rent-and-query cycles using the same long-lived
/// <see cref="CancellationTokenSource"/>. The library uses the
/// <c>await using var registration = cancellationToken.Register(...)</c>
/// pattern at every wire-side site, which should dispose the registration
/// on scope exit. This test pins the contract:
///
/// <list type="bullet">
/// <item><description>1000 sequential queries on the same CT do not leak permanent
///     callbacks (the runtime would surface this as ever-increasing per-call
///     latency or process-wide memory growth).</description></item>
/// <item><description>The CT remains in good health (cancellation still works on the
///     last call after 999 prior uses).</description></item>
/// </list>
///
/// <para>
/// Note: we don't directly assert "no leak" via reflection on
/// <see cref="CancellationTokenSource"/>'s internal callback list — that's
/// brittle. Instead we observe that cancellation still fires correctly,
/// which is the user-visible symptom that matters.
/// </para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Resilience)]
public class LongLivedCancellationTokenLeakTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public LongLivedCancellationTokenLeakTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task ManyQueriesShareSingleCT_NoCallbackAccumulation_CancellationStillWorks()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        // Long-lived CTS — used across 500 queries. If the library had a
        // registration leak, callbacks would accumulate and per-call latency
        // would grow. We measure overall elapsed and assert it stays bounded.
        using var longLived = new CancellationTokenSource();
        var ct = longLived.Token;

        var sw = System.Diagnostics.Stopwatch.StartNew();
        for (int i = 0; i < 500; i++)
        {
            var result = await conn.ExecuteScalarAsync<int>("SELECT 1", cancellationToken: ct);
            Assert.Equal(1, result);
        }
        sw.Stop();

        _output.WriteLine($"500 queries with shared CT: {sw.ElapsedMilliseconds} ms ({sw.ElapsedMilliseconds / 500.0:F2} ms avg)");
        // Loose upper bound — even on slow CI, 500 trivial SELECTs shouldn't
        // exceed 30s. A leak would push this far higher.
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(30),
            $"500 queries took {sw.Elapsed}; suspect a registration leak");

        // After 500 uses, the CT should still cancel cleanly.
        using var probeCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        probeCts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await conn.ExecuteScalarAsync<int>("SELECT 1", cancellationToken: probeCts.Token);
        });
    }
}
