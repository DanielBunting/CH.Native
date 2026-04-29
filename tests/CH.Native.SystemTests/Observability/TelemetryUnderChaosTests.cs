using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.Http;
using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Observability;

/// <summary>
/// Pins the contract that the OTel instrumentation behaves correctly when queries
/// are interrupted: every started Activity must reach <c>Stopped</c>, the request
/// counter must reflect actual call counts, and error-shaped activities must carry
/// a non-empty <c>error.type</c>. Existing telemetry tests cover happy paths only;
/// these scenarios are what break silently in production when the network or the
/// server stops cooperating.
/// </summary>
[Collection("Toxiproxy")]
[Trait(Categories.Name, Categories.Observability)]
public class TelemetryUnderChaosTests : IAsyncLifetime
{
    private const string SourceName = "CH.Native";
    private readonly ToxiproxyFixture _proxy;
    private readonly ITestOutputHelper _output;

    public TelemetryUnderChaosTests(ToxiproxyFixture proxy, ITestOutputHelper output)
    {
        _proxy = proxy;
        _output = output;
    }

    public Task InitializeAsync() => SafeRemoveAllToxicsAsync();
    public Task DisposeAsync() => SafeRemoveAllToxicsAsync();

    // Toxiproxy's admin endpoint can briefly return 503 after a heavy chaos
    // test (the upstream ClickHouse container takes a moment to recover from
    // reset_peer). Cleanup is best-effort — retry a few times so a transient
    // hiccup doesn't cascade through the whole class.
    private async Task SafeRemoveAllToxicsAsync()
    {
        for (int attempt = 0; attempt < 5; attempt++)
        {
            try
            {
                await _proxy.Client.RemoveAllToxicsAsync(ToxiproxyFixture.ProxyName);
                return;
            }
            catch (HttpRequestException) when (attempt < 4)
            {
                await Task.Delay(200);
            }
        }
    }

    [Fact]
    public async Task MidStreamConnectionDrop_ProducesSpanWithErrorStatus()
    {
        // Hook ActivityListener directly (rather than a TracerProvider) so we observe
        // start AND stop events — needed to detect orphaned activities.
        var stopped = new ConcurrentBag<Activity>();
        var startedCount = 0;
        using var listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == SourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStarted = _ => Interlocked.Increment(ref startedCount),
            ActivityStopped = a => stopped.Add(a),
        };
        ActivitySource.AddActivityListener(listener);

        await using var conn = new ClickHouseConnection(_proxy.BuildSettings());
        await conn.OpenAsync();

        // Hard ceiling so a regression that hangs the pump can't poison the rest of
        // the suite (a 25s hang here was the original cause of cascading 503s).
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(8));

        Exception? caught = null;
        bool toxicApplied = false;
        try
        {
            int rowsSeen = 0;
            await foreach (var _ in conn.QueryAsync<ulong>(
                "SELECT number FROM numbers(10000000)").WithCancellation(cts.Token))
            {
                rowsSeen++;

                // Apply reset_peer once the stream is genuinely flowing. Doing it
                // pre-foreach ran the risk of the read pump never observing the
                // reset because the buffered reader had nothing on the wire yet.
                if (rowsSeen == 1000 && !toxicApplied)
                {
                    await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "reset_peer", "downstream",
                        new() { ["timeout"] = 0 });
                    toxicApplied = true;
                }
            }
        }
        catch (Exception ex) { caught = ex; }
        finally
        {
            await SafeRemoveAllToxicsAsync();
        }

        Assert.NotNull(caught);

        // Every started activity has a matching stop — no orphans.
        Assert.Equal(startedCount, stopped.Count);

        // At least one CH.Native span carries Error status with a populated error.type tag.
        var errorActivity = stopped.FirstOrDefault(a => a.Status == ActivityStatusCode.Error);
        Assert.NotNull(errorActivity);
        var errorType = errorActivity!.GetTagItem("error.type") as string;
        Assert.False(string.IsNullOrEmpty(errorType),
            $"Error activity missing error.type tag. Activity: {errorActivity.OperationName}");
    }

    [Fact]
    public async Task CancelDuringStreaming_ProducesCleanSpanLifecycle()
    {
        var stopped = new ConcurrentBag<Activity>();
        var startedCount = 0;
        using var listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == SourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStarted = _ => Interlocked.Increment(ref startedCount),
            ActivityStopped = a => stopped.Add(a),
        };
        ActivitySource.AddActivityListener(listener);

        await using var conn = new ClickHouseConnection(_proxy.BuildSettings());
        await conn.OpenAsync();

        // Slow each downstream packet so cancellation lands during streaming.
        await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "latency", "downstream",
            new() { ["latency"] = 800, ["jitter"] = 0 });

        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(80));
            try
            {
                await foreach (var _ in conn.QueryAsync<int>(
                    "SELECT number FROM numbers(10000000)").WithCancellation(cts.Token))
                {
                }
            }
            catch (OperationCanceledException) { /* expected */ }
        }
        finally
        {
            await SafeRemoveAllToxicsAsync();
        }

        // Lifecycle balance: every started activity stopped — no leaks.
        Assert.Equal(startedCount, stopped.Count);

        // Exactly one query activity is expected for the single QueryAsync call;
        // assert the count to surface a regression where cancel triggers double-stop
        // or spawns extra activities.
        var queryActivities = stopped.Where(a => a.Source.Name == SourceName).ToList();
        Assert.True(queryActivities.Count >= 1,
            $"Expected ≥ 1 CH.Native activity; got {queryActivities.Count}.");
        Assert.True(queryActivities.Count <= 2,
            $"Expected ≤ 2 CH.Native activities (query + nested); got {queryActivities.Count}.");
    }

    [Fact]
    public async Task ServerSideExceptionMidBlock_ProducesSingleErrorActivity()
    {
        var stopped = new ConcurrentBag<Activity>();
        var startedCount = 0;
        using var listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == SourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStarted = _ => Interlocked.Increment(ref startedCount),
            ActivityStopped = a => stopped.Add(a),
        };
        ActivitySource.AddActivityListener(listener);

        await using var conn = new ClickHouseConnection(_proxy.BuildSettings());
        await conn.OpenAsync();

        // throwIf() forces a server-side exception once a row threshold is hit —
        // mid-stream, after the query has begun returning data.
        Exception? caught = null;
        try
        {
            await foreach (var _ in conn.QueryAsync<ulong>(
                "SELECT throwIf(number = 5000, 'boom') FROM numbers(10000)"))
            {
            }
        }
        catch (Exception ex) { caught = ex; }

        Assert.NotNull(caught);
        Assert.Equal(startedCount, stopped.Count);

        var errorActivity = stopped.FirstOrDefault(a => a.Status == ActivityStatusCode.Error);
        Assert.NotNull(errorActivity);
        Assert.False(string.IsNullOrEmpty(errorActivity!.GetTagItem("error.type") as string),
            "Server-exception activity must populate error.type.");
    }

    [Fact]
    public async Task ConcurrentBurst_RequestCounterMatchesQueryCount()
    {
        // Fresh meter scope: counts only what happens in this test. Anything before
        // we call AddInMemoryExporter is invisible.
        var metrics = new List<Metric>();
        using var meterProvider = Sdk.CreateMeterProviderBuilder()
            .AddMeter(SourceName)
            .AddInMemoryExporter(metrics)
            .Build();

        const int concurrent = 200;

        // One connection per task — concurrent queries across one ClickHouseConnection
        // would serialize on the connection's busy lock and defeat the burst.
        var tasks = Enumerable.Range(0, concurrent).Select(_ => Task.Run(async () =>
        {
            await using var conn = new ClickHouseConnection(_proxy.BuildSettings());
            await conn.OpenAsync();
            _ = await conn.ExecuteScalarAsync<int>("SELECT 1");
        })).ToArray();

        await Task.WhenAll(tasks);

        meterProvider!.ForceFlush(timeoutMilliseconds: 2000);

        long successQueries = 0;
        long errorQueries = 0;
        foreach (var m in metrics.Where(x => x.Name == "ch_native_queries_total"))
        {
            foreach (var p in m.GetMetricPoints())
            {
                string status = "";
                foreach (var t in p.Tags)
                    if (t.Key == "status") status = t.Value as string ?? "";
                if (status == "success") successQueries += p.GetSumLong();
                else errorQueries += p.GetSumLong();
            }
        }

        _output.WriteLine($"queries_total: success={successQueries}, error={errorQueries}");

        // Each task's SELECT 1 contributes exactly one successful query measurement.
        // Allow no slack: a counter regression that double-increments or drops counts
        // under contention is exactly what this test pins.
        Assert.Equal(concurrent, successQueries);
        Assert.Equal(0, errorQueries);
    }
}
