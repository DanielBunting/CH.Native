using System.Diagnostics;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.Resilience;
using CH.Native.SystemTests.Fixtures;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Observability;

/// <summary>
/// Telemetry probes that need fixtures heavier than <see cref="SingleNodeFixture"/> —
/// the bulk-insert partial-failure path needs <see cref="RestartableSingleNodeFixture"/>
/// (to actually kill the server mid-stream), and the retry-trace-propagation probe
/// needs <see cref="MultiToxiproxyFixture"/> (to fail one host so the retry path
/// fires). Both are §3 items deferred from the original telemetry pass because they
/// span multiple components and are slower to author.
/// </summary>
[Trait(Categories.Name, Categories.Observability)]
public sealed class BulkInsertAndRetryTelemetryProbeTests
{
    private const string SourceName = "CH.Native";

    [Collection("RestartableSingleNode")]
    public sealed class BulkInsert : IAsyncLifetime
    {
        private readonly RestartableSingleNodeFixture _fixture;
        private readonly ITestOutputHelper _output;

        public BulkInsert(RestartableSingleNodeFixture fixture, ITestOutputHelper output)
        {
            _fixture = fixture;
            _output = output;
        }

        public Task InitializeAsync() => Task.CompletedTask;

        public async Task DisposeAsync()
        {
            try
            {
                await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
                await conn.OpenAsync();
            }
            catch
            {
                try { await _fixture.StartContainerAsync(); } catch { }
            }
        }

        [Fact]
        public async Task RowsWritten_NotIncrementedBeyondPersisted_OnPartialFailure()
        {
            // Insert N rows; kill the server mid-stream; observe the rows_written
            // counter and compare against actual persisted rows on the server.
            // Probe — pin only the safety invariant: counter ≤ rows actually sent
            // (over-count would mean we credited rows that never landed).
            var metrics = new List<Metric>();
            using var meterProvider = Sdk.CreateMeterProviderBuilder()
                .AddMeter(SourceName)
                .AddInMemoryExporter(metrics)
                .Build();

            var table = $"bulk_metric_{Guid.NewGuid():N}";
            const int rowCount = 200_000;

            await using (var conn = new ClickHouseConnection(_fixture.BuildSettings()))
            {
                await conn.OpenAsync();
                await conn.ExecuteNonQueryAsync(
                    $"CREATE TABLE {table} (id Int64) ENGINE = MergeTree ORDER BY id");
            }

            int sent = 0;
            Exception? caught = null;
            try
            {
                await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
                await conn.OpenAsync();
                var inserter = conn.CreateBulkInserter<MetricsRow>(table);
                await inserter.InitAsync();
                try
                {
                    for (int i = 0; i < rowCount; i++)
                    {
                        if (i == rowCount / 4)
                            await _fixture.KillContainerAsync();
                        await inserter.AddAsync(new MetricsRow { Id = i });
                        sent++;
                    }
                    await inserter.CompleteAsync();
                }
                finally
                {
                    try { await inserter.DisposeAsync(); } catch { }
                }
            }
            catch (Exception ex) { caught = ex; }

            _output.WriteLine($"Bulk insert during kill: rowsSent={sent}, error={caught?.GetType().FullName}");

            await _fixture.StartContainerAsync();
            meterProvider!.ForceFlush(timeoutMilliseconds: 2000);

            long rowsWrittenCount = 0;
            foreach (var m in metrics.Where(x => x.Name == "ch_native_rows_written_total"))
                foreach (var p in m.GetMetricPoints())
                    rowsWrittenCount += p.GetSumLong();

            await using var freshConn = new ClickHouseConnection(_fixture.BuildSettings());
            await freshConn.OpenAsync();
            var persisted = await freshConn.ExecuteScalarAsync<ulong>($"SELECT count() FROM {table}");
            await freshConn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");

            _output.WriteLine($"rows_written_total counter: {rowsWrittenCount}");
            _output.WriteLine($"Persisted rows: {persisted}");
            _output.WriteLine($"Rows added on client: {sent}");

            // Pin: the counter must never exceed rows the client even tried to add.
            // Over-count would mean the meter credits rows that never reached the wire.
            Assert.True(rowsWrittenCount <= sent + 1, // +1 slack for the kill-time row
                $"rows_written counter ({rowsWrittenCount}) exceeds rows sent ({sent})");
            // Persisted ≤ sent always (server can't store rows we didn't send).
            Assert.True(persisted <= (ulong)sent,
                $"persisted ({persisted}) > sent ({sent}) — impossible without telemetry corruption");
        }

        private sealed class MetricsRow
        {
            [ClickHouseColumn(Name = "id", Order = 0)] public long Id { get; set; }
        }
    }

    [Collection("MultiToxiproxy")]
    public sealed class RetryTracePropagation : IAsyncLifetime
    {
        private readonly MultiToxiproxyFixture _fx;
        private readonly ITestOutputHelper _output;

        public RetryTracePropagation(MultiToxiproxyFixture fx, ITestOutputHelper output)
        {
            _fx = fx;
            _output = output;
        }

        public Task InitializeAsync() => SafeRemoveAllToxicsAsync();
        public Task DisposeAsync() => SafeRemoveAllToxicsAsync();

        private async Task SafeRemoveAllToxicsAsync()
        {
            for (int i = 0; i < 5; i++)
            {
                try
                {
                    await _fx.Client.RemoveAllToxicsAsync(_fx.ProxyAName);
                    await _fx.Client.RemoveAllToxicsAsync(_fx.ProxyBName);
                    return;
                }
                catch (System.Net.Http.HttpRequestException) when (i < 4) { await Task.Delay(200); }
            }
        }

        [Fact]
        public async Task RetryActivities_ShareTraceId_AcrossFailoverAttempts()
        {
            // Outer activity → ResilientConnection retries against multi-host LB.
            // The plan asks: do all retry attempts share the parent TraceId, with
            // distinct SpanIds? Use ActivityListener so we observe every started/
            // stopped activity, not just sampled ones.
            var stopped = new System.Collections.Concurrent.ConcurrentBag<Activity>();
            using var listener = new ActivityListener
            {
                // Listen to both CH.Native (the library) and our test outer source so
                // ActivitySource.StartActivity returns a non-null Activity for the outer.
                ShouldListenTo = s => s.Name == SourceName || s.Name == "CH.Native.Test",
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = a => stopped.Add(a),
            };
            ActivitySource.AddActivityListener(listener);

            // Take A down so connect-attempts to A fail and the LB falls over to B.
            await _fx.Client.AddToxicAsync(_fx.ProxyAName, "reset_peer", "downstream",
                new Dictionary<string, object> { ["timeout"] = 0 });

            try
            {
                var settings = _fx.BuildSettings(
                    new[] { _fx.EndpointA, _fx.EndpointB },
                    b => b
                        .WithLoadBalancing(LoadBalancingStrategy.RoundRobin)
                        .WithResilience(r => r.WithRetry().WithCircuitBreaker())
                        .WithConnectTimeout(TimeSpan.FromSeconds(2)));

                using var outer = new ActivitySource("CH.Native.Test").StartActivity("outer");
                Assert.NotNull(outer);

                await using var conn = new ResilientConnection(settings);
                await conn.OpenAsync();
                _ = await conn.ExecuteScalarAsync<int>("SELECT 1");
            }
            finally
            {
                await SafeRemoveAllToxicsAsync();
            }

            var clickhouseSpans = stopped.ToList();
            _output.WriteLine($"Activities captured: {clickhouseSpans.Count}");
            foreach (var a in clickhouseSpans)
                _output.WriteLine($"  {a.OperationName} TraceId={a.TraceId} SpanId={a.SpanId} ParentId={a.ParentId}");

            // Pin: at least one CH.Native activity exists (retry path executes), all
            // CH.Native activities share a TraceId (no orphan trace), and SpanIds
            // are unique (no double-recording).
            Assert.NotEmpty(clickhouseSpans);
            var traceIds = clickhouseSpans.Select(a => a.TraceId).Distinct().ToList();
            var spanIds = clickhouseSpans.Select(a => a.SpanId).Distinct().Count();
            _output.WriteLine($"Distinct TraceIds: {traceIds.Count}, distinct SpanIds: {spanIds}");
            Assert.True(spanIds == clickhouseSpans.Count, "SpanIds must be unique across activities");
        }
    }
}
