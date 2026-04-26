using System.Diagnostics;
using System.Diagnostics.Metrics;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using Xunit;

namespace CH.Native.SystemTests.Observability;

/// <summary>
/// Wires the OpenTelemetry SDK with in-memory exporters and asserts that the library's
/// instrumentation produces correctly-shaped traces and metrics under realistic flows.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Observability)]
public class OpenTelemetryPipelineTests
{
    private const string SourceName = "CH.Native";
    private readonly SingleNodeFixture _fixture;

    public OpenTelemetryPipelineTests(SingleNodeFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Tracing_ProducesActivitiesPerQuery()
    {
        var spans = new List<Activity>();
        using var tracer = Sdk.CreateTracerProviderBuilder()
            .AddSource(SourceName)
            .AddInMemoryExporter(spans)
            .Build();

        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        for (int i = 0; i < 5; i++)
            _ = await conn.ExecuteScalarAsync<int>("SELECT 1");

        // Force flush of any in-flight activities.
        tracer!.ForceFlush(timeoutMilliseconds: 1000);

        Assert.NotEmpty(spans);
        // At least one span per query.
        Assert.True(spans.Count >= 5,
            $"Expected ≥ 5 query spans, got {spans.Count}");

        foreach (var s in spans)
        {
            Assert.Equal(SourceName, s.Source.Name);
            Assert.True(s.Duration > TimeSpan.Zero,
                $"Span {s.OperationName} has zero/negative duration {s.Duration}.");
        }

        // At least one span must carry a recognizable db.system tag identifying ClickHouse.
        var hasDbSystem = spans.Any(s => s.TagObjects.Any(t =>
            t.Key == "db.system" && string.Equals(t.Value?.ToString(), "clickhouse", StringComparison.OrdinalIgnoreCase)));
        Assert.True(hasDbSystem,
            "Expected at least one span tagged db.system=clickhouse.");

        // At least one span must carry a non-empty SQL statement attribute.
        var hasStatement = spans.Any(s => s.TagObjects.Any(t =>
            (t.Key == "db.statement" || t.Key == "db.query.text")
            && !string.IsNullOrEmpty(t.Value?.ToString())));
        Assert.True(hasStatement,
            "Expected at least one span with a non-empty db.statement or db.query.text tag.");
    }

    [Fact]
    public async Task Metrics_RowsAndBytesIncrementForRealWorkload()
    {
        var metrics = new List<Metric>();
        using var meter = Sdk.CreateMeterProviderBuilder()
            .AddMeter(SourceName)
            .AddInMemoryExporter(metrics)
            .Build();

        var table = $"otel_metrics_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, name String) ENGINE = Memory");

        try
        {
            await using (var inserter = conn.CreateBulkInserter<Row>(table))
            {
                await inserter.InitAsync();
                for (int i = 0; i < 1000; i++)
                    await inserter.AddAsync(new Row { Id = i, Name = $"v{i}" });
                await inserter.CompleteAsync();
            }

            var read = 0;
            await foreach (var r in conn.QueryAsync($"SELECT id, name FROM {table}"))
            {
                _ = r.GetFieldValue<int>(0);
                _ = r.GetFieldValue<string>(1);
                read++;
            }
            Assert.Equal(1000, read);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }

        meter!.ForceFlush(timeoutMilliseconds: 1000);
        Assert.NotEmpty(metrics);

        long SumLong(string metricName)
        {
            long sum = 0;
            foreach (var m in metrics.Where(x => x.Name == metricName))
            {
                foreach (var p in m.GetMetricPoints())
                    sum += p.GetSumLong();
            }
            return sum;
        }

        // queries_total must have a strictly positive value — not just exist.
        var queries = SumLong("ch_native_queries_total");
        Assert.True(queries > 0, $"ch_native_queries_total reported {queries}; expected > 0.");

        // rows_read_total must reflect the 1000 streamed rows (allow some slack for
        // additional bookkeeping queries the test fires — DROP, count, etc.).
        var rowsRead = SumLong("ch_native_rows_read_total");
        Assert.True(rowsRead >= 1000,
            $"ch_native_rows_read_total reported {rowsRead}; expected ≥ 1000 (the streamed rows).");
    }

    [Fact]
    public async Task Tracing_ContextPropagatesAcrossAsyncContinuations()
    {
        var spans = new List<Activity>();
        using var tracer = Sdk.CreateTracerProviderBuilder()
            .AddSource(SourceName)
            .AddInMemoryExporter(spans)
            .Build();

        using var rootSource = new ActivitySource("CH.Native.SystemTests.Otel");
        using var providerForRoot = Sdk.CreateTracerProviderBuilder()
            .AddSource("CH.Native.SystemTests.Otel")
            .AddInMemoryExporter(spans)
            .Build();

        using var root = rootSource.StartActivity("test-root");
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        _ = await conn.ExecuteScalarAsync<int>("SELECT 1");

        // After the await returned, we're on a continuation thread. Activity.Current
        // must still be the root activity, not null.
        Assert.NotNull(Activity.Current);
        Assert.Equal(root!.Id, Activity.Current!.Id);

        tracer!.ForceFlush(timeoutMilliseconds: 1000);
        providerForRoot!.ForceFlush(timeoutMilliseconds: 1000);

        // CH.Native span(s) should share the root's TraceId.
        var chSpans = spans.Where(s => s.Source.Name == SourceName).ToList();
        Assert.NotEmpty(chSpans);
        foreach (var s in chSpans)
            Assert.Equal(root!.TraceId, s.TraceId);
    }

    [Fact]
    public async Task Tracing_ParameterCardinalityStaysBoundedAcrossDistinctQueries()
    {
        // 100 distinct literal SQL strings exercised. If db.statement is the raw SQL
        // (no sanitization), cardinality is 100 — which is bad for trace backends.
        // The contract this test pins: distinct statement count must be < 5
        // (i.e. parameters/literals must be templated out). Today the library does
        // NOT do this — leave the test failing so the gap is visible.
        var spans = new List<Activity>();
        using var tracer = Sdk.CreateTracerProviderBuilder()
            .AddSource(SourceName)
            .AddInMemoryExporter(spans)
            .Build();

        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        for (int i = 0; i < 100; i++)
            _ = await conn.ExecuteScalarAsync<int>($"SELECT {i}");

        tracer!.ForceFlush(timeoutMilliseconds: 1000);

        var chSpans = spans.Where(s => s.Source.Name == SourceName).ToList();
        var distinctStatements = chSpans
            .SelectMany(s => s.TagObjects)
            .Where(t => t.Key is "db.statement" or "db.query.text")
            .Select(t => t.Value?.ToString() ?? "")
            .ToHashSet();

        // If db.statement isn't tagged at all, cardinality is trivially bounded — OK.
        if (distinctStatements.Count == 0)
            return;

        // Otherwise the library must template/sanitize values. Strict upper bound:
        // < 5 distinct statements for 100 distinct literal queries (one templated form
        // plus a small handful of unrelated ones from setup/connection).
        Assert.True(distinctStatements.Count < 5,
            $"Span cardinality is unbounded — {distinctStatements.Count} distinct statements " +
            "for 100 distinct literal SELECTs. Sanitization gap.");
    }

    private class Row
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "name", Order = 1)]
        public string Name { get; set; } = "";
    }
}
