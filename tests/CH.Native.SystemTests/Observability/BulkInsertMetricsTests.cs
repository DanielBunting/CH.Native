using System.Diagnostics;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using CH.Native.Telemetry;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Observability;

/// <summary>
/// Pins the bulk-insert telemetry surface end-to-end:
///   - <c>ch_native_rows_written_total</c> counter is emitted once per batch
///     flush, with the documented tags (<c>db.system</c>, <c>db.name</c>,
///     <c>db.clickhouse.table</c>).
///   - The <c>clickhouse.bulk_insert</c> activity span is created with the
///     expected attributes.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Observability)]
public class BulkInsertMetricsTests
{
    private const string MeterName = "CH.Native";
    private const string ActivitySourceName = "CH.Native";

    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public BulkInsertMetricsTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    public class IdRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public long Id { get; set; }
    }

    [Fact]
    public async Task RowsWrittenTotal_IncrementsByBatch_WithDocumentedTags()
    {
        var metrics = new List<Metric>();
        using var meterProvider = Sdk.CreateMeterProviderBuilder()
            .AddMeter(MeterName)
            .AddInMemoryExporter(metrics)
            .Build();

        var table = $"bulk_metric_{Guid.NewGuid():N}";
        const int rowCount = 1500;  // > default batch so > 1 flush should occur

        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        try
        {
            await conn.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id Int64) ENGINE = MergeTree ORDER BY id");

            await using var inserter = conn.CreateBulkInserter<IdRow>(table);
            await inserter.InitAsync();

            for (int i = 0; i < rowCount; i++)
                await inserter.AddAsync(new IdRow { Id = i });
            await inserter.CompleteAsync();

            // Force the meter to flush its buffered measurements.
            meterProvider.ForceFlush(timeoutMilliseconds: 5000);

            // Locate ch_native_rows_written_total in the captured metrics.
            var writtenMetric = metrics.SingleOrDefault(m => m.Name == "ch_native_rows_written_total");
            Assert.NotNull(writtenMetric);

            long totalRows = 0;
            string? observedTable = null;
            string? observedDb = null;
            foreach (var pt in writtenMetric!.GetMetricPoints())
            {
                totalRows += pt.GetSumLong();
                foreach (var tag in pt.Tags)
                {
                    if (tag.Key == "db.clickhouse.table") observedTable = tag.Value as string;
                    if (tag.Key == "db.name") observedDb = tag.Value as string;
                }
            }

            _output.WriteLine($"rows_written_total sum = {totalRows} (expected {rowCount})");
            _output.WriteLine($"tags: db.clickhouse.table={observedTable}, db.name={observedDb}");

            Assert.Equal(rowCount, totalRows);
            Assert.Equal(table, observedTable);
            Assert.NotNull(observedDb);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task BulkInsertActivity_HasExpectedAttributes()
    {
        var activities = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = src => src.Name == ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStopped = a => activities.Add(a),
        };
        ActivitySource.AddActivityListener(listener);

        var table = $"bulk_span_{Guid.NewGuid():N}";

        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        try
        {
            await conn.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id Int64) ENGINE = MergeTree ORDER BY id");

            await using var inserter = conn.CreateBulkInserter<IdRow>(table);
            await inserter.InitAsync();
            for (int i = 0; i < 100; i++)
                await inserter.AddAsync(new IdRow { Id = i });
            await inserter.CompleteAsync();

            // Find the parent bulk-insert activity.
            var bulk = activities.SingleOrDefault(a => a.OperationName == "clickhouse.bulk_insert");
            Assert.NotNull(bulk);

            // Pin the documented attributes.
            Assert.Equal("clickhouse", bulk!.GetTagItem("db.system"));
            Assert.Equal(table, bulk.GetTagItem("db.clickhouse.table"));
            Assert.NotNull(bulk.GetTagItem("db.name"));
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }
}
