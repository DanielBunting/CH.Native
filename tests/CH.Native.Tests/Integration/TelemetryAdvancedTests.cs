using System.Diagnostics;
using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.Telemetry;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

[Collection("ClickHouse")]
public class TelemetryAdvancedTests
{
    private readonly ClickHouseFixture _fixture;

    public TelemetryAdvancedTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Telemetry_BulkInsert_EmitsActivity()
    {
        var activities = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "CH.Native",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => activities.Add(activity)
        };
        ActivitySource.AddActivityListener(listener);

        var tableName = $"test_telem_bulk_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Name String
            ) ENGINE = Memory");

        try
        {
            // Clear activities from setup
            activities.Clear();

            // Perform a bulk insert
            await using var inserter = connection.CreateBulkInserter<TelemetryRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new TelemetryRow { Id = 1, Name = "Alice" });
            await inserter.AddAsync(new TelemetryRow { Id = 2, Name = "Bob" });
            await inserter.AddAsync(new TelemetryRow { Id = 3, Name = "Charlie" });

            await inserter.CompleteAsync();

            // BulkInserter should emit at least one activity for the insert operation
            var bulkInsertActivities = activities
                .Where(a => a.OperationName.Contains("bulk_insert") || a.OperationName.Contains("insert"))
                .ToList();

            Assert.True(bulkInsertActivities.Count > 0,
                $"Expected BulkInserter to emit at least one activity, but found none. " +
                $"All activities: [{string.Join(", ", activities.Select(a => a.OperationName))}]");
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task Telemetry_CancelledQuery_StatusError()
    {
        var activities = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "CH.Native",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => activities.Add(activity)
        };
        ActivitySource.AddActivityListener(listener);

        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Clear activities from connection setup
        activities.Clear();

        // Start a query and cancel it
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));

        try
        {
            await connection.ExecuteScalarAsync<long>(
                "SELECT count() FROM numbers(10000000000)",
                cancellationToken: cts.Token);
        }
        catch (OperationCanceledException)
        {
            // Expected
        }

        // Find the query activity
        var queryActivity = activities.FirstOrDefault(a => a.OperationName == "clickhouse.query");
        Assert.NotNull(queryActivity);

        // Cancelled queries should have Error status per OpenTelemetry conventions
        Assert.Equal(ActivityStatusCode.Error, queryActivity.Status);
    }

    [Fact]
    public async Task Telemetry_QueryIds_Unique()
    {
        var activities = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "CH.Native",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => activities.Add(activity)
        };
        ActivitySource.AddActivityListener(listener);

        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Clear connection-related activities
        activities.Clear();

        // Execute 10 sequential queries
        for (int i = 0; i < 10; i++)
        {
            await connection.ExecuteScalarAsync<int>($"SELECT {i}");
        }

        // Collect all query activities with query_id tags
        var queryActivities = activities
            .Where(a => a.OperationName == "clickhouse.query")
            .ToList();

        Assert.True(queryActivities.Count >= 10,
            $"Expected at least 10 query activities, got {queryActivities.Count}");

        // Extract query IDs
        var queryIds = queryActivities
            .Select(a => a.GetTagItem("db.clickhouse.query_id")?.ToString())
            .Where(id => !string.IsNullOrEmpty(id))
            .ToList();

        Assert.True(queryIds.Count >= 10,
            $"Expected at least 10 query IDs, got {queryIds.Count}");

        // All query IDs should be distinct
        var distinctIds = queryIds.Distinct().ToList();
        Assert.Equal(queryIds.Count, distinctIds.Count);
    }

    [Fact]
    public async Task Telemetry_ParentContext_Propagated()
    {
        var activities = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "CH.Native",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => activities.Add(activity)
        };
        ActivitySource.AddActivityListener(listener);

        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Clear connection-related activities
        activities.Clear();

        // Create a parent activity
        using var parentActivity = ClickHouseActivitySource.Source.StartActivity("test.parent");
        Assert.NotNull(parentActivity);

        // Execute a query inside the parent activity context
        await connection.ExecuteScalarAsync<int>("SELECT 42");

        parentActivity.Stop();

        // Find the child query activity
        var childActivity = activities.FirstOrDefault(a => a.OperationName == "clickhouse.query");
        Assert.NotNull(childActivity);

        // Verify parent-child relationship
        Assert.NotNull(childActivity.ParentId);
        Assert.Contains(parentActivity.Id!, childActivity.ParentId!);
    }

    #region Model Classes

    private class TelemetryRow
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    #endregion
}
