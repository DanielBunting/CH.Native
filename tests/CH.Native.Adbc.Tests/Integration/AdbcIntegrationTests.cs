using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Types;
using CH.Native.Connection;
using Xunit;

namespace CH.Native.Adbc.Tests.Integration;

/// <summary>
/// Integration tests covering the bugs found in the ADBC scalar read path. Each test is written to
/// FAIL against the pre-fix code and pass once the corresponding fix lands.
/// </summary>
[Trait("Category", "Integration")]
[Collection("AdbcClickHouse")]
public class AdbcIntegrationTests : AdbcIntegrationTestBase
{
    public AdbcIntegrationTests(AdbcClickHouseFixture fixture) : base(fixture) { }

    // Finding #1 — DateTime/DateTime64 arrays were built with a hardcoded "UTC" timezone while the
    // schema field carried the column's real timezone, producing an inconsistent RecordBatch.
    [Fact]
    public async Task DateTimeWithTimezone_SchemaAndArrayAgree()
    {
        var table = "adbc_tz_" + Guid.NewGuid().ToString("N");
        await ExecuteSetupAsync(
            $"CREATE TABLE {table} (ts DateTime('Asia/Tokyo')) ENGINE = Memory",
            $"INSERT INTO {table} VALUES ('2026-06-29 12:00:00')");

        using var conn = OpenConnection();
        using var stmt = conn.CreateStatement();
        stmt.SqlQuery = $"SELECT ts FROM {table}";
        var result = stmt.ExecuteQuery();
        using var stream = result.Stream!;
        using var batch = await stream.ReadNextRecordBatchAsync();

        Assert.NotNull(batch);
        var schemaType = Assert.IsType<TimestampType>(batch!.Schema.GetFieldByIndex(0).DataType);
        var arrayType = Assert.IsType<TimestampType>(batch.Column(0).Data.DataType);
        Assert.Equal("Asia/Tokyo", schemaType.Timezone);
        Assert.Equal(schemaType.Timezone, arrayType.Timezone);
    }

    // Finding #1 (DateTime64 variant) — the sub-second-precision path carries both a timezone and a
    // time unit; the array's TimestampType must match the schema field's on both.
    [Fact]
    public async Task DateTime64WithTimezone_SchemaAndArrayAgree()
    {
        var table = "adbc_tz64_" + Guid.NewGuid().ToString("N");
        await ExecuteSetupAsync(
            $"CREATE TABLE {table} (ts DateTime64(3, 'Asia/Tokyo')) ENGINE = Memory",
            $"INSERT INTO {table} VALUES ('2026-06-29 12:00:00.123')");

        using var conn = OpenConnection();
        using var stmt = conn.CreateStatement();
        stmt.SqlQuery = $"SELECT ts FROM {table}";
        var result = stmt.ExecuteQuery();
        using var stream = result.Stream!;
        using var batch = await stream.ReadNextRecordBatchAsync();

        Assert.NotNull(batch);
        var schemaType = Assert.IsType<TimestampType>(batch!.Schema.GetFieldByIndex(0).DataType);
        var arrayType = Assert.IsType<TimestampType>(batch.Column(0).Data.DataType);
        Assert.Equal("Asia/Tokyo", schemaType.Timezone);
        Assert.Equal(TimeUnit.Millisecond, schemaType.Unit);
        Assert.Equal(schemaType.Timezone, arrayType.Timezone);
        Assert.Equal(schemaType.Unit, arrayType.Unit);
    }

    // Finding #2 — GetTableSchema returned after the first block without draining, leaving server
    // messages in the pipe and poisoning the connection for its next use.
    [Fact]
    public async Task GetTableSchema_DoesNotPoisonConnection()
    {
        var table = "adbc_schema_" + Guid.NewGuid().ToString("N");
        await ExecuteSetupAsync($"CREATE TABLE {table} (id UInt32, name String) ENGINE = Memory");

        using var conn = OpenConnection();

        var schema = conn.GetTableSchema(null, "default", table);
        Assert.Equal(2, schema.FieldsList.Count);

        // Reusing the SAME ADBC connection must still work — it would not if the schema query
        // left the underlying ClickHouse connection in a half-read (poisoned) state.
        using var stmt = conn.CreateStatement();
        stmt.SqlQuery = "SELECT 1 AS x";
        var result = stmt.ExecuteQuery();
        using var stream = result.Stream!;
        using var batch = await stream.ReadNextRecordBatchAsync();

        Assert.NotNull(batch);
        var arr = Assert.IsType<UInt8Array>(batch!.Column(0));
        Assert.Equal((byte)1, arr.GetValue(0)!.Value);
    }

    // Finding #3 — ReadNextRecordBatchAsync ignored its CancellationToken, so reads were
    // uncancellable. A pre-cancelled token must surface as an OperationCanceledException.
    [Fact]
    public async Task ReadNextRecordBatch_HonorsCancellation()
    {
        var conn = OpenConnection();
        var stmt = conn.CreateStatement();
        stmt.SqlQuery = "SELECT number FROM system.numbers";
        var result = stmt.ExecuteQuery();
        var stream = result.Stream!;

        var first = await stream.ReadNextRecordBatchAsync();
        Assert.NotNull(first);

        var cancelled = new CancellationToken(canceled: true);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await stream.ReadNextRecordBatchAsync(cancelled));

        // Tear down off the test thread: pre-fix dispose can block on the infinite stream.
        _ = Task.Run(() => { try { stream.Dispose(); conn.Dispose(); } catch { /* best effort */ } });
    }

    // Finding #4 — Disposing a partially-read stream drained the entire remaining result; on an
    // infinite stream that never returns. The fix cancels the server query and returns promptly.
    [Fact]
    public async Task DisposingPartialStream_ReturnsPromptly()
    {
        var conn = OpenConnection();
        var stmt = conn.CreateStatement();
        stmt.SqlQuery = "SELECT number FROM system.numbers";
        var result = stmt.ExecuteQuery();
        var stream = result.Stream!;

        var first = await stream.ReadNextRecordBatchAsync();
        Assert.NotNull(first);

        var disposeTask = Task.Run(() => stream.Dispose());
        var finished = await Task.WhenAny(disposeTask, Task.Delay(TimeSpan.FromSeconds(20)));

        Assert.True(finished == disposeTask,
            "Disposing a partially-read stream should cancel the server query and return promptly, " +
            "not drain the entire (here infinite) result.");

        _ = Task.Run(() => { try { conn.Dispose(); } catch { /* best effort */ } });
    }

    // Finding #5 — ExecuteSchema ran the full query and (via drain-on-dispose) transferred the whole
    // result just to read the schema. On an infinite stream it never returns.
    [Fact]
    public async Task ExecuteSchema_ReturnsPromptlyWithoutDrainingResult()
    {
        var conn = OpenConnection();
        var stmt = conn.CreateStatement();
        stmt.SqlQuery = "SELECT number FROM system.numbers";

        Schema? schema = null;
        var schemaTask = Task.Run(() => { schema = stmt.ExecuteSchema(); });
        var finished = await Task.WhenAny(schemaTask, Task.Delay(TimeSpan.FromSeconds(20)));

        Assert.True(finished == schemaTask,
            "ExecuteSchema should obtain the schema and tear down promptly, not transfer the entire " +
            "(here infinite) result.");
        Assert.NotNull(schema);
        Assert.Equal("number", schema!.GetFieldByIndex(0).Name);

        _ = Task.Run(() => { try { conn.Dispose(); } catch { /* best effort */ } });
    }
}
