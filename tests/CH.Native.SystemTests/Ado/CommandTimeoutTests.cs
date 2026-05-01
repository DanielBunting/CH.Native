using System.Data;
using CH.Native.Ado;
using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Ado;

/// <summary>
/// Pins the ADO <see cref="DbCommand.CommandTimeout"/> contract end-to-end.
/// Frameworks (EF Core, Dapper) auto-configure this property and depend on it
/// to terminate runaway queries — anyone using the ADO surface in a typical
/// .NET stack hits this path.
///
/// <para>
/// Verifies:
/// </para>
/// <list type="bullet">
/// <item><description>A query running longer than <c>CommandTimeout</c> is cancelled (not
///     left to run to natural completion).</description></item>
/// <item><description>The cancellation surfaces as <see cref="OperationCanceledException"/>
///     (or a derived type), not as a server-side timeout exception.</description></item>
/// <item><description>The underlying connection remains reusable for a subsequent query
///     — i.e., the wire was drained correctly.</description></item>
/// <item><description><c>CommandTimeout = 0</c> (or negative) disables the timeout per
///     ADO convention.</description></item>
/// </list>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class CommandTimeoutTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public CommandTimeoutTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task ExecuteScalar_ExceedsCommandTimeout_ThrowsOperationCanceled()
    {
        await using var conn = new ClickHouseDbConnection(_fx.ConnectionString);
        await conn.OpenAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT sleep(2)";
        cmd.CommandTimeout = 1; // 1 second — must fire before sleep(2) returns

        var sw = System.Diagnostics.Stopwatch.StartNew();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await cmd.ExecuteScalarAsync();
        });
        sw.Stop();

        _output.WriteLine($"CommandTimeout fired in {sw.ElapsedMilliseconds} ms");
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(2),
            $"CommandTimeout=1s should fire before the 2s sleep completes; took {sw.Elapsed}");
    }

    [Fact]
    public async Task ExecuteNonQuery_ExceedsCommandTimeout_ThrowsOperationCanceled()
    {
        await using var conn = new ClickHouseDbConnection(_fx.ConnectionString);
        await conn.OpenAsync();

        var cmd = conn.CreateCommand();
        // Use a non-trivial DDL-on-sleep pattern: SET takes effect but sleep
        // forces the query to run long enough to trip the timeout.
        cmd.CommandText = "SELECT sleep(2) FORMAT Null";
        cmd.CommandTimeout = 1;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await cmd.ExecuteNonQueryAsync();
        });
    }

    [Fact]
    public async Task ExecuteReader_ExceedsCommandTimeout_ThrowsOperationCanceled()
    {
        await using var conn = new ClickHouseDbConnection(_fx.ConnectionString);
        await conn.OpenAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT sleep(2)";
        cmd.CommandTimeout = 1;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync()) { }
        });
    }

    [Fact]
    public async Task ConnectionReusable_AfterCommandTimeout()
    {
        // After a timed-out query, the wire must be drained so the next query
        // succeeds. Without this, the connection is poisoned and every
        // subsequent ADO call on this connection would surface a confusing
        // "wire out of sync" error.
        await using var conn = new ClickHouseDbConnection(_fx.ConnectionString);
        await conn.OpenAsync();

        var slow = conn.CreateCommand();
        slow.CommandText = "SELECT sleep(2)";
        slow.CommandTimeout = 1;
        try { await slow.ExecuteScalarAsync(); }
        catch (OperationCanceledException) { /* expected */ }

        // Same connection — must be usable.
        var fast = conn.CreateCommand();
        fast.CommandText = "SELECT 42";
        fast.CommandTimeout = 30;
        var result = await fast.ExecuteScalarAsync();
        Assert.Equal(42, Convert.ToInt32(result));
    }

    [Fact]
    public async Task CommandTimeoutZero_DisablesTimeout()
    {
        // ADO convention: CommandTimeout = 0 (or negative) means "no timeout".
        // CreateTimeoutCts returns null in that case, so the call relies
        // solely on the caller's CancellationToken.
        await using var conn = new ClickHouseDbConnection(_fx.ConnectionString);
        await conn.OpenAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT sleep(1)";
        cmd.CommandTimeout = 0;

        // Must succeed — no timeout to fire.
        var result = await cmd.ExecuteScalarAsync();
        // sleep() returns 0 — the value isn't important, only that it didn't time out.
        Assert.NotNull(result);
    }

    [Fact]
    public async Task ExternalCancellation_TakesPrecedenceOverCommandTimeout()
    {
        // If both the command timeout and an explicit CancellationToken are
        // configured, whichever fires first should cancel the query. We use
        // a long-running streaming SELECT (numbers(N) is interruptible mid-
        // stream, unlike sleep() which the server doesn't poll for cancel
        // until the sleep completes) so the cancel actually interrupts.
        await using var conn = new ClickHouseDbConnection(_fx.ConnectionString);
        await conn.OpenAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT count() FROM numbers(10000000000)"; // 10B rows
        cmd.CommandTimeout = 30; // would not fire

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
        var sw = System.Diagnostics.Stopwatch.StartNew();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await cmd.ExecuteScalarAsync(cts.Token);
        });
        sw.Stop();
        _output.WriteLine($"External-token cancel fired in {sw.ElapsedMilliseconds} ms");
        // 10B-row scan would naturally take many seconds; cancellation must
        // fire well before that.
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(10),
            $"External cancellation should fire promptly; took {sw.Elapsed}");
    }
}
