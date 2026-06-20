using System.Data;
using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

// Layer-1 hardening for unsupported-type failures (.tmp/_found-issues/06): when the
// reader factory rejects a column type MID-BLOCK, the connection cannot be salvaged
// (column data is not length-prefixed, so the remaining response bytes are
// unparseable). What we guarantee instead:
//   1. the ORIGINAL exception says the connection was closed (full diagnosis up front),
//   2. the connection closes EAGERLY (State == Closed right away, server session
//      released) rather than lingering broken until its next use,
//   3. the next use names the real cause ("Connection is broken: …"), and
//   4. a pooled DataSource never hands the dead connection back out.
// Trigger: an aggregate state format the registry doesn't decode (same as
// ConnectionRecoveryTests — the last reliably unsupported wire type now that
// Nothing/Interval are implemented).
[Collection("ClickHouse")]
public class UnsupportedTypeFailureHardeningTests
{
    private const string UnsupportedProjection = "uniqExactState(toUInt64(number))";

    private readonly ClickHouseFixture _fixture;

    public UnsupportedTypeFailureHardeningTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task OriginalException_SaysConnectionWasClosed()
    {
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        var ex = await Assert.ThrowsAsync<NotSupportedException>(
            () => conn.ExecuteScalarAsync<object>($"SELECT {UnsupportedProjection} FROM numbers(10)"));

        Assert.Contains("not supported", ex.Message);
        Assert.Contains("connection has been closed", ex.Message);
        // The un-enriched reader-factory failure rides along as the inner exception.
        Assert.IsType<NotSupportedException>(ex.InnerException);
    }

    [Fact]
    public async Task Connection_ClosesEagerly_StateIsClosed()
    {
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();
        Assert.Equal(ConnectionState.Open, conn.State);

        await Assert.ThrowsAsync<NotSupportedException>(
            () => conn.ExecuteScalarAsync<object>($"SELECT {UnsupportedProjection} FROM numbers(10)"));

        Assert.Equal(ConnectionState.Closed, conn.State);
    }

    [Fact]
    public async Task NextUse_NamesTheRealCause()
    {
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        await Assert.ThrowsAsync<NotSupportedException>(
            () => conn.ExecuteScalarAsync<object>($"SELECT {UnsupportedProjection} FROM numbers(10)"));

        var next = await Assert.ThrowsAsync<InvalidOperationException>(
            () => conn.ExecuteScalarAsync<int>("SELECT 1"));
        Assert.Contains("Connection is broken", next.Message);
        Assert.Contains("could not be fully read", next.Message);
    }

    [Fact]
    public async Task StateChangeEvent_FiresOpenToClosed()
    {
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        var transitions = new List<(ConnectionState From, ConnectionState To)>();
        conn.StateChange += (_, e) => transitions.Add((e.OriginalState, e.CurrentState));

        await Assert.ThrowsAsync<NotSupportedException>(
            () => conn.ExecuteScalarAsync<object>($"SELECT {UnsupportedProjection} FROM numbers(10)"));

        Assert.Contains((ConnectionState.Open, ConnectionState.Closed), transitions);
    }

    [Fact]
    public async Task PooledDataSource_DoesNotReuseTheDeadConnection()
    {
        await using var dataSource = new ClickHouseDataSource(_fixture.ConnectionString);

        ClickHouseConnection pooled;
        await using (pooled = await dataSource.OpenConnectionAsync())
        {
            await Assert.ThrowsAsync<NotSupportedException>(
                () => pooled.ExecuteScalarAsync<object>($"SELECT {UnsupportedProjection} FROM numbers(10)"));
        } // returns to pool — must be discarded, not pooled

        await using var fresh = await dataSource.OpenConnectionAsync();
        Assert.NotSame(pooled, fresh);
        Assert.Equal(42, await fresh.ExecuteScalarAsync<int>("SELECT 42"));
    }

    // Typed fast path: QueryTypedAsync<T> reads blocks through its own pump
    // (ReadTypedBlocksAsync) rather than ReadServerMessagesAsync, and must carry the
    // same enriched exception and eager-close semantics.
    private class AnyRow
    {
        public object? Value { get; set; }
    }

    [Fact]
    public async Task QueryTyped_FailureAlsoEnrichesAndClosesEagerly()
    {
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        var ex = await Assert.ThrowsAsync<NotSupportedException>(async () =>
        {
            await foreach (var _ in conn.QueryTypedAsync<AnyRow>(
                $"SELECT {UnsupportedProjection} AS value FROM numbers(10)")) { }
        });

        Assert.Contains("connection has been closed", ex.Message);
        Assert.IsType<NotSupportedException>(ex.InnerException);
        Assert.Equal(ConnectionState.Closed, conn.State);

        var next = await Assert.ThrowsAsync<InvalidOperationException>(
            () => conn.ExecuteScalarAsync<int>("SELECT 1"));
        Assert.Contains("Connection is broken", next.Message);
    }

    // Streaming path: the failure surfaces through QueryStreamAsync's enumerator and
    // must carry the same eager-close semantics as the scalar path.
    [Fact]
    public async Task QueryStream_FailureAlsoClosesEagerly()
    {
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
        {
            await foreach (var _ in conn.QueryStreamAsync(
                $"SELECT {UnsupportedProjection} FROM numbers(10)")) { }
        });

        Assert.Equal(ConnectionState.Closed, conn.State);
    }
}
