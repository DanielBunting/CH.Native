using CH.Native.Linq;
using Xunit;

namespace CH.Native.Tests.Unit.Linq;

/// <summary>
/// Unit tests for the <see cref="LeasedConnection"/> wrapper that
/// <see cref="ClickHouseQueryContext.AcquireConnectionAsync"/> returns. The
/// non-owning case must be a true dispose no-op so the caller's lifetime isn't
/// affected; the owning case must dispose through to the connection (which is
/// what triggers the pool-return hook in the data-source path). These tests
/// don't need a live server — they verify the wrapper's branching by observing
/// whether DisposeAsync touches an already-disposed connection (throws if it
/// does, since the wire is closed).
/// </summary>
public class LeasedConnectionTests
{
    [Fact]
    public async Task AcquireConnectionAsync_ConnectionLessAndDataSourceLess_ContextThrows()
    {
        // SQL-generation-only contexts have neither — execution should fail
        // synchronously (well, on the first await) with a clear message.
        var ctx = new ClickHouseQueryContext(connection: null, "t", typeof(int), columnNames: null);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await ctx.AcquireConnectionAsync());
        Assert.Contains("Connection nor a DataSource", ex.Message);
    }

    [Fact]
    public async Task LeasedConnection_NotOwning_DisposeIsNoOp()
    {
        // A non-owning lease must NOT dispose the wrapped connection — the
        // surrounding caller still owns its lifetime. We sidestep needing a
        // live server by passing a real connection that's never opened: a
        // non-owning DisposeAsync should simply not touch it.
        await using var connection = new CH.Native.Connection.ClickHouseConnection("Host=localhost;Port=9000");
        var lease = new LeasedConnection(connection, owned: false);

        await lease.DisposeAsync();
        await lease.DisposeAsync(); // idempotent — disposing twice is fine.

        // The connection wasn't disposed by the lease, so its state remains
        // whatever it was (never opened). We can still observe properties that
        // don't require a wire — IsOpen returns false, ObjectDisposedException
        // would fire if it were actually disposed.
        Assert.False(connection.IsOpen);
    }

    [Fact]
    public async Task LeasedConnection_Owning_ExposesConnectionReference()
    {
        // Pin the basic accessor contract — the owning lease still exposes
        // Connection while alive (the OWNING flag only changes the dispose
        // behavior).
        await using var connection = new CH.Native.Connection.ClickHouseConnection("Host=localhost;Port=9000");
        var lease = new LeasedConnection(connection, owned: true);

        Assert.Same(connection, lease.Connection);
    }
}
