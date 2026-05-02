using System.Reflection;
using CH.Native.Connection;
using Xunit;

namespace CH.Native.Tests.Unit.Connection;

/// <summary>
/// Pre-fix <see cref="ClickHouseConnection.DisposeAsync"/> only disposed
/// <c>_writeLock</c> on the no-hook success path. If the pool-return hook
/// threw, the catch block tore down the socket but skipped <c>_writeLock.Dispose()</c>,
/// leaking the SemaphoreSlim and leaving any in-flight waiters blocked
/// indefinitely instead of receiving <see cref="ObjectDisposedException"/>.
/// </summary>
public class ConnectionDisposeHookFailureTests
{
    [Fact]
    public async Task DisposeAsync_HookThrows_DisposesWriteLock()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("127.0.0.1")
            .WithPort(1) // not opened — never connects
            .Build();

        var conn = new ClickHouseConnection(settings);

        // Reach the internal PoolReturnHook setter via reflection to install a
        // throwing hook. Using BindingFlags.Instance | NonPublic lets the test
        // exercise the catch path without requiring an InternalsVisibleTo.
        var hookProp = typeof(ClickHouseConnection).GetProperty(
            "PoolReturnHook",
            BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(hookProp);

        Func<ClickHouseConnection, ValueTask> throwingHook = _ =>
            throw new InvalidOperationException("simulated pool-return failure");
        hookProp!.SetValue(conn, throwingHook);

        await conn.DisposeAsync();

        // Verify _writeLock is disposed: WaitAsync against a disposed SemaphoreSlim
        // throws ObjectDisposedException synchronously.
        var lockField = typeof(ClickHouseConnection).GetField(
            "_writeLock",
            BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(lockField);
        var sem = (SemaphoreSlim)lockField!.GetValue(conn)!;
        Assert.Throws<ObjectDisposedException>(() => sem.Wait(0));
    }

    [Fact]
    public async Task DisposeAsync_NoHook_DisposesWriteLock()
    {
        // Sanity: the no-hook path was already correct pre-fix; pin it so the
        // refactor doesn't regress it.
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("127.0.0.1")
            .WithPort(1)
            .Build();

        var conn = new ClickHouseConnection(settings);
        await conn.DisposeAsync();

        var lockField = typeof(ClickHouseConnection).GetField(
            "_writeLock",
            BindingFlags.Instance | BindingFlags.NonPublic);
        var sem = (SemaphoreSlim)lockField!.GetValue(conn)!;
        Assert.Throws<ObjectDisposedException>(() => sem.Wait(0));
    }
}
