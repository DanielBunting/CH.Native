using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Finding #14: <see cref="ClickHouseDataSource.DisposeAsync"/> sets
/// <c>_disposed = true</c>, drains idle connections, and then calls
/// <c>_gate.Dispose()</c>. Threads parked in <c>_gate.WaitAsync()</c> that unblock
/// mid-teardown can see partially-torn state.
/// </summary>
[Collection("ClickHouse")]
public class DataSourceDisposalRaceTests
{
    private readonly ClickHouseFixture _fixture;

    public DataSourceDisposalRaceTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Dispose_WithPendingWaiter_CompletesOrThrowsCleanly()
    {
        // Saturate a single-permit pool so the next rent parks on the gate.
        var options = new ClickHouseDataSourceOptions
        {
            Settings = ClickHouseConnectionSettings.Parse(_fixture.ConnectionString),
            MaxPoolSize = 1,
            ConnectionWaitTimeout = TimeSpan.FromSeconds(5),
        };

        var dataSource = new ClickHouseDataSource(options);

        // Take the one permit and keep it.
        var held = await dataSource.OpenConnectionAsync();

        // Park a second rent on the gate.
        var waiter = Task.Run(async () =>
        {
            try
            {
                return await dataSource.OpenConnectionAsync();
            }
            catch (Exception ex)
            {
                return (object)ex;
            }
        });

        // Small delay to ensure the waiter is parked.
        await Task.Delay(200);

        // Dispose while the waiter is parked.
        var disposeTask = dataSource.DisposeAsync().AsTask();

        // Meanwhile release the held permit — this unparks the waiter. Depending on
        // whether the waiter sees a disposed gate or a valid permit, it either:
        //   - returns an open connection (the inner ctor should handle teardown), or
        //   - throws ObjectDisposedException/TimeoutException.
        await held.DisposeAsync();

        // Neither of these should hang indefinitely.
        await Task.WhenAll(disposeTask, waiter).WaitAsync(TimeSpan.FromSeconds(15));

        // Waiter must have terminated with either a connection or a clean exception.
        var result = await waiter;
        if (result is ClickHouseConnection conn)
        {
            // Nothing to assert beyond "it didn't crash"; tear it down.
            await conn.DisposeAsync();
        }
        else
        {
            Assert.True(
                result is ObjectDisposedException
                    or InvalidOperationException
                    or TimeoutException,
                $"Unexpected exception type for waiter: {result.GetType()}");
        }
    }

    [Fact]
    public async Task OpenConnection_AfterDispose_ThrowsObjectDisposed()
    {
        var options = new ClickHouseDataSourceOptions
        {
            Settings = ClickHouseConnectionSettings.Parse(_fixture.ConnectionString),
            MaxPoolSize = 1,
        };
        var dataSource = new ClickHouseDataSource(options);

        await dataSource.DisposeAsync();

        // Clean post-dispose semantics: ObjectDisposedException, not TimeoutException
        // and not a hung WaitAsync.
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await dataSource.OpenConnectionAsync());
    }

    [Fact]
    public async Task Dispose_DoesNotDeadlockWithConcurrentRents()
    {
        // Stress: many concurrent rents racing against dispose. Guards against:
        //   - deadlock (via WaitAsync on disposed gate)
        //   - unbalanced Release (ObjectDisposedException/SemaphoreFullException)
        var options = new ClickHouseDataSourceOptions
        {
            Settings = ClickHouseConnectionSettings.Parse(_fixture.ConnectionString),
            MaxPoolSize = 4,
            ConnectionWaitTimeout = TimeSpan.FromSeconds(5),
        };
        var dataSource = new ClickHouseDataSource(options);

        var cts = new CancellationTokenSource();
        var renters = Enumerable.Range(0, 16).Select(_ => Task.Run(async () =>
        {
            while (!cts.IsCancellationRequested)
            {
                try
                {
                    await using var c = await dataSource.OpenConnectionAsync(cts.Token);
                    await c.ExecuteScalarAsync<int>("SELECT 1", cancellationToken: cts.Token);
                }
                catch (ObjectDisposedException) { return; }
                catch (OperationCanceledException) { return; }
                catch (TimeoutException) { /* retry or end */ }
            }
        })).ToArray();

        await Task.Delay(500);

        var disposeTask = dataSource.DisposeAsync().AsTask();
        cts.Cancel();

        // 15s is generous; WaitAsync after dispose should not hang indefinitely.
        await Task.WhenAll(renters.Append(disposeTask)).WaitAsync(TimeSpan.FromSeconds(15));
    }
}
