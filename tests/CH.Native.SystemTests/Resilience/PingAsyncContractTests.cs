using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Resilience;

/// <summary>
/// Pins the contract of <see cref="ClickHouseDataSource.PingAsync(System.Threading.CancellationToken)"/>:
/// it is the documented liveness check that operators wire into <c>/healthz</c>.
/// Existing health-check coverage is at the ASP.NET integration layer; this
/// covers the API surface directly.
/// </summary>
[Collection("RestartableSingleNode")]
[Trait(Categories.Name, Categories.Resilience)]
public class PingAsyncContractTests
{
    private readonly RestartableSingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public PingAsyncContractTests(RestartableSingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task PingAsync_HappyPath_ReturnsTrue_AndDoesNotPoisonPool()
    {
        await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _fx.BuildSettings(),
            MaxPoolSize = 2,
        });

        var result = await ds.PingAsync();
        Assert.True(result);

        // After a successful ping, the connection went back into the pool
        // and the next rent reuses it.
        var statsAfter = ds.GetStatistics();
        _output.WriteLine($"Stats after ping: {statsAfter}");
        Assert.True(statsAfter.Idle >= 1);
        Assert.Equal(0, statsAfter.Busy);

        await using var conn = await ds.OpenConnectionAsync();
        Assert.Equal(1, await conn.ExecuteScalarAsync<int>("SELECT 1"));
    }

    [Fact]
    public async Task PingAsync_WithCancelledToken_ReturnsFalse_DoesNotThrow()
    {
        // PingAsync is documented as a non-throwing health probe — even
        // with a pre-cancelled token, it should swallow OCE and return
        // false rather than propagate.
        await using var ds = new ClickHouseDataSource(_fx.BuildSettings());

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var result = await ds.PingAsync(cts.Token);
        _output.WriteLine($"Pre-cancelled ping result: {result}");
        Assert.False(result);
    }

    [Fact]
    public async Task PingAsync_AgainstStoppedServer_ReturnsFalse_RecoversWhenServerComesBack()
    {
        await using var ds = new ClickHouseDataSource(_fx.BuildSettings());

        // Sanity: server is up, ping succeeds.
        Assert.True(await ds.PingAsync());

        await _fx.StopContainerAsync();
        try
        {
            // Server is down — ping returns false rather than throwing.
            var downResult = await ds.PingAsync();
            _output.WriteLine($"Ping while server down: {downResult}");
            Assert.False(downResult);
        }
        finally
        {
            await _fx.StartContainerAsync();
        }

        // Server is back. Ping recovers — the data source must not have
        // been wedged by the failed pings.
        await PollPingAsync(ds, expected: true, timeout: TimeSpan.FromSeconds(10));
    }

    private async Task PollPingAsync(ClickHouseDataSource ds, bool expected, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                if (await ds.PingAsync() == expected) return;
            }
            catch { /* PingAsync is documented as non-throwing, but be lenient during recovery */ }
            await Task.Delay(200);
        }
        throw new TimeoutException($"Ping never reached expected={expected} within {timeout}.");
    }
}
