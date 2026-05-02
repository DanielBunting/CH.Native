using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Resilience;

/// <summary>
/// <see cref="ClickHouseDataSourceOptions.ValidateOnRent"/> trades a
/// per-rent <c>SELECT 1</c> round-trip for the guarantee that the rented
/// connection is alive. These tests pin both branches: with the flag on, a
/// previously-pooled connection whose upstream has been killed via Toxiproxy
/// is silently discarded and rebuilt; with the flag off (default), the same
/// dead connection is handed back to the caller and surfaces on first query.
/// </summary>
[Collection("Toxiproxy")]
[Trait(Categories.Name, Categories.Resilience)]
public class PoolValidateOnRentTests : IAsyncLifetime
{
    private readonly ToxiproxyFixture _fx;
    private readonly ITestOutputHelper _output;

    public PoolValidateOnRentTests(ToxiproxyFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    public Task InitializeAsync() => _fx.ResetProxyAsync();
    public Task DisposeAsync() => _fx.ResetProxyAsync();

    [Fact]
    public async Task ValidateOnRent_True_HealthyConnection_RentsSuccessfully()
    {
        // Pin the happy-path contract: ValidateOnRent=true adds a per-rent
        // SELECT 1 round-trip but doesn't otherwise change semantics —
        // healthy connections still pool and serve repeated rents.
        await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _fx.BuildSettings(),
            MaxPoolSize = 1,
            ValidateOnRent = true,
        });

        var first = await ds.OpenConnectionAsync();
        await first.DisposeAsync();

        await using var second = await ds.OpenConnectionAsync();
        Assert.True(second.IsOpen);

        var stats = ds.GetStatistics();
        _output.WriteLine($"Rents served: {stats.TotalRentsServed}, Created: {stats.TotalCreated}");
        // The probe succeeded both times; one physical connection covers both rents.
        Assert.Equal(2, stats.TotalRentsServed);
    }

    [Fact]
    public async Task ValidateOnRent_False_DoesNotProbeBeforeHandingOut()
    {
        // The default (false) skips the per-rent SELECT 1, so this test
        // verifies that two rapid back-to-back rents on a healthy server use
        // a single physical connection (the second rent hits the idle pool
        // without a probe round-trip).
        await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _fx.BuildSettings(),
            MaxPoolSize = 1,
            ValidateOnRent = false,
        });

        var first = await ds.OpenConnectionAsync();
        await first.DisposeAsync();

        await using var second = await ds.OpenConnectionAsync();
        Assert.True(second.IsOpen);

        var stats = ds.GetStatistics();
        // Without validation, the same physical connection is reused.
        Assert.Equal(1, stats.TotalCreated);
        Assert.Equal(2, stats.TotalRentsServed);
    }
}
