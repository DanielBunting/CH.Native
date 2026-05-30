using CH.Native.Connection;
using CH.Native.DependencyInjection;
using CH.Native.Resilience;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace CH.Native.Tests.Unit.DependencyInjection;

/// <summary>
/// Pins that <see cref="IClickHouseDataSourceBuilder.WithResilience(ResilienceOptions)"/>
/// (and the builder-callback overload) flow into
/// <see cref="ClickHouseConnectionSettings.Resilience"/> — the property
/// <c>ClickHouseConnection.OpenAsync</c> consults for retry-on-connect. This closes
/// the DI ergonomics gap where retry was only reachable via a connection string or
/// hand-built settings. No server needed: resolving the DataSource only constructs
/// it (MinPoolSize 0, PrewarmOnStart false), so nothing connects.
/// </summary>
public class WithResilienceBuilderTests
{
    private const string ConnString = "Host=localhost;Port=9000";

    [Fact]
    public void WithResilience_Options_FlowsIntoDataSourceSettings()
    {
        var services = new ServiceCollection();
        services.AddClickHouse(ConnString)
            .WithResilience(ResilienceOptions.WithRetryDefaults());

        using var sp = services.BuildServiceProvider();
        var ds = sp.GetRequiredService<ClickHouseDataSource>();

        Assert.NotNull(ds.Resilience);
        Assert.True(ds.Resilience!.HasRetry);
        Assert.NotNull(ds.Settings.Resilience);
        Assert.Same(ds.Resilience, ds.Settings.Resilience);
    }

    [Fact]
    public void WithResilience_BuilderCallback_ConfiguresRetry()
    {
        var services = new ServiceCollection();
        services.AddClickHouse(ConnString)
            .WithResilience(r => r.WithRetry(new RetryOptions { MaxRetries = 5 }));

        using var sp = services.BuildServiceProvider();
        var ds = sp.GetRequiredService<ClickHouseDataSource>();

        Assert.NotNull(ds.Resilience?.Retry);
        Assert.Equal(5, ds.Resilience!.Retry!.MaxRetries);
    }

    [Fact]
    public void WithoutWithResilience_ResilienceIsNull()
    {
        var services = new ServiceCollection();
        services.AddClickHouse(ConnString);

        using var sp = services.BuildServiceProvider();
        var ds = sp.GetRequiredService<ClickHouseDataSource>();

        Assert.Null(ds.Resilience);
    }

    [Fact]
    public void WithResilience_ComposesWithCredentialProvider()
    {
        // The shared settings-builder seam must apply resilience even when a
        // credential provider is configured (that path rebuilds settings from a
        // fresh builder). We can't assert the rotating rebuild without opening a
        // connection, but registering both must at least resolve cleanly and keep
        // resilience on the baseline settings.
        var services = new ServiceCollection();
        services.AddClickHouse(ConnString)
            .WithPasswordProvider(_ => _ => new ValueTask<string>("pw"))
            .WithResilience(ResilienceOptions.WithRetryDefaults());

        using var sp = services.BuildServiceProvider();
        var ds = sp.GetRequiredService<ClickHouseDataSource>();

        Assert.True(ds.Resilience!.HasRetry);
    }
}
