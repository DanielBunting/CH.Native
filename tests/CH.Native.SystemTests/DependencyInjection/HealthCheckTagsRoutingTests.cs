using CH.Native.DependencyInjection;
using CH.Native.DependencyInjection.HealthChecks;
using CH.Native.SystemTests.Fixtures;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.DependencyInjection;

/// <summary>
/// Pins the K8s readiness/liveness routing pattern operators rely on:
/// <c>AddClickHouse(tags: new[] { "ready" })</c> + a predicate filter on
/// the corresponding endpoint. Surface area §5.1 fragility flags
/// stringly-typed tags as a silent-de-routing risk; pin the contract
/// and the typo-failure mode.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.DependencyInjection)]
public class HealthCheckTagsRoutingTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public HealthCheckTagsRoutingTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task ReadyTaggedCheck_RoutesToReadyPredicate_NotLivePredicate()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddClickHouse(_fx.ConnectionString);
        services.AddHealthChecks()
            .AddClickHouse(name: "ch", tags: new[] { "ready" });

        await using var provider = services.BuildServiceProvider();
        var health = provider.GetRequiredService<HealthCheckService>();

        var readyReport = await health.CheckHealthAsync(r => r.Tags.Contains("ready"));
        var liveReport = await health.CheckHealthAsync(r => r.Tags.Contains("live"));

        _output.WriteLine($"ready: {readyReport.Entries.Count} entries; live: {liveReport.Entries.Count}");

        Assert.Single(readyReport.Entries);
        Assert.Empty(liveReport.Entries);
    }

    [Fact]
    public async Task TwoChecks_DifferentTags_EachRoutesToCorrectPredicate()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddClickHouse("ch_a", _fx.ConnectionString);
        services.AddClickHouse("ch_b", _fx.ConnectionString);

        services.AddHealthChecks()
            .AddClickHouse(name: "a_ready", serviceKey: "ch_a", tags: new[] { "ready", "db" })
            .AddClickHouse(name: "b_live", serviceKey: "ch_b", tags: new[] { "live" });

        await using var provider = services.BuildServiceProvider();
        var health = provider.GetRequiredService<HealthCheckService>();

        var readyReport = await health.CheckHealthAsync(r => r.Tags.Contains("ready"));
        var liveReport = await health.CheckHealthAsync(r => r.Tags.Contains("live"));
        var dbReport = await health.CheckHealthAsync(r => r.Tags.Contains("db"));

        Assert.Single(readyReport.Entries, e => e.Key == "a_ready");
        Assert.Single(liveReport.Entries, e => e.Key == "b_live");
        Assert.Single(dbReport.Entries, e => e.Key == "a_ready");
    }

    [Fact]
    public async Task TypoTag_SilentlyExcludesCheck_DocumentedFragility()
    {
        // Surface-area §5.1 fragility: tags are stringly-typed. A typo
        // ("redy" instead of "ready") silently de-routes the check.
        // Pin the failure mode so users grep this test when puzzled.
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddClickHouse(_fx.ConnectionString);
        services.AddHealthChecks()
            .AddClickHouse(name: "ch", tags: new[] { "redy" });

        await using var provider = services.BuildServiceProvider();
        var health = provider.GetRequiredService<HealthCheckService>();

        // Filtering by the intended tag silently misses the check.
        var report = await health.CheckHealthAsync(r => r.Tags.Contains("ready"));
        Assert.Empty(report.Entries);

        // The only way to find it is the (mis-spelled) actual tag.
        var typoReport = await health.CheckHealthAsync(r => r.Tags.Contains("redy"));
        Assert.Single(typoReport.Entries);
    }
}
