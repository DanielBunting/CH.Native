using CH.Native.Connection;
using CH.Native.DependencyInjection;
using CH.Native.DependencyInjection.HealthChecks;
using CH.Native.SystemTests.Fixtures;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Xunit;

namespace CH.Native.SystemTests.DependencyInjection;

/// <summary>
/// End-to-end coverage for the optional Microsoft.Extensions.DependencyInjection
/// package against a real ClickHouse node.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.DependencyInjection)]
public sealed class DependencyInjectionSystemTests
{
    private readonly SingleNodeFixture _fixture;

    public DependencyInjectionSystemTests(SingleNodeFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task AddClickHouse_UnkeyedDataSource_ExecutesQueriesThroughPool()
    {
        var services = new ServiceCollection();
        services.AddClickHouse(_fixture.ConnectionString)
            .WithPool(options =>
            {
                options.MaxPoolSize = 2;
                options.ConnectionWaitTimeout = TimeSpan.FromSeconds(5);
            });

        await using var provider = services.BuildServiceProvider();
        var dataSource = provider.GetRequiredService<ClickHouseDataSource>();

        var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(async value =>
        {
            await using var connection = await dataSource.OpenConnectionAsync();
            return await connection.ExecuteScalarAsync<int>($"SELECT {value}");
        }));

        Assert.Equal(Enumerable.Range(0, 8), results.Order());

        var stats = dataSource.GetStatistics();
        Assert.Equal(8, stats.TotalRentsServed);
        Assert.True(stats.TotalCreated <= 2,
            $"DI pool should honor MaxPoolSize=2; created {stats.TotalCreated} physical connections.");
    }

    [Fact]
    public async Task AddClickHouse_KeyedDataSource_HealthCheckUsesSelectedKey()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddClickHouse("primary", _fixture.ConnectionString);
        services.AddHealthChecks()
            .AddClickHouse(
                name: "clickhouse-primary",
                serviceKey: "primary",
                timeout: TimeSpan.FromSeconds(5),
                tags: new[] { "ready" });

        await using var provider = services.BuildServiceProvider();

        var dataSource = provider.GetRequiredKeyedService<ClickHouseDataSource>("primary");
        await using (var connection = await dataSource.OpenConnectionAsync())
        {
            Assert.Equal(42, await connection.ExecuteScalarAsync<int>("SELECT 42"));
        }

        var health = provider.GetRequiredService<HealthCheckService>();
        var report = await health.CheckHealthAsync();

        Assert.Equal(HealthStatus.Healthy, report.Status);
        Assert.True(report.Entries.TryGetValue("clickhouse-primary", out var entry),
            "Expected keyed ClickHouse health check to be registered.");
        Assert.Equal(HealthStatus.Healthy, entry.Status);
    }

    [Fact]
    public async Task AddClickHouse_PasswordProvider_OverridesBaselinePassword()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new PasswordHolder(_fixture.Password));
        services.AddClickHouse(builder => builder
                .WithHost(_fixture.Host)
                .WithPort(_fixture.Port)
                .WithCredentials(_fixture.Username, "wrong_password"))
            .WithPasswordProvider(sp =>
            {
                var holder = sp.GetRequiredService<PasswordHolder>();
                return _ => new ValueTask<string>(holder.Password);
            });

        await using var provider = services.BuildServiceProvider();
        var dataSource = provider.GetRequiredService<ClickHouseDataSource>();

        await using var connection = await dataSource.OpenConnectionAsync();
        Assert.Equal(7, await connection.ExecuteScalarAsync<int>("SELECT 7"));
    }

    private sealed record PasswordHolder(string Password);
}
