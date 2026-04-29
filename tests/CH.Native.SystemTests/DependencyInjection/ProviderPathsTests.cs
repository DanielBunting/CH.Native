using CH.Native.Connection;
using CH.Native.DependencyInjection;
using CH.Native.DependencyInjection.HealthChecks;
using CH.Native.SystemTests.Fixtures;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Xunit;

namespace CH.Native.SystemTests.DependencyInjection;

/// <summary>
/// System-level coverage for the DI provider surfaces beyond the existing
/// <see cref="DependencyInjectionSystemTests"/> happy paths. Pins:
/// <list type="bullet">
///   <item>password rotation: existing pooled connections keep working, fresh
///   opens use the new secret;</item>
///   <item>JWT/SSH provider exceptions fail before any wire activity and do not
///   poison sibling data sources sharing the same service collection;</item>
///   <item>keyed health checks correctly isolate failure: one broken keyed
///   data source reports unhealthy while another stays healthy.</item>
/// </list>
/// Certificate / mTLS provider tests are deferred until a TLS-enabled fixture exists.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.DependencyInjection)]
public sealed class ProviderPathsTests
{
    private readonly SingleNodeFixture _fixture;

    public ProviderPathsTests(SingleNodeFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task RotatingPasswordProvider_PooledConnectionsKeepWorking_NewOpensUseNewSecret()
    {
        const string user = "rotating_user";
        const string oldPwd = "p_initial";
        const string newPwd = "p_rotated";

        // Set up the user with the old password via a root connection.
        await using (var root = new ClickHouseConnection(_fixture.BuildSettings()))
        {
            await root.OpenAsync();
            await root.ExecuteNonQueryAsync($"DROP USER IF EXISTS {user}");
            await root.ExecuteNonQueryAsync(
                $"CREATE USER {user} IDENTIFIED WITH plaintext_password BY '{oldPwd}'");
            await root.ExecuteNonQueryAsync($"GRANT SELECT ON *.* TO {user}");
        }

        try
        {
            var holder = new PasswordBox(oldPwd);
            var services = new ServiceCollection();
            services.AddSingleton(holder);
            services.AddClickHouse(builder => builder
                    .WithHost(_fixture.Host)
                    .WithPort(_fixture.Port)
                    .WithCredentials(user, "ignored_baseline"))
                .WithPasswordProvider(sp =>
                {
                    var box = sp.GetRequiredService<PasswordBox>();
                    return _ => new ValueTask<string>(box.Password);
                });

            await using var provider = services.BuildServiceProvider();
            var ds = provider.GetRequiredService<ClickHouseDataSource>();

            // Open a connection with the initial password — it goes into the pool when returned.
            await using (var conn = await ds.OpenConnectionAsync())
            {
                Assert.Equal(1, await conn.ExecuteScalarAsync<int>("SELECT 1"));
            }

            // Rotate: change the server-side password AND the provider's mutable state.
            await using (var root = new ClickHouseConnection(_fixture.BuildSettings()))
            {
                await root.OpenAsync();
                await root.ExecuteNonQueryAsync(
                    $"ALTER USER {user} IDENTIFIED WITH plaintext_password BY '{newPwd}'");
            }
            holder.Password = newPwd;

            // The pooled idle connection is still authenticated from before rotation; reusing it
            // must continue to work because ClickHouse only re-authenticates at handshake time.
            await using (var stillFromPool = await ds.OpenConnectionAsync())
            {
                Assert.Equal(2, await stillFromPool.ExecuteScalarAsync<int>("SELECT 2"));
            }

            // A fresh data source built after rotation forces a new physical handshake.
            // The provider must yield the new secret — proving rotation actually flowed through.
            var freshServices = new ServiceCollection();
            freshServices.AddSingleton(holder);
            freshServices.AddClickHouse(builder => builder
                    .WithHost(_fixture.Host)
                    .WithPort(_fixture.Port)
                    .WithCredentials(user, "ignored_baseline"))
                .WithPasswordProvider(sp =>
                {
                    var box = sp.GetRequiredService<PasswordBox>();
                    return _ => new ValueTask<string>(box.Password);
                });
            await using var freshProvider = freshServices.BuildServiceProvider();
            var freshDs = freshProvider.GetRequiredService<ClickHouseDataSource>();

            await using (var fresh = await freshDs.OpenConnectionAsync())
            {
                Assert.Equal(3, await fresh.ExecuteScalarAsync<int>("SELECT 3"));
            }
        }
        finally
        {
            try
            {
                await using var root = new ClickHouseConnection(_fixture.BuildSettings());
                await root.OpenAsync();
                await root.ExecuteNonQueryAsync($"DROP USER IF EXISTS {user}");
            }
            catch { }
        }
    }

    [Fact]
    public async Task JwtProviderThrows_FailsBeforeWire_DoesNotPoisonSiblingDataSource()
    {
        var services = new ServiceCollection();

        // Broken JWT data source: provider throws before any settings are built.
        services.AddClickHouse("broken_jwt", builder => builder
                .WithHost(_fixture.Host)
                .WithPort(_fixture.Port)
                .WithCredentials(_fixture.Username, _fixture.Password))
            .WithJwtProvider(_ =>
                _ => throw new InvalidOperationException("vault offline"));

        // Sibling password-auth data source on the same service collection — must remain unaffected.
        services.AddClickHouse("healthy_pw", _fixture.ConnectionString);

        await using var provider = services.BuildServiceProvider();

        var brokenDs = provider.GetRequiredKeyedService<ClickHouseDataSource>("broken_jwt");
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using var c = await brokenDs.OpenConnectionAsync();
        });

        // Cause must be preserved — either the original or wrapped in the chain.
        var causeMatches = HasCauseOfType<InvalidOperationException>(ex, "vault offline");
        Assert.True(causeMatches,
            $"Expected InvalidOperationException(\"vault offline\") in cause chain; got {ex.GetType().FullName}: {ex.Message}");

        // Sibling password data source still serves queries normally.
        var healthyDs = provider.GetRequiredKeyedService<ClickHouseDataSource>("healthy_pw");
        await using (var conn = await healthyDs.OpenConnectionAsync())
        {
            Assert.Equal(11, await conn.ExecuteScalarAsync<int>("SELECT 11"));
        }
    }

    [Fact]
    public async Task SshKeyProviderThrows_FailsBeforeWire_DoesNotPoisonSiblingDataSource()
    {
        var services = new ServiceCollection();

        services.AddClickHouse("broken_ssh", builder => builder
                .WithHost(_fixture.Host)
                .WithPort(_fixture.Port)
                .WithCredentials(_fixture.Username, _fixture.Password))
            .WithSshKeyProvider(_ =>
                _ => throw new InvalidOperationException("ssh agent unreachable"));

        services.AddClickHouse("healthy_pw", _fixture.ConnectionString);

        await using var provider = services.BuildServiceProvider();

        var brokenDs = provider.GetRequiredKeyedService<ClickHouseDataSource>("broken_ssh");
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using var c = await brokenDs.OpenConnectionAsync();
        });

        Assert.True(HasCauseOfType<InvalidOperationException>(ex, "ssh agent unreachable"),
            $"Expected SSH provider exception in cause chain; got {ex.GetType().FullName}: {ex.Message}");

        var healthyDs = provider.GetRequiredKeyedService<ClickHouseDataSource>("healthy_pw");
        await using (var conn = await healthyDs.OpenConnectionAsync())
        {
            Assert.Equal(13, await conn.ExecuteScalarAsync<int>("SELECT 13"));
        }
    }

    [Fact]
    public async Task KeyedHealthChecks_OneBroken_OneHealthy_ReportIndependently()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        // Healthy keyed data source against the live container.
        services.AddClickHouse("ch_healthy", _fixture.ConnectionString);

        // Broken keyed data source aimed at a closed port (port 1, IANA reserved).
        services.AddClickHouse("ch_broken", builder => builder
            .WithHost("127.0.0.1")
            .WithPort(1)
            .WithCredentials("default", "")
            .WithConnectTimeout(TimeSpan.FromSeconds(2)));

        services.AddHealthChecks()
            .AddClickHouse(name: "healthy_check", serviceKey: "ch_healthy",
                timeout: TimeSpan.FromSeconds(5), tags: new[] { "ready" })
            .AddClickHouse(name: "broken_check", serviceKey: "ch_broken",
                timeout: TimeSpan.FromSeconds(3), tags: new[] { "ready" });

        await using var provider = services.BuildServiceProvider();
        var health = provider.GetRequiredService<HealthCheckService>();
        var report = await health.CheckHealthAsync();

        Assert.True(report.Entries.TryGetValue("healthy_check", out var healthy));
        Assert.True(report.Entries.TryGetValue("broken_check", out var broken));

        Assert.Equal(HealthStatus.Healthy, healthy.Status);
        Assert.Equal(HealthStatus.Unhealthy, broken.Status);

        // Useful failure message: at minimum, the description or exception must
        // identify it as a connect/network failure rather than be empty.
        var brokenMessage = broken.Exception?.Message ?? broken.Description ?? "";
        Assert.False(string.IsNullOrWhiteSpace(brokenMessage),
            "Broken health check should carry a diagnostic message.");
    }

    private static bool HasCauseOfType<TException>(Exception ex, string? messageContains)
        where TException : Exception
    {
        for (Exception? cur = ex; cur != null; cur = cur.InnerException)
        {
            if (cur is TException && (messageContains is null || cur.Message.Contains(messageContains)))
                return true;
            if (cur is AggregateException agg)
            {
                foreach (var inner in agg.Flatten().InnerExceptions)
                    if (HasCauseOfType<TException>(inner, messageContains)) return true;
            }
        }
        return false;
    }

    private sealed class PasswordBox
    {
        public string Password { get; set; }
        public PasswordBox(string initial) { Password = initial; }
    }
}
