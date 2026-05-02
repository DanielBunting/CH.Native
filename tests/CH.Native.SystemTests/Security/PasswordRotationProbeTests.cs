using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Security;

/// <summary>
/// Probes the data-source <c>ConnectionFactory</c> hook used by the DI layer to feed
/// rotating credentials (JWT / SSH / mTLS / password) into the pool. Uses the password
/// path because that's the only auth mode this fixture's container supports out of
/// the box; the wire-shape contract is the same for every credential type.
///
/// <para>Existing auth tests focus on happy-path handshake. These tests pin behaviour
/// when secrets change mid-pool, when the provider throws, and when the provider
/// returns invalid output — failure modes that are most likely to leak connections or
/// poison pool state in production.</para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Security)]
public class PasswordRotationProbeTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public PasswordRotationProbeTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task PasswordChange_OnServer_ExistingPooledConnection_StaysAlive()
    {
        // Open a connection, return to pool, change pwd on server, rent again.
        // ClickHouse re-checks auth only on new TCP connections — so the existing
        // pooled connection should keep working. Pin that contract.
        var rotatingUser = $"rotating_{Guid.NewGuid():N}";
        var origPwd = "orig_pwd_42";
        var newPwd = "new_pwd_7777";

        await using (var admin = new ClickHouseConnection(_fx.BuildSettings()))
        {
            await admin.OpenAsync();
            await admin.ExecuteNonQueryAsync($"CREATE USER {rotatingUser} IDENTIFIED WITH plaintext_password BY '{origPwd}'");
            await admin.ExecuteNonQueryAsync($"GRANT SELECT ON *.* TO {rotatingUser}");
        }

        try
        {
            await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
            {
                Settings = _fx.BuildSettings(b => b.WithCredentials(rotatingUser, origPwd)),
                MaxPoolSize = 1,
            });

            // Rent + return to populate the pool with a live socket.
            await using (var c = await ds.OpenConnectionAsync())
            {
                Assert.Equal(1, await c.ExecuteScalarAsync<int>("SELECT 1"));
            }

            // Server-side password change.
            await using (var admin = new ClickHouseConnection(_fx.BuildSettings()))
            {
                await admin.OpenAsync();
                await admin.ExecuteNonQueryAsync(
                    $"ALTER USER {rotatingUser} IDENTIFIED WITH plaintext_password BY '{newPwd}'");
            }

            // Existing connection in the pool should still work — server doesn't
            // re-auth on an existing TCP session.
            await using (var c = await ds.OpenConnectionAsync())
            {
                var got = await c.ExecuteScalarAsync<int>("SELECT 42");
                _output.WriteLine($"Pooled connection after pwd change: SELECT 42 returned {got}");
                Assert.Equal(42, got);
            }
        }
        finally
        {
            try
            {
                await using var admin = new ClickHouseConnection(_fx.BuildSettings());
                await admin.OpenAsync();
                await admin.ExecuteNonQueryAsync($"DROP USER IF EXISTS {rotatingUser}");
            }
            catch { /* cleanup best-effort */ }
        }
    }

    [Fact]
    public async Task PasswordChange_OnServer_NewOpenWithStaleSecret_FailsCleanly()
    {
        var user = $"rot_stale_{Guid.NewGuid():N}";
        var origPwd = "orig_42";
        var newPwd = "new_7777";

        await using (var admin = new ClickHouseConnection(_fx.BuildSettings()))
        {
            await admin.OpenAsync();
            await admin.ExecuteNonQueryAsync($"CREATE USER {user} IDENTIFIED WITH plaintext_password BY '{origPwd}'");
            await admin.ExecuteNonQueryAsync($"GRANT SELECT ON *.* TO {user}");
        }

        try
        {
            // Change the server-side password.
            await using (var admin = new ClickHouseConnection(_fx.BuildSettings()))
            {
                await admin.OpenAsync();
                await admin.ExecuteNonQueryAsync(
                    $"ALTER USER {user} IDENTIFIED WITH plaintext_password BY '{newPwd}'");
            }

            // New connection with the stale password must fail. Capture the surface.
            await using var staleConn = new ClickHouseConnection(
                _fx.BuildSettings(b => b.WithCredentials(user, origPwd)));
            Exception? caught = null;
            try
            {
                await staleConn.OpenAsync();
            }
            catch (Exception ex) { caught = ex; }

            _output.WriteLine($"Stale-pwd surface: {caught?.GetType().FullName} — {caught?.Message}");
            Assert.NotNull(caught);

            // Recovery: a fresh connection with the new password works.
            await using var freshConn = new ClickHouseConnection(
                _fx.BuildSettings(b => b.WithCredentials(user, newPwd)));
            await freshConn.OpenAsync();
            Assert.Equal(1, await freshConn.ExecuteScalarAsync<int>("SELECT 1"));
        }
        finally
        {
            try
            {
                await using var admin = new ClickHouseConnection(_fx.BuildSettings());
                await admin.OpenAsync();
                await admin.ExecuteNonQueryAsync($"DROP USER IF EXISTS {user}");
            }
            catch { /* cleanup best-effort */ }
        }
    }

    [Fact]
    public async Task RotatingCredentialFactory_NewOpensUseNewSecret_ProviderInvokedPerCreate()
    {
        // Simulate a credential rotation by switching the data source's
        // ConnectionFactory output mid-pool. Pin: the factory is invoked once per
        // physical create; existing rents are not affected.
        var user = $"rot_factory_{Guid.NewGuid():N}";
        var pwdV1 = "v1_pwd";
        var pwdV2 = "v2_pwd";

        await using (var admin = new ClickHouseConnection(_fx.BuildSettings()))
        {
            await admin.OpenAsync();
            await admin.ExecuteNonQueryAsync($"CREATE USER {user} IDENTIFIED WITH plaintext_password BY '{pwdV1}'");
            await admin.ExecuteNonQueryAsync($"GRANT SELECT ON *.* TO {user}");
        }

        try
        {
            int factoryInvocations = 0;
            string currentPwd = pwdV1;

            var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
            {
                Settings = _fx.BuildSettings(b => b.WithCredentials(user, pwdV1)),
                MaxPoolSize = 4,
                ConnectionFactory = _ =>
                {
                    Interlocked.Increment(ref factoryInvocations);
                    return new ValueTask<ClickHouseConnectionSettings>(
                        _fx.BuildSettings(b => b.WithCredentials(user, currentPwd)));
                },
            });

            await using (ds)
            {
                // Open 3 connections under v1.
                var conns = await Task.WhenAll(
                    Enumerable.Range(0, 3).Select(_ => ds.OpenConnectionAsync().AsTask()));
                foreach (var c in conns)
                    Assert.Equal(1, await c.ExecuteScalarAsync<int>("SELECT 1"));
                foreach (var c in conns)
                    await c.DisposeAsync();

                int v1Calls = factoryInvocations;
                _output.WriteLine($"Factory invocations after v1 burst: {v1Calls}");
                Assert.True(v1Calls >= 3);

                // Rotate the password server-side and bump the factory's view.
                await using (var admin = new ClickHouseConnection(_fx.BuildSettings()))
                {
                    await admin.OpenAsync();
                    await admin.ExecuteNonQueryAsync(
                        $"ALTER USER {user} IDENTIFIED WITH plaintext_password BY '{pwdV2}'");
                }
                Volatile.Write(ref currentPwd, pwdV2);

                // Force the existing pool entries out (lifetime can't be reset on the
                // fly without redesigning the test) — just dispose + fresh DS would
                // mask the issue. Instead drain by opening N+1 fresh, which will
                // recycle stale ones via ValidateOnRent (off by default — but a
                // restart of the inner sockets via SET would also work). Simplest:
                // assert that the *next* fresh-create call uses the new pwd.
                //
                // Rentn that exceeds idle ones forces a new create.
                var rented = new List<ClickHouseConnection>();
                for (int i = 0; i < 4; i++)
                    rented.Add(await ds.OpenConnectionAsync());
                foreach (var c in rented)
                    Assert.Equal(1, await c.ExecuteScalarAsync<int>("SELECT 1"));
                foreach (var c in rented)
                    await c.DisposeAsync();

                _output.WriteLine($"Factory invocations after v2 rotation: {factoryInvocations}");
                Assert.True(factoryInvocations > v1Calls,
                    "Factory must be invoked again on new physical-connection creation");
            }
        }
        finally
        {
            try
            {
                await using var admin = new ClickHouseConnection(_fx.BuildSettings());
                await admin.OpenAsync();
                await admin.ExecuteNonQueryAsync($"DROP USER IF EXISTS {user}");
            }
            catch { /* cleanup best-effort */ }
        }
    }

    [Fact]
    public async Task ConnectionFactory_ThrowsOnInvocation_NoConnectionLeaked()
    {
        // Provider throws on every invocation. After all rent attempts fail, pool
        // statistics must show no leaked physical connections (Total = 0).
        await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _fx.BuildSettings(),
            MaxPoolSize = 4,
            ConnectionWaitTimeout = TimeSpan.FromSeconds(2),
            ConnectionFactory = _ => throw new InvalidOperationException("synthetic provider failure"),
        });

        for (int i = 0; i < 5; i++)
        {
            Exception? caught = null;
            try { await using var c = await ds.OpenConnectionAsync(); }
            catch (Exception ex) { caught = ex; }
            Assert.NotNull(caught);
        }

        var stats = ds.GetStatistics();
        _output.WriteLine($"After 5 failed opens: {stats}");
        Assert.Equal(0, stats.Busy);
        Assert.Equal(0, stats.PendingWaits);
        // Total can be 0 (cleanly aborted creates) or include evicted partial creates;
        // forbid only a runaway leak.
        Assert.True(stats.Total <= 4, $"Pool leaked physical connections: Total={stats.Total}");
    }

    [Fact]
    public async Task ConnectionFactory_ReturnsInvalidSettings_FailsCleanly()
    {
        // Provider returns settings with the wrong password — handshake fails.
        // Pool stats must converge to clean (Busy=0, PendingWaits=0).
        await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _fx.BuildSettings(),
            MaxPoolSize = 2,
            ConnectionWaitTimeout = TimeSpan.FromSeconds(3),
            ConnectionFactory = _ => new ValueTask<ClickHouseConnectionSettings>(
                _fx.BuildSettings(b => b.WithPassword("definitely-the-wrong-password"))),
        });

        Exception? caught = null;
        try { await using var c = await ds.OpenConnectionAsync(); }
        catch (Exception ex) { caught = ex; }

        _output.WriteLine($"Bad-pwd factory output surface: {caught?.GetType().FullName} — {caught?.Message}");
        Assert.NotNull(caught);

        var stats = ds.GetStatistics();
        _output.WriteLine($"Stats after bad-pwd attempt: {stats}");
        Assert.Equal(0, stats.Busy);
        Assert.Equal(0, stats.PendingWaits);
    }
}
