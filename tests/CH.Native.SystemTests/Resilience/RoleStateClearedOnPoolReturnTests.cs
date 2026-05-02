using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Resilience;

/// <summary>
/// Pre-fix: any connection that ever issued <c>SET ROLE</c> latched
/// <c>_rolesExplicitlySet=true</c> on <see cref="ClickHouseConnection"/>; that
/// flag was never cleared, and <c>CanBePooled</c> gated on it, so every
/// role-using connection was silently discarded on pool return.
/// Result: pool churn that looked like a connection leak under role-based
/// multi-tenancy.
///
/// Fix: <c>ResetSessionStateAsync</c> now restores defaults via
/// <c>SET ROLE DEFAULT</c> and clears the latch so the same physical
/// connection can be re-rented.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Resilience)]
public class RoleStateClearedOnPoolReturnTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public RoleStateClearedOnPoolReturnTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task SetRole_ThenReturnToPool_ConnectionIsRetainedAndDefaultsRestored()
    {
        // Use a fresh SQL-managed user — GRANT to the container's default
        // user fails with code 495 because users.xml is read-only.
        var suffix = Guid.NewGuid().ToString("N").Substring(0, 12);
        var roleName = $"r_{suffix}";
        var userName = $"u_{suffix}";
        var userPassword = "p_" + suffix;

        await using (var setup = new ClickHouseConnection(_fx.BuildSettings()))
        {
            await setup.OpenAsync();
            try
            {
                await setup.ExecuteNonQueryAsync($"CREATE ROLE {roleName}");
                await setup.ExecuteNonQueryAsync(
                    $"CREATE USER {userName} IDENTIFIED WITH plaintext_password BY '{userPassword}'");
                await setup.ExecuteNonQueryAsync($"GRANT {roleName} TO {userName}");
                // ClickHouse makes granted roles default; pin the default to NONE so
                // SET ROLE DEFAULT actually clears the role rather than reactivating it.
                await setup.ExecuteNonQueryAsync($"SET DEFAULT ROLE NONE TO {userName}");
            }
            catch (ClickHouseServerException ex)
            {
                _output.WriteLine($"setup failed (insufficient access): {ex.Message}");
                try { await setup.ExecuteNonQueryAsync($"DROP USER IF EXISTS {userName}"); } catch { }
                try { await setup.ExecuteNonQueryAsync($"DROP ROLE IF EXISTS {roleName}"); } catch { }
                return;
            }
        }

        var poolSettings = _fx.BuildSettings(b => b.WithCredentials(userName, userPassword));

        try
        {
            await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
            {
                Settings = poolSettings,
                MaxPoolSize = 1,
            });

            // Rent 1: assume the role.
            await using (var conn = await ds.OpenConnectionAsync())
            {
                await conn.ExecuteNonQueryAsync($"SET ROLE {roleName}");
                var current = await conn.ExecuteScalarAsync<string>(
                    "SELECT arrayStringConcat(currentRoles(), ',')");
                Assert.Contains(roleName, current);
            }

            // After return, the connection must remain in the pool — the
            // pre-fix bug discarded it because _rolesExplicitlySet stayed true.
            var stats = ds.GetStatistics();
            _output.WriteLine($"Pool stats after rent 1: Total={stats.Total} Idle={stats.Idle}");
            Assert.Equal(1, stats.Total);
            Assert.Equal(1, stats.Idle);

            // Rent 2: same physical connection. Defaults must be active —
            // the previous role must NOT leak into this rent.
            await using (var conn = await ds.OpenConnectionAsync())
            {
                var current = await conn.ExecuteScalarAsync<string>(
                    "SELECT arrayStringConcat(currentRoles(), ',')");
                _output.WriteLine($"Rent 2 currentRoles: '{current}'");
                Assert.DoesNotContain(roleName, current ?? string.Empty);
            }
        }
        finally
        {
            await using var cleanup = new ClickHouseConnection(_fx.BuildSettings());
            await cleanup.OpenAsync();
            try { await cleanup.ExecuteNonQueryAsync($"DROP USER IF EXISTS {userName}"); } catch { }
            try { await cleanup.ExecuteNonQueryAsync($"DROP ROLE IF EXISTS {roleName}"); } catch { }
        }
    }
}
