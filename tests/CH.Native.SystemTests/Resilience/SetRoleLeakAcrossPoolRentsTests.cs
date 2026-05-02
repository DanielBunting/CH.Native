using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Resilience;

/// <summary>
/// Probes for role-state session leaks across pool rents. ClickHouse roles
/// can be assumed via <c>SET ROLE role_name</c>; the role persists for the
/// session. If a caller assumes an admin role, runs a privileged query,
/// and returns the connection without resetting, the next renter inherits
/// the role silently.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Resilience)]
public class SetRoleLeakAcrossPoolRentsTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public SetRoleLeakAcrossPoolRentsTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task SetRole_PersistsToNextRent_WhenSamePhysicalConnectionReused()
    {
        // Setup: create a role, grant it to the test user. If grant fails
        // (e.g., user has insufficient privileges in the test container),
        // skip the body since SET ROLE will fail unrelated to the leak we
        // want to probe.
        var roleName = $"r_{Guid.NewGuid():N}";
        await using (var setup = new ClickHouseConnection(_fx.BuildSettings()))
        {
            await setup.OpenAsync();
            try { await setup.ExecuteNonQueryAsync($"CREATE ROLE {roleName}"); }
            catch (ClickHouseServerException ex)
            {
                _output.WriteLine($"CREATE ROLE failed (likely insufficient access): {ex.Message}");
                return;
            }
            try { await setup.ExecuteNonQueryAsync($"GRANT {roleName} TO {_fx.Username}"); }
            catch (ClickHouseServerException ex)
            {
                _output.WriteLine($"GRANT failed (insufficient privileges in test container): {ex.Message}");
                try { await setup.ExecuteNonQueryAsync($"DROP ROLE IF EXISTS {roleName}"); } catch { }
                return;
            }
        }

        try
        {
            await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
            {
                Settings = _fx.BuildSettings(),
                MaxPoolSize = 1,
            });

            // Rent 1: assume the role.
            await using (var conn = await ds.OpenConnectionAsync())
            {
                await conn.ExecuteNonQueryAsync($"SET ROLE {roleName}");
                var current = await conn.ExecuteScalarAsync<string>(
                    "SELECT arrayStringConcat(currentRoles(), ',')");
                _output.WriteLine($"Rent 1 currentRoles: {current}");
                Assert.Contains(roleName, current);
            }

            // Rent 2: same physical connection. Does the role persist?
            await using (var conn = await ds.OpenConnectionAsync())
            {
                var current = await conn.ExecuteScalarAsync<string>(
                    "SELECT arrayStringConcat(currentRoles(), ',')");
                _output.WriteLine($"Rent 2 currentRoles: {current}");

                // OBSERVE today's behaviour. If the role persists, this is
                // a session-state leak. If it's reset (default behaviour),
                // current would be empty or "default".
                _output.WriteLine($"Role-leak observed across rents: {current?.Contains(roleName) ?? false}");
            }
        }
        finally
        {
            await using var cleanup = new ClickHouseConnection(_fx.BuildSettings());
            await cleanup.OpenAsync();
            try { await cleanup.ExecuteNonQueryAsync($"DROP ROLE IF EXISTS {roleName}"); } catch { }
        }
    }
}
