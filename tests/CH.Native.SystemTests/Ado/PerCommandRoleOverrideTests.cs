using CH.Native.Ado;
using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Ado;

/// <summary>
/// Pins the per-command role override contract. Surface-area §7.1 #19
/// documents three scopes (connection default, per-command,
/// per-bulk-insert). Existing pool-leak tests cover state isolation;
/// this covers the override semantics for a single command.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class PerCommandRoleOverrideTests : IAsyncLifetime
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;
    private const string ReadOnlyRole = "po_test_readonly";
    private const string WriterRole = "po_test_writer";
    private readonly string _user = $"po_test_user_{Guid.NewGuid():N}";

    public PerCommandRoleOverrideTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    public async Task InitializeAsync()
    {
        await using var root = new ClickHouseConnection(_fx.BuildSettings());
        await root.OpenAsync();
        await root.ExecuteNonQueryAsync($"DROP ROLE IF EXISTS {ReadOnlyRole}, {WriterRole}");
        await root.ExecuteNonQueryAsync($"DROP USER IF EXISTS {_user}");
        await root.ExecuteNonQueryAsync($"CREATE ROLE {ReadOnlyRole}");
        await root.ExecuteNonQueryAsync($"CREATE ROLE {WriterRole}");
        await root.ExecuteNonQueryAsync($"GRANT SELECT ON *.* TO {ReadOnlyRole}");
        await root.ExecuteNonQueryAsync($"GRANT SELECT, INSERT ON *.* TO {WriterRole}");
        await root.ExecuteNonQueryAsync($"CREATE USER {_user} IDENTIFIED WITH plaintext_password BY 'pwd'");
        await root.ExecuteNonQueryAsync($"GRANT {ReadOnlyRole}, {WriterRole} TO {_user}");
        // Default role: readonly only.
        await root.ExecuteNonQueryAsync(
            $"ALTER USER {_user} DEFAULT ROLE {ReadOnlyRole}");
    }

    public async Task DisposeAsync()
    {
        await using var root = new ClickHouseConnection(_fx.BuildSettings());
        await root.OpenAsync();
        await root.ExecuteNonQueryAsync($"DROP USER IF EXISTS {_user}");
        await root.ExecuteNonQueryAsync($"DROP ROLE IF EXISTS {ReadOnlyRole}, {WriterRole}");
    }

    private string TestUserConnectionString =>
        $"Host={_fx.Host};Port={_fx.Port};Username={_user};Password=pwd";

    [Fact]
    public async Task PerCommandRoleOverride_ActivatesOnlyForThatCommand()
    {
        await using var conn = new ClickHouseDbConnection(TestUserConnectionString);
        await conn.OpenAsync();

        // Default role (readonly) — the visible roles via currentRoles()
        // should NOT include the writer role.
        using (var defaultCmd = (ClickHouseDbCommand)conn.CreateCommand())
        {
            defaultCmd.CommandText = "SELECT arrayStringConcat(currentRoles(), ',') AS roles";
            var defaultRoles = (string)(await defaultCmd.ExecuteScalarAsync())!;
            _output.WriteLine($"Default roles: {defaultRoles}");
            Assert.Contains(ReadOnlyRole, defaultRoles);
            Assert.DoesNotContain(WriterRole, defaultRoles);
        }

        // Override on a single command.
        using (var overrideCmd = (ClickHouseDbCommand)conn.CreateCommand())
        {
            overrideCmd.CommandText = "SELECT arrayStringConcat(currentRoles(), ',') AS roles";
            overrideCmd.Roles.Add(WriterRole);
            var overriddenRoles = (string)(await overrideCmd.ExecuteScalarAsync())!;
            _output.WriteLine($"Per-command override roles: {overriddenRoles}");
            Assert.Contains(WriterRole, overriddenRoles);
        }

        // Next command on same connection — back to default role.
        using (var afterCmd = (ClickHouseDbCommand)conn.CreateCommand())
        {
            afterCmd.CommandText = "SELECT arrayStringConcat(currentRoles(), ',') AS roles";
            var laterRoles = (string)(await afterCmd.ExecuteScalarAsync())!;
            _output.WriteLine($"Default roles (post-override): {laterRoles}");
            Assert.Contains(ReadOnlyRole, laterRoles);
            Assert.DoesNotContain(WriterRole, laterRoles);
        }
    }

    [Fact]
    public async Task PerCommandRoleOverride_NonExistentRole_SurfacesAccessDenied_DoesNotPoisonConnection()
    {
        await using var conn = new ClickHouseDbConnection(TestUserConnectionString);
        await conn.OpenAsync();

        // Override with a role the user does not have.
        using (var bogusCmd = (ClickHouseDbCommand)conn.CreateCommand())
        {
            bogusCmd.CommandText = "SELECT 1";
            bogusCmd.Roles.Add($"never_granted_{Guid.NewGuid():N}");

            var ex = await Assert.ThrowsAnyAsync<Exception>(() => bogusCmd.ExecuteScalarAsync());
            _output.WriteLine($"Bogus role surfaced: {ex.GetType().Name}: {ex.Message}");
        }

        // Subsequent command on the same connection succeeds — the failed
        // role override didn't break the connection.
        using (var ok = (ClickHouseDbCommand)conn.CreateCommand())
        {
            ok.CommandText = "SELECT 42";
            var result = await ok.ExecuteScalarAsync();
            Assert.Equal(42, Convert.ToInt32(result));
        }
    }
}
