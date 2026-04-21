using CH.Native.Ado;
using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

[Collection("ClickHouseRoles")]
[Trait("Category", "Integration")]
public class RolesTests
{
    private readonly ClickHouseRolesFixture _fixture;
    public RolesTests(ClickHouseRolesFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task NoRoleSet_FailsWithAccessDenied()
    {
        // Default role is NONE, so SELECT without activating anything must fail.
        await using var conn = new ClickHouseConnection(_fixture.RolesUserConnectionString);
        await conn.OpenAsync();

        var ex = await Assert.ThrowsAsync<ClickHouseServerException>(() =>
            conn.ExecuteScalarAsync<uint>($"SELECT count(*) FROM {ClickHouseRolesFixture.TableName}"));

        Assert.Equal(497, ex.ErrorCode); // ACCESS_DENIED
    }

    [Fact]
    public async Task ConnectionRoles_Granted_SelectSucceeds()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host).WithPort(_fixture.Port)
            .WithUsername(ClickHouseRolesFixture.RolesUser)
            .WithPassword(ClickHouseRolesFixture.RolesUserPassword)
            .WithRoles(ClickHouseRolesFixture.GrantedRole)
            .Build();

        await using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();

        var count = await conn.ExecuteScalarAsync<ulong>(
            $"SELECT count(*) FROM {ClickHouseRolesFixture.TableName}");
        Assert.Equal(2UL, count);
    }

    [Fact]
    public async Task ConnectionRoles_Ungranted_FailsWithAccessDenied()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host).WithPort(_fixture.Port)
            .WithUsername(ClickHouseRolesFixture.RolesUser)
            .WithPassword(ClickHouseRolesFixture.RolesUserPassword)
            .WithRoles(ClickHouseRolesFixture.UngrantedRole)   // granted to user but no SELECT on table
            .Build();

        await using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();

        var ex = await Assert.ThrowsAsync<ClickHouseServerException>(() =>
            conn.ExecuteScalarAsync<ulong>($"SELECT count(*) FROM {ClickHouseRolesFixture.TableName}"));

        Assert.Equal(497, ex.ErrorCode); // ACCESS_DENIED
    }

    [Fact]
    public async Task CommandRoles_OverrideConnectionDefault()
    {
        // Connection default = ungranted role → SELECT should fail. Command-level
        // override with granted role should succeed.
        var connStr = _fixture.RolesUserConnectionString + $";Roles={ClickHouseRolesFixture.UngrantedRole}";

        await using var conn = new Ado.ClickHouseDbConnection(connStr);
        await conn.OpenAsync();

        using var cmd = new ClickHouseDbCommand(
            $"SELECT count(*) FROM {ClickHouseRolesFixture.TableName}", conn)
        {
            Roles = { ClickHouseRolesFixture.GrantedRole }
        };

        var result = await cmd.ExecuteScalarAsync();
        Assert.Equal(2UL, Convert.ToUInt64(result));
    }

    [Fact]
    public async Task RoleStripping_ViaChangeRolesAsync_FailsWhenReset()
    {
        // Start with granted role (SELECT works), then strip via ChangeRolesAsync
        // (the command-level IList<string> Roles can't express strip — empty list
        // means "inherit" for parity with ClickHouse.Driver). Second SELECT must
        // fail ACCESS_DENIED.
        var connStr = _fixture.RolesUserConnectionString + $";Roles={ClickHouseRolesFixture.GrantedRole}";

        await using var conn = new ClickHouseConnection(connStr);
        await conn.OpenAsync();

        var ok = await conn.ExecuteScalarAsync<ulong>(
            $"SELECT count(*) FROM {ClickHouseRolesFixture.TableName}");
        Assert.Equal(2UL, ok);

        await conn.ChangeRolesAsync(Array.Empty<string>());

        var ex = await Assert.ThrowsAsync<ClickHouseServerException>(() =>
            conn.ExecuteScalarAsync<ulong>($"SELECT count(*) FROM {ClickHouseRolesFixture.TableName}"));
        Assert.Equal(497, ex.ErrorCode);
    }

    [Fact]
    public async Task RoleCache_RepeatedSameRoles_IssuesSetRoleOnce()
    {
        // Proves _currentServerRoles cache skips SET ROLE re-issue for matching
        // consecutive queries on the same connection. Regression guard.
        // Differential counting: the container is shared across tests in the
        // ClickHouseRoles collection, so absolute counts are contaminated by
        // other tests' SET ROLE traffic.
        var connStr = _fixture.RolesUserConnectionString + $";Roles={ClickHouseRolesFixture.GrantedRole}";

        await using var conn = new ClickHouseConnection(connStr);
        await conn.OpenAsync();

        // Warm the session's SET ROLE once (the very first query triggers it), then
        // snapshot the query_log baseline.
        await conn.ExecuteScalarAsync<ulong>($"SELECT count(*) FROM {ClickHouseRolesFixture.TableName}");
        await conn.ExecuteNonQueryAsync("SYSTEM FLUSH LOGS");
        var baseline = await ReadSetRoleCount(conn);

        // Five more queries with the same roles — none should issue SET ROLE.
        for (var i = 0; i < 5; i++)
            await conn.ExecuteScalarAsync<ulong>($"SELECT count(*) FROM {ClickHouseRolesFixture.TableName}");

        await conn.ExecuteNonQueryAsync("SYSTEM FLUSH LOGS");
        var after = await ReadSetRoleCount(conn);

        Assert.Equal(baseline, after);
    }

    [Fact]
    public async Task Reconnect_ReIssuesSetRole()
    {
        // Closing and re-opening the same connection must reset _currentServerRoles
        // so the next query re-issues SET ROLE on the fresh session.
        var connStr = _fixture.RolesUserConnectionString + $";Roles={ClickHouseRolesFixture.GrantedRole}";

        // Baseline: one SET ROLE from a warmup session.
        await using (var warm = new ClickHouseConnection(connStr))
        {
            await warm.OpenAsync();
            await warm.ExecuteScalarAsync<ulong>($"SELECT count(*) FROM {ClickHouseRolesFixture.TableName}");
            await warm.ExecuteNonQueryAsync("SYSTEM FLUSH LOGS");
            var baseline = await ReadSetRoleCount(warm);

            // Close warm; open a fresh session; run one query. Expect exactly +1.
            await warm.CloseAsync();

            await using var fresh = new ClickHouseConnection(connStr);
            await fresh.OpenAsync();
            await fresh.ExecuteScalarAsync<ulong>($"SELECT count(*) FROM {ClickHouseRolesFixture.TableName}");
            await fresh.ExecuteNonQueryAsync("SYSTEM FLUSH LOGS");
            var after = await ReadSetRoleCount(fresh);

            Assert.Equal(baseline + 1UL, after);
        }
    }

    private static Task<ulong> ReadSetRoleCount(ClickHouseConnection conn)
        => conn.ExecuteScalarAsync<ulong>(
            "SELECT count() FROM system.query_log " +
            "WHERE type = 'QueryStart' AND startsWith(query, 'SET ROLE')");

    [Fact]
    public async Task ChangeRolesAsync_AppliesRolesToSubsequentQueries()
    {
        // No connection-level roles; pin via ChangeRolesAsync, then run a
        // privileged SELECT.
        await using var conn = new ClickHouseConnection(_fixture.RolesUserConnectionString);
        await conn.OpenAsync();

        // Without roles → SELECT fails (default role is NONE).
        var denied = await Assert.ThrowsAsync<ClickHouseServerException>(() =>
            conn.ExecuteScalarAsync<ulong>($"SELECT count(*) FROM {ClickHouseRolesFixture.TableName}"));
        Assert.Equal(497, denied.ErrorCode);

        // Pin roles → subsequent SELECT succeeds.
        await conn.ChangeRolesAsync(new[] { ClickHouseRolesFixture.GrantedRole });
        var count = await conn.ExecuteScalarAsync<ulong>(
            $"SELECT count(*) FROM {ClickHouseRolesFixture.TableName}");
        Assert.Equal(2UL, count);

        // Strip via empty list → SELECT fails again.
        await conn.ChangeRolesAsync(Array.Empty<string>());
        var deniedAgain = await Assert.ThrowsAsync<ClickHouseServerException>(() =>
            conn.ExecuteScalarAsync<ulong>($"SELECT count(*) FROM {ClickHouseRolesFixture.TableName}"));
        Assert.Equal(497, deniedAgain.ErrorCode);
    }

    [Fact]
    public async Task RoleWithSpecialCharacters_IsSafelyQuoted()
    {
        // Create a role whose name contains a backtick. If we didn't quote it
        // correctly, SET ROLE would either fail to parse or (worse) get
        // interpreted as extra SQL — the command below would succeed by accident.
        await using var admin = new ClickHouseConnection(
            $"Host={_fixture.Host};Port={_fixture.Port};Username=default;Password=adminpass");
        await admin.OpenAsync();

        var trickyRole = "role`with`ticks";
        try
        {
            await admin.ExecuteNonQueryAsync($"CREATE ROLE OR REPLACE `{trickyRole.Replace("`", "``")}`");
            await admin.ExecuteNonQueryAsync(
                $"GRANT SELECT ON {ClickHouseRolesFixture.TableName} TO `{trickyRole.Replace("`", "``")}`");
            await admin.ExecuteNonQueryAsync(
                $"GRANT `{trickyRole.Replace("`", "``")}` TO {ClickHouseRolesFixture.RolesUser}");

            var settings = ClickHouseConnectionSettings.CreateBuilder()
                .WithHost(_fixture.Host).WithPort(_fixture.Port)
                .WithUsername(ClickHouseRolesFixture.RolesUser)
                .WithPassword(ClickHouseRolesFixture.RolesUserPassword)
                .WithRoles(trickyRole)
                .Build();

            await using var conn = new ClickHouseConnection(settings);
            await conn.OpenAsync();

            var count = await conn.ExecuteScalarAsync<ulong>(
                $"SELECT count(*) FROM {ClickHouseRolesFixture.TableName}");
            Assert.Equal(2UL, count);
        }
        finally
        {
            await admin.ExecuteNonQueryAsync($"DROP ROLE IF EXISTS `{trickyRole.Replace("`", "``")}`");
        }
    }
}
