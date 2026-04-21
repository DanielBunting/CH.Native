using System.Text;
using CH.Native.Connection;
using DotNet.Testcontainers.Builders;
using Testcontainers.ClickHouse;
using Xunit;

namespace CH.Native.Tests.Fixtures;

/// <summary>
/// Shared fixture with a pre-seeded user (<c>roles_user</c>) granted two roles
/// (<c>role_grant</c> — SELECT on a test table; <c>role_deny</c> — nothing).
/// The user's default role is <c>NONE</c>, so naïve queries fail with ACCESS_DENIED;
/// roles must be activated explicitly to succeed.
/// </summary>
public sealed class ClickHouseRolesFixture : IAsyncLifetime
{
    public const string TableName = "roles_test_table";
    public const string RolesUser = "roles_user";
    public const string RolesUserPassword = "rolespass";
    public const string GrantedRole = "role_grant";
    public const string UngrantedRole = "role_deny";

    // Admin user bootstrap: default user needs access_management=1 to run
    // CREATE USER / CREATE ROLE / GRANT. Testcontainers.ClickHouse's WithUsername/
    // WithPassword path generates its own default-user.xml without this flag, so
    // we skip that and inject a self-contained overlay instead.
    //
    // The file name ('z_…') matters: ClickHouse merges users.d alphabetically, so
    // our overlay must sort AFTER the entrypoint-generated default-user.xml to win.
    private static readonly byte[] AccessMgmtOverlay = Encoding.UTF8.GetBytes(@"
<clickhouse>
  <users>
    <default>
      <password>adminpass</password>
      <networks><ip>::/0</ip></networks>
      <profile>default</profile>
      <quota>default</quota>
      <access_management>1</access_management>
      <named_collection_control>1</named_collection_control>
    </default>
  </users>
</clickhouse>");

    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:25.10")
        // Note: no WithUsername/WithPassword here — the overlay below owns the
        // default user's config entirely (access_management + password).
        .WithResourceMapping(AccessMgmtOverlay,
            "/etc/clickhouse-server/users.d/z_access_management.xml")
        .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(9000))
        .Build();

    public string Host => _container.Hostname;
    public int Port => _container.GetMappedPublicPort(9000);

    public string RolesUserConnectionString =>
        $"Host={Host};Port={Port};Username={RolesUser};Password={RolesUserPassword}";

    public async Task InitializeAsync()
    {
        await _container.StartAsync();

        // Wait until the native protocol actually accepts handshakes (image reports
        // port-open before listener is ready on first boot).
        await using var admin = new ClickHouseConnection(
            $"Host={Host};Port={Port};Username=default;Password=adminpass");
        for (var attempt = 1; attempt <= 20; attempt++)
        {
            try { await admin.OpenAsync(); break; }
            catch
            {
                if (attempt == 20) throw;
                await Task.Delay(500);
            }
        }

        // Provision the user, roles, table, grants, default-role-none.
        // Each statement issued separately because the native protocol rejects
        // multi-statement queries.
        async Task Exec(string sql) => await admin.ExecuteNonQueryAsync(sql);

        await Exec($"CREATE USER {RolesUser} IDENTIFIED WITH plaintext_password BY '{RolesUserPassword}'");
        await Exec($"CREATE ROLE {GrantedRole}");
        await Exec($"CREATE ROLE {UngrantedRole}");

        await Exec($"CREATE TABLE {TableName} (id UInt32, name String) ENGINE = MergeTree ORDER BY id");
        await Exec($"INSERT INTO {TableName} VALUES (1, 'alice'), (2, 'bob')");

        await Exec($"GRANT SELECT ON {TableName} TO {GrantedRole}");
        // Needed by the role-cache integration tests that read from system.query_log.
        await Exec($"GRANT SELECT ON system.query_log TO {GrantedRole}");
        await Exec($"GRANT SYSTEM FLUSH LOGS ON *.* TO {GrantedRole}");
        // role_deny intentionally has no grants.

        await Exec($"GRANT {GrantedRole}, {UngrantedRole} TO {RolesUser}");
        // Critical: neither role is default, so fresh connection has zero privileges.
        await Exec($"SET DEFAULT ROLE NONE TO {RolesUser}");
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }
}

[CollectionDefinition("ClickHouseRoles")]
public class ClickHouseRolesCollection : ICollectionFixture<ClickHouseRolesFixture>
{
}
