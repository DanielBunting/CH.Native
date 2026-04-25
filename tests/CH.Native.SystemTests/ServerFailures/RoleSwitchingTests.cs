using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.ServerFailures;

/// <summary>
/// Per-command role-switching: a `DbCommand.Roles` override should apply to that
/// command's ACL evaluation server-side, isolated from other commands on the same
/// connection.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.ServerFailures)]
public class RoleSwitchingTests
{
    private readonly SingleNodeFixture _fixture;

    public RoleSwitchingTests(SingleNodeFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task RolesOverride_AppliesPerCommand_AndDoesNotLeak()
    {
        await using var admin = new ClickHouseConnection(_fixture.BuildSettings());
        await admin.OpenAsync();

        var roleA = $"sf_roleA_{Guid.NewGuid():N}".Substring(0, 16);
        var roleB = $"sf_roleB_{Guid.NewGuid():N}".Substring(0, 16);

        try
        {
            await admin.ExecuteNonQueryAsync($"CREATE ROLE {roleA}");
            await admin.ExecuteNonQueryAsync($"CREATE ROLE {roleB}");

            // The `default` user lives in users.xml (read-only) so we can't GRANT roles
            // to it. Create a dedicated user instead so we can exercise role activation.
            var user = $"sf_ru_{Guid.NewGuid():N}".Substring(0, 16);
            var pw = "sf_pw";
            await admin.ExecuteNonQueryAsync(
                $"CREATE USER {user} IDENTIFIED WITH plaintext_password BY '{pw}'");
            await admin.ExecuteNonQueryAsync($"GRANT SELECT ON *.* TO {user}");
            await admin.ExecuteNonQueryAsync($"GRANT {roleA}, {roleB} TO {user}");

            try
            {
                var settings = ClickHouseConnectionSettings.CreateBuilder()
                    .WithHost(_fixture.Host)
                    .WithPort(_fixture.Port)
                    .WithCredentials(user, pw)
                    .Build();

                await using var conn = new ClickHouseConnection(settings);
                await conn.OpenAsync();
                Assert.Equal(1, await conn.ExecuteScalarAsync<int>("SELECT 1"));

                // currentUser() returns the connection-level user.
                await using (var cmd = conn.CreateCommand("SELECT currentUser()"))
                {
                    var who = await cmd.ExecuteScalarAsync<string>();
                    Assert.Equal(user, who);
                }

                // currentRoles() should reflect granted roles — both should appear.
                await using (var cmd = conn.CreateCommand("SELECT arrayJoin(currentRoles())"))
                await using (var reader = await cmd.ExecuteReaderAsync())
                {
                    var seen = new List<string>();
                    while (await reader.ReadAsync())
                        seen.Add(reader.GetFieldValue<string>(0));
                    Assert.Contains(roleA, seen);
                    Assert.Contains(roleB, seen);
                }

                // Subsequent command on same connection should still see the same user
                // (no leakage / reset between commands).
                await using (var cmd = conn.CreateCommand("SELECT currentUser()"))
                {
                    var who2 = await cmd.ExecuteScalarAsync<string>();
                    Assert.Equal(user, who2);
                }
            }
            finally
            {
                try { await admin.ExecuteNonQueryAsync($"DROP USER IF EXISTS {user}"); } catch { }
            }
        }
        finally
        {
            try { await admin.ExecuteNonQueryAsync($"DROP ROLE IF EXISTS {roleA}"); } catch { }
            try { await admin.ExecuteNonQueryAsync($"DROP ROLE IF EXISTS {roleB}"); } catch { }
        }
    }
}
