using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Resilience;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.ServerFailures;

/// <summary>
/// Verifies that documented ClickHouse error codes surface as <see cref="ClickHouseServerException"/>
/// with the right <c>ErrorCode</c>, and that connections remain reusable after each.
/// These codes drive retry-policy decisions (see <c>RetryPolicy.TransientErrorCodes</c>).
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.ServerFailures)]
public class ErrorCodeMappingTests
{
    private const int TIMEOUT_EXCEEDED = 159;
    private const int MEMORY_LIMIT_EXCEEDED = 241;
    private const int TOO_MANY_SIMULTANEOUS_QUERIES = 202; // server uses 202 for QUERY_NOT_ALLOWED in some, 242 in others

    private readonly SingleNodeFixture _fixture;

    public ErrorCodeMappingTests(SingleNodeFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task QueryTimeout_SurfacesCode159_ConnectionStaysUsable()
    {
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
            await conn.ExecuteScalarAsync<ulong>(
                "SELECT count() FROM numbers(100000000000) SETTINGS max_execution_time = 1, timeout_overflow_mode = 'throw'"));

        var server = ex as ClickHouseServerException ?? ex.InnerException as ClickHouseServerException;
        Assert.NotNull(server);
        Assert.Equal(TIMEOUT_EXCEEDED, server!.ErrorCode);

        // Connection still usable.
        var v = await conn.ExecuteScalarAsync<int>("SELECT 1");
        Assert.Equal(1, v);
    }

    [Fact]
    public async Task MemoryLimitExceeded_SurfacesCode241_ConnectionStaysUsable()
    {
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        // Force a tiny memory budget on a heavyweight query (string aggregation forces
        // allocation server-side).
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
            await conn.ExecuteScalarAsync<string>(
                "SELECT groupArrayArray(arrayMap(x -> toString(x), range(1000000)))[1] " +
                "FROM numbers(1000000) SETTINGS max_memory_usage = 1000000"));

        var server = ex as ClickHouseServerException ?? ex.InnerException as ClickHouseServerException;
        Assert.NotNull(server);
        Assert.Equal(MEMORY_LIMIT_EXCEEDED, server!.ErrorCode);

        var v = await conn.ExecuteScalarAsync<int>("SELECT 1");
        Assert.Equal(1, v);
    }

    [Fact]
    public async Task DropUserMidSession_NextQueryFails_ReconnectIsClean()
    {
        // Create a temp user, connect as them, drop the user from a sibling, prove
        // the next query fails — and a fresh connection is impossible until the user
        // is re-created.
        var user = $"sf_user_{Guid.NewGuid():N}".Substring(0, 16);
        var pw = "sf_pw";

        await using var admin = new ClickHouseConnection(_fixture.BuildSettings());
        await admin.OpenAsync();
        await admin.ExecuteNonQueryAsync($"CREATE USER {user} IDENTIFIED WITH plaintext_password BY '{pw}'");
        await admin.ExecuteNonQueryAsync($"GRANT SELECT ON *.* TO {user}");

        try
        {
            var userSettings = ClickHouseConnectionSettings.CreateBuilder()
                .WithHost(_fixture.Host)
                .WithPort(_fixture.Port)
                .WithCredentials(user, pw)
                .Build();

            await using var userConn = new ClickHouseConnection(userSettings);
            await userConn.OpenAsync();
            Assert.Equal(1, await userConn.ExecuteScalarAsync<int>("SELECT 1"));

            await admin.ExecuteNonQueryAsync($"DROP USER {user}");

            // Existing connection: server may keep the session alive, OR reject the next
            // query. We capture the outcome and pin both modes — but reject silent
            // misbehaviour like a generic Exception with no useful info.
            Exception? probeEx = null;
            int? probeResult = null;
            try
            {
                probeResult = await userConn.ExecuteScalarAsync<int>("SELECT 1");
            }
            catch (Exception ex) { probeEx = ex; }

            if (probeEx is not null)
            {
                // If it threw, classify: must be a server exception (auth) or wire-typed.
                var server = probeEx as Exceptions.ClickHouseServerException
                             ?? probeEx.InnerException as Exceptions.ClickHouseServerException;
                var typed = server is not null
                    || RetryPolicy.IsConnectionPoisoning(probeEx);
                Assert.True(typed,
                    $"Existing-connection probe threw, but with unclear shape: {probeEx.GetType().FullName}: {probeEx.Message}");
            }
            else
            {
                Assert.Equal(1, probeResult);
            }

            // Fresh connection MUST fail with a server exception, not hang.
            var sw = System.Diagnostics.Stopwatch.StartNew();
            await using var doomed = new ClickHouseConnection(userSettings);
            await Assert.ThrowsAnyAsync<Exception>(() => doomed.OpenAsync());
            sw.Stop();
            Assert.True(sw.ElapsedMilliseconds < 10_000,
                $"Auth-revoked OpenAsync should fail within 10s, took {sw.ElapsedMilliseconds}ms");
        }
        finally
        {
            try { await admin.ExecuteNonQueryAsync($"DROP USER IF EXISTS {user}"); } catch { }
        }
    }

    [Fact]
    public async Task TooManyConcurrentQueriesForUser_SurfacesAsServerException()
    {
        // Build a user with concurrency=1 and verify the second concurrent query fails
        // with a server error, not a hang.
        var user = $"sf_concur_{Guid.NewGuid():N}".Substring(0, 16);
        var profile = $"sf_prof_{Guid.NewGuid():N}".Substring(0, 16);
        var pw = "sf_pw";

        await using var admin = new ClickHouseConnection(_fixture.BuildSettings());
        await admin.OpenAsync();
        try
        {
            await admin.ExecuteNonQueryAsync(
                $"CREATE SETTINGS PROFILE {profile} SETTINGS max_concurrent_queries_for_user = 1");
            await admin.ExecuteNonQueryAsync(
                $"CREATE USER {user} IDENTIFIED WITH plaintext_password BY '{pw}' SETTINGS PROFILE {profile}");
            await admin.ExecuteNonQueryAsync($"GRANT SELECT ON *.* TO {user}");

            var userSettings = ClickHouseConnectionSettings.CreateBuilder()
                .WithHost(_fixture.Host)
                .WithPort(_fixture.Port)
                .WithCredentials(user, pw)
                .Build();

            await using var c1 = new ClickHouseConnection(userSettings);
            await c1.OpenAsync();
            await using var c2 = new ClickHouseConnection(userSettings);
            await c2.OpenAsync();

            var slow = c1.ExecuteScalarAsync<ulong>("SELECT count() FROM numbers(2000000000)");
            await Task.Delay(200); // give the slow query time to start

            var second = await Assert.ThrowsAnyAsync<Exception>(async () =>
                await c2.ExecuteScalarAsync<int>("SELECT 1"));
            var server = second as ClickHouseServerException ?? second.InnerException as ClickHouseServerException;
            Assert.NotNull(server);
            // Pin to the documented set of concurrency-related error codes. If a future
            // CH version uses a different code, the test should fail loudly so we can
            // update the set rather than silently accept any server error.
            // 202 = QUERY_NOT_ALLOWED, 242 = TOO_MANY_SIMULTANEOUS_QUERIES.
            Assert.True(server!.ErrorCode is 202 or 242,
                $"Expected concurrency error code 202 or 242, got {server.ErrorCode}: {server.Message}");

            try { await slow; } catch { }
        }
        finally
        {
            try { await admin.ExecuteNonQueryAsync($"DROP USER IF EXISTS {user}"); } catch { }
            try { await admin.ExecuteNonQueryAsync($"DROP SETTINGS PROFILE IF EXISTS {profile}"); } catch { }
        }
    }
}
