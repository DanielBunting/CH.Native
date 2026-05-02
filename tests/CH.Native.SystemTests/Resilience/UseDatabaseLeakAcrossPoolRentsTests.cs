using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Resilience;

/// <summary>
/// Probes for an extension of the H9 finding: <c>USE database</c> changes
/// the session's default database server-side and persists for the lifetime
/// of the physical connection. If the pool reuses the same connection for
/// the next rent without resetting the session, that next caller queries
/// the wrong database silently.
///
/// <para>
/// Common in production: an admin tool issues <c>USE analytics</c> to switch
/// context, then returns the connection. The next user's <c>SELECT *</c>
/// against an unqualified table name resolves against <c>analytics</c>
/// instead of the user's expected default.
/// </para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Resilience)]
public class UseDatabaseLeakAcrossPoolRentsTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public UseDatabaseLeakAcrossPoolRentsTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task UseDatabase_PersistsToNextRent_WhenSamePhysicalConnectionReused()
    {
        // Setup: create a side database for the test.
        var sideDb = $"use_leak_{Guid.NewGuid():N}";
        await using (var setup = new ClickHouseConnection(_fx.BuildSettings()))
        {
            await setup.OpenAsync();
            await setup.ExecuteNonQueryAsync($"CREATE DATABASE {sideDb}");
        }

        try
        {
            await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
            {
                Settings = _fx.BuildSettings(),
                MaxPoolSize = 1, // Force same physical connection reuse.
            });

            // Rent 1: switch to side database.
            await using (var conn = await ds.OpenConnectionAsync())
            {
                await conn.ExecuteNonQueryAsync($"USE {sideDb}");
                var current = await conn.ExecuteScalarAsync<string>("SELECT currentDatabase()");
                Assert.Equal(sideDb, current);
            }

            // Rent 2 — same physical connection. ResetSessionStateOnReturn
            // (default true) restored the default database before this rent.
            await using (var conn = await ds.OpenConnectionAsync())
            {
                var current = await conn.ExecuteScalarAsync<string>("SELECT currentDatabase()");
                _output.WriteLine($"Database on second rent: {current}");
                Assert.NotEqual(sideDb, current);
            }
        }
        finally
        {
            await using var cleanup = new ClickHouseConnection(_fx.BuildSettings());
            await cleanup.OpenAsync();
            try { await cleanup.ExecuteNonQueryAsync($"DROP DATABASE IF EXISTS {sideDb}"); } catch { }
        }
    }

    [Fact]
    public async Task TemporaryTable_PersistsToNextRent_WhenSamePhysicalConnectionReused()
    {
        // Same pattern as USE: a temp table created in rent #1 is visible
        // to rent #2 if the pool reuses the same connection. This is a
        // server-side semantic (temp table = session-scoped), so the only
        // way to fix is for the pool to reset session state on return.
        await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _fx.BuildSettings(),
            MaxPoolSize = 1,
        });

        var tempName = $"tmp_{Guid.NewGuid():N}";

        await using (var conn = await ds.OpenConnectionAsync())
        {
            await conn.ExecuteNonQueryAsync(
                $"CREATE TEMPORARY TABLE {tempName} (id Int32) ENGINE = Memory");
            await conn.ExecuteNonQueryAsync($"INSERT INTO {tempName} VALUES (42)");
            var v = await conn.ExecuteScalarAsync<int>($"SELECT id FROM {tempName}");
            Assert.Equal(42, v);
        }

        await using (var conn = await ds.OpenConnectionAsync())
        {
            // ResetSessionStateOnReturn drops temporary tables before the
            // next rent. The next rent must NOT see the previous temp table.
            Exception? caught = null;
            int? value = null;
            try
            {
                value = await conn.ExecuteScalarAsync<int>($"SELECT id FROM {tempName}");
            }
            catch (Exception ex)
            {
                caught = ex;
            }
            _output.WriteLine($"Temp table access on second rent: thrown={caught?.GetType().Name}, value={value}");

            // Server-side error 60 (UNKNOWN_TABLE) or similar — the temp
            // table was dropped on return.
            Assert.NotNull(caught);
            Assert.Null(value);
        }
    }
}
