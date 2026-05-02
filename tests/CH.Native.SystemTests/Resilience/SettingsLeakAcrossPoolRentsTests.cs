using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Resilience;

/// <summary>
/// Probes whether ClickHouse session-scoped <c>SET</c> settings leak across
/// pool rents. <c>SET</c> persists for the connection's session lifetime in
/// ClickHouse — a setting issued by one caller stays in effect for whoever
/// rents the same physical connection next. Common cases:
///
/// <list type="bullet">
/// <item><description><c>SET max_memory_usage = 1000000</c> — caller A throttles, then B
///     gets the same throttle without asking for it.</description></item>
/// <item><description><c>SET allow_experimental_object_type = 1</c> — caller A enables
///     an experimental type, B inherits the flag.</description></item>
/// <item><description><c>SET max_threads = 1</c> — caller A serializes, B gets reduced
///     parallelism silently.</description></item>
/// </list>
///
/// <para>
/// This is "probe-and-document": the answer might be (a) the library wipes
/// settings between rents, (b) it doesn't and they leak — pin whichever is
/// real today so a future change is visible.
/// </para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Resilience)]
public class SettingsLeakAcrossPoolRentsTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public SettingsLeakAcrossPoolRentsTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task SetMaxThreads_Persists_WhenSamePhysicalConnectionIsRentedAgain()
    {
        await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _fx.BuildSettings(),
            MaxPoolSize = 1, // Force reuse of the same physical connection.
        });

        // Rent #1: pin a non-default value via SET.
        await using (var conn = await ds.OpenConnectionAsync())
        {
            await conn.ExecuteNonQueryAsync("SET max_threads = 1");
            // Verify it took effect in this rent.
            var v = await conn.ExecuteScalarAsync<string>(
                "SELECT value FROM system.settings WHERE name = 'max_threads'");
            Assert.Equal("1", v);
        }

        // Rent #2 — same physical connection (MaxPoolSize=1 + recent rent).
        // ResetSessionStateOnReturn (default true) restored max_threads to
        // its default before this rent received the connection.
        await using (var conn = await ds.OpenConnectionAsync())
        {
            var v = await conn.ExecuteScalarAsync<string>(
                "SELECT value FROM system.settings WHERE name = 'max_threads'");

            _output.WriteLine($"max_threads after second rent on same conn: {v}");
            Assert.NotEqual("1", v);
        }
    }

    [Fact]
    public async Task SetMaxThreads_LeaksAcrossRents_WhenResetIsDisabled()
    {
        // Opt-out flag preserves the legacy leak behavior for callers who
        // explicitly want raw session control.
        await using var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
        {
            Settings = _fx.BuildSettings(),
            MaxPoolSize = 1,
            ResetSessionStateOnReturn = false,
        });

        await using (var conn = await ds.OpenConnectionAsync())
        {
            await conn.ExecuteNonQueryAsync("SET max_threads = 1");
        }

        await using (var conn = await ds.OpenConnectionAsync())
        {
            var v = await conn.ExecuteScalarAsync<string>(
                "SELECT value FROM system.settings WHERE name = 'max_threads'");
            Assert.Equal("1", v);
        }
    }

    [Fact]
    public async Task PerQuerySettingsClause_DoesNotPersistAcrossQueries()
    {
        // Sanity check: the recommended per-query SETTINGS form does NOT
        // leak — it's scoped to the single query. This is the safe pattern
        // and we pin it as such.
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        // Set max_threads = 1 for ONE query only.
        await conn.ExecuteNonQueryAsync(
            "SELECT 1 SETTINGS max_threads = 1");

        var v = await conn.ExecuteScalarAsync<string>(
            "SELECT value FROM system.settings WHERE name = 'max_threads'");
        // Should be the default, not '1'.
        _output.WriteLine($"max_threads after per-query SETTINGS: {v}");
        Assert.NotEqual("1", v);
    }

    [Fact]
    public async Task SetExperimentalFlag_PersistsOnSameRent()
    {
        // Confirms that within a single connection rent, multiple queries
        // see the same session state. (Sanity check for the pin above.)
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        await conn.ExecuteNonQueryAsync("SET max_block_size = 4096");

        var v = await conn.ExecuteScalarAsync<string>(
            "SELECT value FROM system.settings WHERE name = 'max_block_size'");
        Assert.Equal("4096", v);
    }
}
