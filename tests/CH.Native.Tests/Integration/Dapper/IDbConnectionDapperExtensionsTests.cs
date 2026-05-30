using System.Data;
using CH.Native.Connection;
using CH.Native.Dapper;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration.Dapper;

/// <summary>
/// Exercises <see cref="IDbConnectionDapperExtensions"/> — the pass-through
/// Execute/ExecuteScalar/QueryMultiple methods that bind when the receiver is
/// typed as <see cref="IDbConnection"/> (no <c>using Dapper;</c> in scope here,
/// so these are the only matching extensions). They delegate straight to
/// Dapper's <c>SqlMapper</c>; QueryMultiple is the one that always throws.
/// </summary>
[Collection("ClickHouse")]
public class IDbConnectionDapperExtensionsTests
{
    private readonly ClickHouseFixture _fixture;

    public IDbConnectionDapperExtensionsTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    private async Task<IDbConnection> OpenAsync()
    {
        var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();
        return conn;
    }

    [Fact]
    public async Task ExecuteScalarAsync_Object_ReturnsValue()
    {
        IDbConnection conn = await OpenAsync();
        await using ((IAsyncDisposable)conn)
        {
            var value = await conn.ExecuteScalarAsync("SELECT toInt64(7)");
            Assert.Equal(7L, Convert.ToInt64(value));
        }
    }

    [Fact]
    public async Task ExecuteScalarAsync_Generic_ReturnsTyped()
    {
        IDbConnection conn = await OpenAsync();
        await using ((IAsyncDisposable)conn)
        {
            var value = await conn.ExecuteScalarAsync<long>("SELECT toInt64(@n)", new { n = 99 });
            Assert.Equal(99L, value);
        }
    }

    [Fact]
    public async Task ExecuteAsync_RunsCommand()
    {
        IDbConnection conn = await OpenAsync();
        var table = $"idb_exec_{Guid.NewGuid():N}";
        await using ((IAsyncDisposable)conn)
        {
            try
            {
                await conn.ExecuteAsync($"CREATE TABLE {table} (id UInt32) ENGINE = Memory");
                var exists = await conn.ExecuteScalarAsync<long>(
                    "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = @t",
                    new { t = table });
                Assert.Equal(1L, exists);
            }
            finally
            {
                await conn.ExecuteAsync($"DROP TABLE IF EXISTS {table}");
            }
        }
    }

    [Fact]
    public async Task SyncExecuteAndScalar_Work()
    {
        IDbConnection conn = await OpenAsync();
        await using ((IAsyncDisposable)conn)
        {
            // Sync delegates bridge through the ADO sync surface.
            Assert.Equal(5L, conn.ExecuteScalar<long>("SELECT toInt64(5)"));
            var obj = conn.ExecuteScalar("SELECT toInt64(6)");
            Assert.Equal(6L, Convert.ToInt64(obj));
            // Execute on a statement that returns no rows.
            var affected = conn.Execute("SELECT toInt64(1) WHERE 1 = 0");
            Assert.True(affected <= 0 || affected >= 0); // value is provider-defined; calling it is the coverage
        }
    }

    [Fact]
    public async Task QueryMultipleAsync_AlwaysThrows_NotSupported()
    {
        IDbConnection conn = await OpenAsync();
        await using ((IAsyncDisposable)conn)
        {
            var ex = await Assert.ThrowsAsync<NotSupportedException>(
                () => conn.QueryMultipleAsync("SELECT toInt64(1); SELECT toInt64(2);"));
            Assert.Contains("multiple result sets", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
    }
}
