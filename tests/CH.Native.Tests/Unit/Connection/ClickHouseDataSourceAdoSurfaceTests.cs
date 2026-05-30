using System.Data.Common;
using CH.Native.Connection;
using CH.Native.Resilience;
using Xunit;

namespace CH.Native.Tests.Unit.Connection;

/// <summary>
/// Unit coverage for the <see cref="DbDataSource"/>-derived surface added when
/// <see cref="ClickHouseDataSource"/> became an ADO data source: the
/// <see cref="ClickHouseDataSource.ConnectionString"/> / <see cref="ClickHouseDataSource.Resilience"/>
/// accessors and the synchronous <see cref="DbDataSource.CreateConnection"/>
/// factory hook. None of these open a socket, so no server is required.
/// (Default options: MinPoolSize 0, PrewarmOnStart false — construction is inert.)
/// </summary>
public class ClickHouseDataSourceAdoSurfaceTests
{
    private const string ConnString = "Host=localhost;Port=9000;Username=default;Password=";

    [Fact]
    public async Task ConnectionString_ReflectsBaselineSettings()
    {
        await using var ds = new ClickHouseDataSource(ConnString);

        var cs = ds.ConnectionString;
        Assert.False(string.IsNullOrEmpty(cs));
        Assert.Contains("localhost", cs, StringComparison.OrdinalIgnoreCase);
        // Matches the connection's own canonical (password-less) rendering.
        Assert.Equal(ds.Settings.ToString(), cs);
    }

    [Fact]
    public async Task Resilience_IsNull_WhenNoneConfigured()
    {
        await using var ds = new ClickHouseDataSource(ConnString);
        Assert.Null(ds.Resilience);
    }

    [Fact]
    public async Task Resilience_ReflectsConfiguredOptions()
    {
        var settings = new ClickHouseConnectionSettingsBuilder()
            .WithHost("localhost")
            .WithResilience(ResilienceOptions.WithRetryDefaults())
            .Build();

        await using var ds = new ClickHouseDataSource(settings);

        Assert.NotNull(ds.Resilience);
        Assert.True(ds.Resilience!.HasRetry);
        Assert.Same(settings.Resilience, ds.Resilience);
    }

    [Fact]
    public async Task CreateConnection_Typed_ReturnsUnopenedConnection()
    {
        await using var ds = new ClickHouseDataSource(ConnString);

        using var conn = ds.CreateConnection();
        Assert.Equal(System.Data.ConnectionState.Closed, conn.State);
    }

    [Fact]
    public async Task DbDataSource_CreateConnection_RoutesThroughCreateDbConnection()
    {
        await using var ds = new ClickHouseDataSource(ConnString);

        // Calling the non-virtual base CreateConnection() invokes the protected
        // CreateDbConnection() override, which must yield a ClickHouseConnection.
        DbConnection conn = ((DbDataSource)ds).CreateConnection();
        using (conn)
        {
            Assert.IsType<ClickHouseConnection>(conn);
            Assert.Equal(System.Data.ConnectionState.Closed, conn.State);
        }
    }
}
