using System.Data;
using System.Data.Common;
using CH.Native.Connection;
using CH.Native.Resilience;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration.Connection;

/// <summary>
/// Covers the <see cref="DbDataSource"/> open hooks added to
/// <see cref="ClickHouseDataSource"/>. The base's non-virtual
/// <see cref="DbDataSource.OpenConnectionAsync"/> / <see cref="DbDataSource.OpenConnection"/>
/// route into the protected <c>OpenDbConnectionAsync</c> / <c>OpenDbConnection</c>
/// overrides, which rent from the pool — so an ADO consumer holding only a
/// <see cref="DbDataSource"/> gets an open, pooled connection.
/// </summary>
[Collection("ClickHouse")]
public class ClickHouseDataSourceDbDataSourceTests
{
    private readonly ClickHouseFixture _fixture;

    public ClickHouseDataSourceDbDataSourceTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task OpenConnectionAsync_ViaDbDataSource_ReturnsOpenConnection()
    {
        await using var ds = new ClickHouseDataSource(_fixture.ConnectionString);
        DbDataSource baseDs = ds;

        await using var conn = await baseDs.OpenConnectionAsync();
        Assert.Equal(ConnectionState.Open, conn.State);
        Assert.IsType<ClickHouseConnection>(conn);
    }

    [Fact]
    public void OpenConnection_Sync_ViaDbDataSource_ReturnsOpenConnection()
    {
        using var ds = new ClickHouseDataSource(_fixture.ConnectionString);
        DbDataSource baseDs = ds;

        using var conn = baseDs.OpenConnection();
        Assert.Equal(ConnectionState.Open, conn.State);
    }

    [Fact]
    public async Task OpenAsync_WithRetryConfigured_SucceedsAgainstHealthyServer()
    {
        // Exercises the happy path of the retry-on-connect branch: connect +
        // handshake succeed on the first attempt, so the policy returns without
        // entering the catch/reset path.
        var settings = new ClickHouseConnectionSettingsBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithCredentials(_fixture.Username, _fixture.Password)
            .WithResilience(ResilienceOptions.WithRetryDefaults())
            .Build();

        await using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();

        Assert.Equal(ConnectionState.Open, conn.State);
        Assert.Equal(1L, await conn.ExecuteScalarAsync<long>("SELECT toInt64(1)"));
    }
}
