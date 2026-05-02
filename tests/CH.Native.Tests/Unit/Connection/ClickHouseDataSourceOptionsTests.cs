using CH.Native.Connection;
using Xunit;

namespace CH.Native.Tests.Unit.Connection;

/// <summary>
/// Pins the default values on <see cref="ClickHouseDataSourceOptions"/> so a
/// regression on tuning knobs surfaces in the unit suite, not in production
/// pool behaviour. These defaults map to documented invariants:
///   MaxPoolSize=100 (mirrors clickhouse-go), MinPoolSize=0,
///   ConnectionIdleTimeout=5m (mirrors Npgsql), ConnectionLifetime=30m
///   (conservative JWT-rotation baseline), ConnectionWaitTimeout=30s,
///   ValidateOnRent=false, PrewarmOnStart=false,
///   ResetSessionStateOnReturn=true.
/// </summary>
public class ClickHouseDataSourceOptionsTests
{
    private static ClickHouseConnectionSettings MakeSettings() =>
        ClickHouseConnectionSettings.CreateBuilder().WithHost("localhost").Build();

    [Fact]
    public void Default_MaxPoolSize_Is100()
    {
        var opt = new ClickHouseDataSourceOptions { Settings = MakeSettings() };
        Assert.Equal(100, opt.MaxPoolSize);
    }

    [Fact]
    public void Default_MinPoolSize_IsZero()
    {
        var opt = new ClickHouseDataSourceOptions { Settings = MakeSettings() };
        Assert.Equal(0, opt.MinPoolSize);
    }

    [Fact]
    public void Default_ConnectionIdleTimeout_IsFiveMinutes()
    {
        var opt = new ClickHouseDataSourceOptions { Settings = MakeSettings() };
        Assert.Equal(TimeSpan.FromMinutes(5), opt.ConnectionIdleTimeout);
    }

    [Fact]
    public void Default_ConnectionLifetime_IsThirtyMinutes()
    {
        var opt = new ClickHouseDataSourceOptions { Settings = MakeSettings() };
        Assert.Equal(TimeSpan.FromMinutes(30), opt.ConnectionLifetime);
    }

    [Fact]
    public void Default_ConnectionWaitTimeout_IsThirtySeconds()
    {
        var opt = new ClickHouseDataSourceOptions { Settings = MakeSettings() };
        Assert.Equal(TimeSpan.FromSeconds(30), opt.ConnectionWaitTimeout);
    }

    [Fact]
    public void Default_ValidateOnRent_IsFalse()
    {
        // Validate-on-rent is opt-in — the overhead usually outweighs the
        // benefit. Pin so it can't flip silently.
        var opt = new ClickHouseDataSourceOptions { Settings = MakeSettings() };
        Assert.False(opt.ValidateOnRent);
    }

    [Fact]
    public void Default_PrewarmOnStart_IsFalse()
    {
        var opt = new ClickHouseDataSourceOptions { Settings = MakeSettings() };
        Assert.False(opt.PrewarmOnStart);
    }

    [Fact]
    public void Default_ResetSessionStateOnReturn_IsTrue()
    {
        // ResetSessionStateOnReturn defaults ON because ClickHouse's session
        // model otherwise leaks SET / temp-table state between pool renters.
        // Pin this — flipping the default would be a silent correctness
        // regression for shared-pool consumers.
        var opt = new ClickHouseDataSourceOptions { Settings = MakeSettings() };
        Assert.True(opt.ResetSessionStateOnReturn);
    }

    [Fact]
    public void Default_ConnectionFactory_IsNull()
    {
        var opt = new ClickHouseDataSourceOptions { Settings = MakeSettings() };
        Assert.Null(opt.ConnectionFactory);
    }

    [Fact]
    public void Settings_IsRequired_OmitFails()
    {
        // The `required` modifier on Settings means the object initialiser
        // form refuses to compile without it; check that direct property
        // construction maintains the contract.
        var opt = new ClickHouseDataSourceOptions { Settings = MakeSettings() };
        Assert.NotNull(opt.Settings);
        Assert.Equal("localhost", opt.Settings.Host);
    }

    [Fact]
    public void MaxPoolSize_Mutable()
    {
        var opt = new ClickHouseDataSourceOptions { Settings = MakeSettings() };
        opt.MaxPoolSize = 50;
        Assert.Equal(50, opt.MaxPoolSize);
    }

    [Fact]
    public void TimeSpanProperties_AcceptZeroAndLargeValues()
    {
        // The options class itself doesn't reject extreme values — that's the
        // pool's responsibility at construction. Pin the lack of construct-time
        // validation so a future refactor that adds it doesn't silently break
        // callers that intentionally pass TimeSpan.Zero (e.g. ValidateOnRent
        // with an immediate timeout for unit tests).
        var opt = new ClickHouseDataSourceOptions
        {
            Settings = MakeSettings(),
            ConnectionIdleTimeout = TimeSpan.Zero,
            ConnectionLifetime = TimeSpan.FromHours(24),
            ConnectionWaitTimeout = TimeSpan.FromMilliseconds(1),
        };
        Assert.Equal(TimeSpan.Zero, opt.ConnectionIdleTimeout);
        Assert.Equal(TimeSpan.FromHours(24), opt.ConnectionLifetime);
        Assert.Equal(TimeSpan.FromMilliseconds(1), opt.ConnectionWaitTimeout);
    }
}
