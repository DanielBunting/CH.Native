using CH.Native.Connection;
using CH.Native.DependencyInjection;
using Xunit;

namespace CH.Native.Tests.Unit.Connection;

/// <summary>
/// The DI-layer validator runs at registration time so misconfigured options
/// fail fast (with a section path in the message) instead of surfacing as
/// opaque <see cref="ArgumentOutOfRangeException"/>s on first use. These
/// tests pin the failure surface — pool bounds, auth/credential pairings,
/// port ranges — and confirm the consolidated error message includes the
/// section path when supplied.
/// </summary>
public class ClickHouseConnectionOptionsValidatorTests
{
    [Fact]
    public void EmptyOptions_PassesValidation()
    {
        // Default-constructed options are valid (all defaults applied later).
        ClickHouseConnectionOptionsValidator.ValidateOrThrow(new ClickHouseConnectionOptions());
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Pool_MaxPoolSize_BelowOne_Fails(int max)
    {
        var opts = new ClickHouseConnectionOptions
        {
            Pool = new ClickHouseConnectionOptions.PoolOptions { MaxPoolSize = max },
        };

        var ex = Assert.Throws<ArgumentException>(() =>
            ClickHouseConnectionOptionsValidator.ValidateOrThrow(opts));
        Assert.Contains("MaxPoolSize", ex.Message);
    }

    [Fact]
    public void Pool_MinGreaterThanMax_Fails()
    {
        var opts = new ClickHouseConnectionOptions
        {
            Pool = new ClickHouseConnectionOptions.PoolOptions { MaxPoolSize = 5, MinPoolSize = 10 },
        };

        var ex = Assert.Throws<ArgumentException>(() =>
            ClickHouseConnectionOptionsValidator.ValidateOrThrow(opts));
        Assert.Contains("MinPoolSize", ex.Message);
    }

    [Fact]
    public void Pool_MinPoolSize_Negative_Fails()
    {
        var opts = new ClickHouseConnectionOptions
        {
            Pool = new ClickHouseConnectionOptions.PoolOptions { MinPoolSize = -1 },
        };

        var ex = Assert.Throws<ArgumentException>(() =>
            ClickHouseConnectionOptionsValidator.ValidateOrThrow(opts));
        Assert.Contains("MinPoolSize", ex.Message);
    }

    [Theory]
    [InlineData("ConnectionWaitTimeout")]
    [InlineData("ConnectionLifetime")]
    [InlineData("ConnectionIdleTimeout")]
    public void Pool_NegativeTimeSpan_Fails(string fieldName)
    {
        var pool = new ClickHouseConnectionOptions.PoolOptions();
        var negative = TimeSpan.FromSeconds(-1);
        switch (fieldName)
        {
            case "ConnectionWaitTimeout": pool.ConnectionWaitTimeout = negative; break;
            case "ConnectionLifetime": pool.ConnectionLifetime = negative; break;
            case "ConnectionIdleTimeout": pool.ConnectionIdleTimeout = negative; break;
        }

        var opts = new ClickHouseConnectionOptions { Pool = pool };
        var ex = Assert.Throws<ArgumentException>(() =>
            ClickHouseConnectionOptionsValidator.ValidateOrThrow(opts));
        Assert.Contains(fieldName, ex.Message);
    }

    [Fact]
    public void Auth_Jwt_WithoutToken_Fails()
    {
        var opts = new ClickHouseConnectionOptions
        {
            AuthMethod = ClickHouseAuthMethod.Jwt,
        };

        var ex = Assert.Throws<ArgumentException>(() =>
            ClickHouseConnectionOptionsValidator.ValidateOrThrow(opts));
        Assert.Contains("Jwt", ex.Message);
        Assert.Contains("JwtToken", ex.Message);
    }

    [Fact]
    public void Auth_Ssh_WithoutKeyPath_Fails()
    {
        var opts = new ClickHouseConnectionOptions
        {
            AuthMethod = ClickHouseAuthMethod.SshKey,
        };

        var ex = Assert.Throws<ArgumentException>(() =>
            ClickHouseConnectionOptionsValidator.ValidateOrThrow(opts));
        Assert.Contains("SshKey", ex.Message);
        Assert.Contains("SshPrivateKeyPath", ex.Message);
    }

    [Fact]
    public void Auth_WithConnectionString_DefersValidation()
    {
        // When a ConnectionString is supplied the validator skips the
        // auth/credential checks because the connection-string parser owns
        // that responsibility. Pin so the deferral can't accidentally come
        // back as a duplicate validation pass.
        var opts = new ClickHouseConnectionOptions
        {
            ConnectionString = "Host=localhost",
            AuthMethod = ClickHouseAuthMethod.Jwt,  // no token, but ConnectionString defers
        };

        ClickHouseConnectionOptionsValidator.ValidateOrThrow(opts);  // does not throw
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(65536)]
    public void Port_OutOfRange_Fails(int port)
    {
        var opts = new ClickHouseConnectionOptions { Port = port };

        var ex = Assert.Throws<ArgumentException>(() =>
            ClickHouseConnectionOptionsValidator.ValidateOrThrow(opts));
        Assert.Contains("Port", ex.Message);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(65536)]
    public void TlsPort_OutOfRange_Fails(int tlsPort)
    {
        var opts = new ClickHouseConnectionOptions { TlsPort = tlsPort };

        var ex = Assert.Throws<ArgumentException>(() =>
            ClickHouseConnectionOptionsValidator.ValidateOrThrow(opts));
        Assert.Contains("TlsPort", ex.Message);
    }

    [Fact]
    public void Failure_IncludesSectionPath_WhenProvided()
    {
        var opts = new ClickHouseConnectionOptions { Port = -1 };

        var ex = Assert.Throws<ArgumentException>(() =>
            ClickHouseConnectionOptionsValidator.ValidateOrThrow(opts, sectionPath: "ClickHouse:Primary"));
        Assert.Contains("ClickHouse:Primary", ex.Message);
    }

    [Fact]
    public void Failure_AggregatesMultipleErrors()
    {
        var opts = new ClickHouseConnectionOptions
        {
            Port = 0,
            TlsPort = 0,
            Pool = new ClickHouseConnectionOptions.PoolOptions { MaxPoolSize = 0, MinPoolSize = -5 },
        };

        var ex = Assert.Throws<ArgumentException>(() =>
            ClickHouseConnectionOptionsValidator.ValidateOrThrow(opts));
        Assert.Contains("Port", ex.Message);
        Assert.Contains("TlsPort", ex.Message);
        Assert.Contains("MaxPoolSize", ex.Message);
        Assert.Contains("MinPoolSize", ex.Message);
    }
}
