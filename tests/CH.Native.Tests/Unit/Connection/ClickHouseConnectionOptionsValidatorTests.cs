using CH.Native.Connection;
using CH.Native.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace CH.Native.Tests.Unit.Connection;

/// <summary>
/// The DI-layer validator is split: <see cref="ClickHouseConnectionOptionsValidator.ValidateOrThrow"/>
/// pins shape errors (pool bounds, port ranges) at registration time;
/// <c>ValidateAuthCredentialsOrThrow</c> pairs <c>AuthMethod</c> against the
/// builder's chained provider state at first DataSource resolution. Both
/// surfaces — including the consolidated message and section-path prefix —
/// are pinned here.
/// </summary>
public class ClickHouseConnectionOptionsValidatorTests
{
    private static ClickHouseDataSourceBuilder NewBuilder() =>
        new(new ServiceCollection(), serviceKey: null);

    private sealed class FakeJwtProvider : IClickHouseJwtProvider
    {
        public ValueTask<string> GetTokenAsync(CancellationToken ct) => new("test.jwt.token");
    }

    private sealed class FakeSshKeyProvider : IClickHouseSshKeyProvider
    {
        public ValueTask<SshKeyMaterial> GetKeyAsync(CancellationToken ct) =>
            new(new SshKeyMaterial(new byte[] { 0x00 }, Passphrase: null));
    }

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
    public void ValidateOrThrow_DoesNotCheckAuthPairing()
    {
        // ValidateOrThrow is shape-only — auth pairing is deferred to
        // ValidateAuthCredentialsOrThrow so chained provider registrations
        // get a chance to run on the builder first.
        var opts = new ClickHouseConnectionOptions
        {
            AuthMethod = ClickHouseAuthMethod.Jwt,  // no token, no provider yet
        };

        ClickHouseConnectionOptionsValidator.ValidateOrThrow(opts);  // does not throw
    }

    [Fact]
    public void AuthCredentials_Jwt_WithoutTokenAndWithoutProvider_Fails()
    {
        var opts = new ClickHouseConnectionOptions { AuthMethod = ClickHouseAuthMethod.Jwt };
        var builder = NewBuilder();

        var ex = Assert.Throws<ArgumentException>(() =>
            ClickHouseConnectionOptionsValidator.ValidateAuthCredentialsOrThrow(opts, builder));
        Assert.Contains("Jwt", ex.Message);
        Assert.Contains("JwtToken", ex.Message);
    }

    [Fact]
    public void AuthCredentials_Jwt_WithStaticToken_Passes()
    {
        var opts = new ClickHouseConnectionOptions
        {
            AuthMethod = ClickHouseAuthMethod.Jwt,
            JwtToken = "static.jwt.token",
        };

        ClickHouseConnectionOptionsValidator.ValidateAuthCredentialsOrThrow(opts, NewBuilder());
    }

    [Fact]
    public void AuthCredentials_Jwt_WithChainedProvider_Passes()
    {
        // The chained .WithJwtProvider<>() satisfies the requirement even
        // when JwtToken is empty — this is the bug-fix path that the user
        // can rely on going forward.
        var opts = new ClickHouseConnectionOptions { AuthMethod = ClickHouseAuthMethod.Jwt };
        var builder = NewBuilder();
        builder.WithJwtProvider<FakeJwtProvider>();

        ClickHouseConnectionOptionsValidator.ValidateAuthCredentialsOrThrow(opts, builder);
    }

    [Fact]
    public void AuthCredentials_Ssh_WithoutPathAndWithoutProvider_Fails()
    {
        var opts = new ClickHouseConnectionOptions { AuthMethod = ClickHouseAuthMethod.SshKey };
        var builder = NewBuilder();

        var ex = Assert.Throws<ArgumentException>(() =>
            ClickHouseConnectionOptionsValidator.ValidateAuthCredentialsOrThrow(opts, builder));
        Assert.Contains("SshKey", ex.Message);
        Assert.Contains("SshPrivateKeyPath", ex.Message);
    }

    [Fact]
    public void AuthCredentials_Ssh_WithStaticPath_Passes()
    {
        var opts = new ClickHouseConnectionOptions
        {
            AuthMethod = ClickHouseAuthMethod.SshKey,
            SshPrivateKeyPath = "/etc/ch/key.pem",
        };

        ClickHouseConnectionOptionsValidator.ValidateAuthCredentialsOrThrow(opts, NewBuilder());
    }

    [Fact]
    public void AuthCredentials_Ssh_WithChainedProvider_Passes()
    {
        var opts = new ClickHouseConnectionOptions { AuthMethod = ClickHouseAuthMethod.SshKey };
        var builder = NewBuilder();
        builder.WithSshKeyProvider<FakeSshKeyProvider>();

        ClickHouseConnectionOptionsValidator.ValidateAuthCredentialsOrThrow(opts, builder);
    }

    [Fact]
    public void AuthCredentials_WithConnectionString_DefersValidation()
    {
        // When a ConnectionString is supplied the validator skips the
        // auth/credential checks because the connection-string parser owns
        // that responsibility.
        var opts = new ClickHouseConnectionOptions
        {
            ConnectionString = "Host=localhost",
            AuthMethod = ClickHouseAuthMethod.Jwt,  // no token, but ConnectionString defers
        };

        ClickHouseConnectionOptionsValidator.ValidateAuthCredentialsOrThrow(opts, NewBuilder());
    }

    [Fact]
    public void AuthCredentials_PasswordAndCertificateMethods_NeverFailOnPairing()
    {
        // Empty password is a valid ClickHouse auth, and certificate auth
        // is wired by WithCertificateProvider itself — neither gets a
        // pairing rule.
        ClickHouseConnectionOptionsValidator.ValidateAuthCredentialsOrThrow(
            new ClickHouseConnectionOptions { AuthMethod = ClickHouseAuthMethod.Password },
            NewBuilder());
        ClickHouseConnectionOptionsValidator.ValidateAuthCredentialsOrThrow(
            new ClickHouseConnectionOptions { AuthMethod = ClickHouseAuthMethod.TlsClientCertificate },
            NewBuilder());
    }

    [Fact]
    public void AuthCredentials_FailureIncludesSectionPath()
    {
        var opts = new ClickHouseConnectionOptions { AuthMethod = ClickHouseAuthMethod.Jwt };
        var builder = NewBuilder();

        var ex = Assert.Throws<ArgumentException>(() =>
            ClickHouseConnectionOptionsValidator.ValidateAuthCredentialsOrThrow(opts, builder, "ClickHouse:Primary"));
        Assert.Contains("ClickHouse:Primary", ex.Message);
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
