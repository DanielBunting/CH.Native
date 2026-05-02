using CH.Native.Compression;
using CH.Native.Connection;
using CH.Native.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace CH.Native.Tests.Unit.Connection;

/// <summary>
/// <see cref="ClickHouseConnectionOptions"/> is the binder-friendly POCO that
/// <c>AddClickHouse(IConfiguration)</c> hydrates from <c>appsettings.json</c>.
/// These tests verify the binding contract end-to-end with the
/// <see cref="Microsoft.Extensions.Configuration"/> binder so a regression
/// in property names / casing surfaces here, not as silent defaults at
/// runtime.
/// </summary>
public class ClickHouseConnectionOptionsBindingTests
{
    private static IConfigurationSection BuildSection(IDictionary<string, string?> entries, string root = "ClickHouse")
    {
        var prefixed = entries.ToDictionary(kv => $"{root}:{kv.Key}", kv => kv.Value);
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(prefixed!)
            .Build();
        return config.GetSection(root);
    }

    [Fact]
    public void Binding_FlatPrimitives_Hydrate()
    {
        var section = BuildSection(new Dictionary<string, string?>
        {
            ["Host"] = "ch.example.com",
            ["Port"] = "9001",
            ["Database"] = "analytics",
            ["Username"] = "reader",
            ["Password"] = "s3cr3t",
            ["Compress"] = "true",
            ["CompressionMethod"] = "Zstd",
        });

        var opts = section.Get<ClickHouseConnectionOptions>();

        Assert.NotNull(opts);
        Assert.Equal("ch.example.com", opts!.Host);
        Assert.Equal(9001, opts.Port);
        Assert.Equal("analytics", opts.Database);
        Assert.Equal("reader", opts.Username);
        Assert.Equal("s3cr3t", opts.Password);
        Assert.True(opts.Compress);
        Assert.Equal(CompressionMethod.Zstd, opts.CompressionMethod);
    }

    [Fact]
    public void Binding_ConnectionString_Hydrates()
    {
        var section = BuildSection(new Dictionary<string, string?>
        {
            ["ConnectionString"] = "Host=ch.example.com;Port=9000;Database=mydb",
        });

        var opts = section.Get<ClickHouseConnectionOptions>();

        Assert.NotNull(opts);
        Assert.Equal("Host=ch.example.com;Port=9000;Database=mydb", opts!.ConnectionString);
    }

    [Fact]
    public void Binding_Pool_NestedObject_Hydrates()
    {
        var section = BuildSection(new Dictionary<string, string?>
        {
            ["Pool:MaxPoolSize"] = "50",
            ["Pool:MinPoolSize"] = "5",
            ["Pool:ConnectionLifetime"] = "00:30:00",
        });

        var opts = section.Get<ClickHouseConnectionOptions>();

        Assert.NotNull(opts!.Pool);
        Assert.Equal(50, opts.Pool.MaxPoolSize);
        Assert.Equal(5, opts.Pool.MinPoolSize);
        Assert.Equal(TimeSpan.FromMinutes(30), opts.Pool.ConnectionLifetime);
    }

    [Fact]
    public void Binding_AuthMethod_EnumValue()
    {
        var section = BuildSection(new Dictionary<string, string?>
        {
            ["AuthMethod"] = "Jwt",
            ["JwtToken"] = "eyJhbGc.test.token",
        });

        var opts = section.Get<ClickHouseConnectionOptions>();

        Assert.Equal(ClickHouseAuthMethod.Jwt, opts!.AuthMethod);
        Assert.Equal("eyJhbGc.test.token", opts.JwtToken);
    }

    [Fact]
    public void Binding_Roles_AsArray_Hydrates()
    {
        var section = BuildSection(new Dictionary<string, string?>
        {
            ["Roles:0"] = "reader",
            ["Roles:1"] = "analyst",
        });

        var opts = section.Get<ClickHouseConnectionOptions>();

        Assert.NotNull(opts!.Roles);
        Assert.Equal(2, opts.Roles!.Count);
        Assert.Equal("reader", opts.Roles[0]);
        Assert.Equal("analyst", opts.Roles[1]);
    }

    [Fact]
    public void Binding_EmptyConfig_AllNull()
    {
        var config = new ConfigurationBuilder().Build();
        var section = config.GetSection("Missing");

        var opts = section.Get<ClickHouseConnectionOptions>();

        // Empty section binds to either null or a default-constructed POCO
        // depending on framework version. Both are valid — pin the
        // observable consequence (no host wired up).
        if (opts is not null)
        {
            Assert.Null(opts.Host);
            Assert.Null(opts.ConnectionString);
        }
    }

    [Fact]
    public void Binding_BoundOptions_PassValidatorWhenWellFormed()
    {
        // The binder + validator pipeline is what AddClickHouse(IConfiguration)
        // runs at registration time. Pin that a well-formed section makes it
        // through both gates without an exception.
        var section = BuildSection(new Dictionary<string, string?>
        {
            ["Host"] = "localhost",
            ["Port"] = "9000",
            ["Pool:MaxPoolSize"] = "20",
            ["Pool:MinPoolSize"] = "0",
        });

        var opts = section.Get<ClickHouseConnectionOptions>();
        Assert.NotNull(opts);
        // Should not throw.
        ClickHouseConnectionOptionsValidator.ValidateOrThrow(opts!, sectionPath: "ClickHouse");
    }
}
