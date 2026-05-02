using CH.Native.Connection;
using CH.Native.Resilience;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Ado;

/// <summary>
/// Pins the equivalence between the two settings entry points: connection
/// strings parsed via <see cref="ClickHouseConnectionSettings.Parse(string)"/>
/// and the fluent <see cref="ClickHouseConnectionSettingsBuilder"/>. Without
/// dedicated coverage, drift between the two surfaces is invisible — every
/// sample uses one or the other but never both for the same property.
///
/// <para>
/// Also covers malformed-value diagnostics (the exception must name the
/// offending key) and the partial round-trip identity exposed via
/// <see cref="ClickHouseConnectionSettings.ToString"/>.
/// </para>
/// </summary>
[Trait(Categories.Name, Categories.Suite)]
public class ConnectionStringRoundTripTests
{
    private readonly ITestOutputHelper _output;

    public ConnectionStringRoundTripTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public void Equivalence_HostPort_ParseMatchesBuilder()
    {
        var fromString = ClickHouseConnectionSettings.Parse("Host=h.example.com;Port=9999");
        var fromBuilder = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("h.example.com")
            .WithPort(9999)
            .Build();

        Assert.Equal(fromBuilder.Host, fromString.Host);
        Assert.Equal(fromBuilder.Port, fromString.Port);
    }

    [Fact]
    public void Equivalence_DatabaseUsernamePassword_ParseMatchesBuilder()
    {
        var fromString = ClickHouseConnectionSettings.Parse(
            "Host=h;Database=metrics;Username=svc;Password=p1");
        var fromBuilder = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("h")
            .WithDatabase("metrics")
            .WithCredentials("svc", "p1")
            .Build();

        Assert.Equal(fromBuilder.Database, fromString.Database);
        Assert.Equal(fromBuilder.Username, fromString.Username);
        Assert.Equal(fromBuilder.Password, fromString.Password);
    }

    [Fact]
    public void Equivalence_CompressionFlags_ParseMatchesBuilder()
    {
        var fromString = ClickHouseConnectionSettings.Parse("Host=h;Compress=true;CompressionMethod=Zstd");
        var fromBuilder = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("h")
            .WithCompression(true)
            .WithCompressionMethod(CH.Native.Compression.CompressionMethod.Zstd)
            .Build();

        Assert.Equal(fromBuilder.Compress, fromString.Compress);
        Assert.Equal(fromBuilder.CompressionMethod, fromString.CompressionMethod);
    }

    [Fact]
    public void Equivalence_TlsTriple_ParseMatchesBuilder()
    {
        var fromString = ClickHouseConnectionSettings.Parse(
            "Host=h;UseTls=true;TlsPort=9440;AllowInsecureTls=true");
        var fromBuilder = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("h")
            .WithTls()
            .WithTlsPort(9440)
            .WithAllowInsecureTls()
            .Build();

        Assert.Equal(fromBuilder.UseTls, fromString.UseTls);
        Assert.Equal(fromBuilder.TlsPort, fromString.TlsPort);
        Assert.Equal(fromBuilder.AllowInsecureTls, fromString.AllowInsecureTls);
    }

    [Fact]
    public void Equivalence_LoadBalancingAndServers_ParseMatchesBuilder()
    {
        var fromString = ClickHouseConnectionSettings.Parse(
            "Servers=a:9000,b:9000;LoadBalancing=Random");
        var fromBuilder = ClickHouseConnectionSettings.CreateBuilder()
            .WithServer("a", 9000)
            .WithServer("b", 9000)
            .WithLoadBalancing(LoadBalancingStrategy.Random)
            .Build();

        Assert.Equal(fromBuilder.LoadBalancing, fromString.LoadBalancing);
        Assert.Equal(
            fromBuilder.Servers.Select(s => (s.Host, s.Port)),
            fromString.Servers.Select(s => (s.Host, s.Port)));
    }

    [Fact]
    public void Equivalence_JwtToken_ParseMatchesBuilder()
    {
        var fromString = ClickHouseConnectionSettings.Parse("Host=h;Jwt=eyJhbGc.test.token");
        var fromBuilder = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("h")
            .WithJwt("eyJhbGc.test.token")
            .Build();

        Assert.Equal(fromBuilder.JwtToken, fromString.JwtToken);
        Assert.Equal(fromBuilder.AuthMethod, fromString.AuthMethod);
    }

    [Theory]
    [InlineData("Host=h;Port=abc", "Port", "abc", "1 and 65535")]
    [InlineData("Host=h;Compress=maybe", "Compress", "maybe", "true, false")]
    [InlineData("Host=h;LoadBalancing=Spiral", "LoadBalancing", "Spiral", "RoundRobin, Random, FirstAvailable")]
    [InlineData("Host=h;CompressionMethod=Snappy", "CompressionMethod", "Snappy", "Lz4, Zstd")]
    [InlineData("Host=h;Timeout=-5", "Timeout", "-5", "non-negative integer")]
    [InlineData("Host=h;TlsPort=99999", "TlsPort", "99999", "1 and 65535")]
    [InlineData("Host=h;UseTls=perhaps", "UseTls", "perhaps", "true, false")]
    [InlineData("Host=h;AllowInsecureTls=sortof", "AllowInsecureTls", "sortof", "true, false")]
    [InlineData("Host=h;StringMaterialization=Bionic", "StringMaterialization", "Bionic", "Eager, Lazy")]
    [InlineData("Host=h;MaxRetries=-1", "MaxRetries", "-1", "non-negative integer")]
    [InlineData("Host=h;CircuitBreakerThreshold=0", "CircuitBreakerThreshold", "0", "positive integer")]
    [InlineData("Host=h;HealthCheckInterval=-2", "HealthCheckInterval", "-2", "positive integer")]
    [InlineData("Host=h;UseSchemaCache=yesplease", "UseSchemaCache", "yesplease", "true, false")]
    public void MalformedValue_ParseThrows_ExceptionNamesKeyValueAndValidValues(
        string connectionString,
        string expectedKey,
        string expectedValue,
        string expectedValidValuesHint)
    {
        var ex = Assert.ThrowsAny<Exception>(
            () => ClickHouseConnectionSettings.Parse(connectionString));
        _output.WriteLine($"{connectionString} → {ex.GetType().Name}: {ex.Message}");

        // Every parser-rejection message must include all three pieces:
        // the original key (preserves case so the user can grep their connection
        // string), the offending value, and a "valid values" hint so the user
        // doesn't have to consult docs to fix the typo.
        Assert.Contains(expectedKey, ex.Message, StringComparison.Ordinal);
        Assert.Contains(expectedValue, ex.Message, StringComparison.Ordinal);
        Assert.Contains(expectedValidValuesHint, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void RoundTrip_ToStringThenParse_PreservesCoreFieldsThatToStringEmits()
    {
        // ToString deliberately omits Password, Roles, etc. for security.
        // Round-trip identity therefore covers Host/Port/Database/Username
        // and TLS triple; that's the documented contract.
        var original = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("h.example.com")
            .WithPort(9001)
            .WithDatabase("metrics")
            .WithCredentials("svc", "p1")
            .WithTls()
            .WithTlsPort(9440)
            .WithAllowInsecureTls()
            .Build();

        var serialised = original.ToString();
        var parsed = ClickHouseConnectionSettings.Parse(serialised);

        _output.WriteLine($"Round-trip: {serialised}");

        Assert.Equal(original.Host, parsed.Host);
        Assert.Equal(original.Port, parsed.Port);
        Assert.Equal(original.Database, parsed.Database);
        Assert.Equal(original.Username, parsed.Username);
        Assert.Equal(original.UseTls, parsed.UseTls);
        Assert.Equal(original.TlsPort, parsed.TlsPort);
        Assert.Equal(original.AllowInsecureTls, parsed.AllowInsecureTls);
    }

    [Fact]
    public void ServersAndHost_BothSpecified_ThrowsArgumentExceptionNamingBothKeys()
    {
        // Host= and Servers= have similar names but different semantics
        // (single endpoint vs. failover list). Silently merging them
        // masks misconfiguration ("I configured failover but it doesn't
        // fail over"), so the parser rejects the combination outright.
        var ex = Assert.Throws<ArgumentException>(() =>
            ClickHouseConnectionSettings.Parse(
                "Host=primary.example.com;Port=9000;Servers=a:9000,b:9000"));

        _output.WriteLine(ex.Message);
        Assert.Contains("Host", ex.Message, StringComparison.Ordinal);
        Assert.Contains("Servers", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ServersAndHost_OrderIndependent_BothOrderingsThrow()
    {
        // The conflict check must not depend on parse order: whichever
        // key is read second must still trip the same exception.
        var hostFirst = Assert.Throws<ArgumentException>(() =>
            ClickHouseConnectionSettings.Parse("Host=h;Servers=a:9000"));
        var serversFirst = Assert.Throws<ArgumentException>(() =>
            ClickHouseConnectionSettings.Parse("Servers=a:9000;Host=h"));

        Assert.Contains("Host", hostFirst.Message, StringComparison.Ordinal);
        Assert.Contains("Servers", hostFirst.Message, StringComparison.Ordinal);
        Assert.Contains("Host", serversFirst.Message, StringComparison.Ordinal);
        Assert.Contains("Servers", serversFirst.Message, StringComparison.Ordinal);
    }
}
