using CH.Native.Compression;
using CH.Native.Connection;
using CH.Native.Data;
using Xunit;

namespace CH.Native.Tests.Unit.Connection;

public class ClickHouseConnectionSettingsTests
{
    [Fact]
    public void Parse_MinimalConnectionString_UsesDefaults()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost");

        Assert.Equal("localhost", settings.Host);
        Assert.Equal(9000, settings.Port);
        Assert.Equal("default", settings.Database);
        Assert.Equal("default", settings.Username);
        Assert.Equal("", settings.Password);
    }

    [Fact]
    public void Parse_FullConnectionString_ParsesAllValues()
    {
        var settings = ClickHouseConnectionSettings.Parse(
            "Host=clickhouse.example.com;Port=9001;Database=mydb;Username=admin;Password=secret");

        Assert.Equal("clickhouse.example.com", settings.Host);
        Assert.Equal(9001, settings.Port);
        Assert.Equal("mydb", settings.Database);
        Assert.Equal("admin", settings.Username);
        Assert.Equal("secret", settings.Password);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void Parse_EmptyOrWhitespace_ThrowsArgumentException(string connectionString)
    {
        Assert.Throws<ArgumentException>(() =>
            ClickHouseConnectionSettings.Parse(connectionString));
    }

    [Fact]
    public void Parse_Null_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() =>
            ClickHouseConnectionSettings.Parse(null!));
    }

    [Fact]
    public void Parse_MissingHost_ThrowsInvalidOperationException()
    {
        Assert.Throws<InvalidOperationException>(() =>
            ClickHouseConnectionSettings.Parse("Port=9000"));
    }

    [Theory]
    [InlineData("Host=localhost;Port=0")]
    [InlineData("Host=localhost;Port=-1")]
    [InlineData("Host=localhost;Port=65536")]
    [InlineData("Host=localhost;Port=abc")]
    public void Parse_InvalidPort_ThrowsArgumentException(string connectionString)
    {
        Assert.Throws<ArgumentException>(() =>
            ClickHouseConnectionSettings.Parse(connectionString));
    }

    [Fact]
    public void Parse_CaseInsensitiveKeys()
    {
        var settings = ClickHouseConnectionSettings.Parse(
            "HOST=localhost;PORT=9001;DATABASE=mydb");

        Assert.Equal("localhost", settings.Host);
        Assert.Equal(9001, settings.Port);
        Assert.Equal("mydb", settings.Database);
    }

    [Fact]
    public void Parse_WhitespaceAround_IsTrimmed()
    {
        var settings = ClickHouseConnectionSettings.Parse(
            " Host = localhost ; Port = 9000 ");

        Assert.Equal("localhost", settings.Host);
        Assert.Equal(9000, settings.Port);
    }

    [Fact]
    public void Parse_ServerAlias_MapsToHost()
    {
        var settings = ClickHouseConnectionSettings.Parse("Server=localhost");
        Assert.Equal("localhost", settings.Host);
    }

    [Fact]
    public void Parse_UserAlias_MapsToUsername()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;User=admin");
        Assert.Equal("admin", settings.Username);
    }

    [Fact]
    public void Parse_DbAlias_MapsToDatabase()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;Db=mydb");
        Assert.Equal("mydb", settings.Database);
    }

    [Fact]
    public void Parse_PwdAlias_MapsToPassword()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;Pwd=secret");
        Assert.Equal("secret", settings.Password);
    }

    [Fact]
    public void Parse_EmptyPassword_AllowsEmpty()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;Password=");
        Assert.Equal("", settings.Password);
    }

    [Fact]
    public void Builder_CreatesValidSettings()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("clickhouse.example.com")
            .WithPort(9001)
            .WithDatabase("mydb")
            .WithCredentials("admin", "secret")
            .Build();

        Assert.Equal("clickhouse.example.com", settings.Host);
        Assert.Equal(9001, settings.Port);
        Assert.Equal("mydb", settings.Database);
        Assert.Equal("admin", settings.Username);
        Assert.Equal("secret", settings.Password);
    }

    [Fact]
    public void Builder_WithDefaults_UsesDefaultValues()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .Build();

        Assert.Equal("localhost", settings.Host);
        Assert.Equal(9000, settings.Port);
        Assert.Equal("default", settings.Database);
        Assert.Equal("default", settings.Username);
        Assert.Equal("", settings.Password);
        Assert.Equal(TimeSpan.FromSeconds(10), settings.ConnectTimeout);
    }

    [Fact]
    public void Builder_WithTimeout_SetsTimeout()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithConnectTimeout(TimeSpan.FromSeconds(30))
            .Build();

        Assert.Equal(TimeSpan.FromSeconds(30), settings.ConnectTimeout);
    }

    [Fact]
    public void Builder_WithClientName_SetsClientName()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithClientName("MyApp")
            .Build();

        Assert.Equal("MyApp", settings.ClientName);
    }

    [Fact]
    public void Builder_InvalidPort_ThrowsArgumentOutOfRangeException()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            ClickHouseConnectionSettings.CreateBuilder()
                .WithPort(0));

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            ClickHouseConnectionSettings.CreateBuilder()
                .WithPort(65536));
    }

    [Fact]
    public void Builder_NullHost_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            ClickHouseConnectionSettings.CreateBuilder()
                .WithHost(null!));
    }

    [Fact]
    public void Builder_MissingHost_ThrowsInvalidOperationException()
    {
        Assert.Throws<InvalidOperationException>(() =>
            ClickHouseConnectionSettings.CreateBuilder()
                .WithHost("")
                .Build());
    }

    [Fact]
    public void ToString_ReturnsConnectionStringWithoutPassword()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithPort(9001)
            .WithDatabase("mydb")
            .WithCredentials("admin", "secret")
            .Build();

        var str = settings.ToString();

        Assert.Contains("Host=localhost", str);
        Assert.Contains("Port=9001", str);
        Assert.Contains("Database=mydb", str);
        Assert.Contains("Username=admin", str);
        Assert.DoesNotContain("secret", str);
    }

    [Fact]
    public void Builder_WithCompression_EnablesCompression()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithCompression(true)
            .Build();

        Assert.True(settings.Compress);
    }

    [Fact]
    public void Builder_WithoutCompression_CompressionEnabledByDefault()
    {
        // Compression is enabled by default for better performance (27-35x faster for large result sets)
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .Build();

        Assert.True(settings.Compress);
    }

    [Fact]
    public void Builder_WithCompressionMethod_SetsMethod()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithCompressionMethod(CompressionMethod.Zstd)
            .Build();

        Assert.Equal(CompressionMethod.Zstd, settings.CompressionMethod);
    }

    [Fact]
    public void Builder_DefaultCompressionMethod_IsLz4()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .Build();

        Assert.Equal(CompressionMethod.Lz4, settings.CompressionMethod);
    }

    [Fact]
    public void Parse_WithCompressTrue_EnablesCompression()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;Compress=true");

        Assert.True(settings.Compress);
    }

    [Fact]
    public void Parse_WithCompressFalse_DisablesCompression()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;Compress=false");

        Assert.False(settings.Compress);
    }

    [Fact]
    public void Parse_WithCompress1_EnablesCompression()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;Compress=1");

        Assert.True(settings.Compress);
    }

    [Fact]
    public void Parse_WithCompress0_DisablesCompression()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;Compress=0");

        Assert.False(settings.Compress);
    }

    [Fact]
    public void Parse_WithCompressionAlias_EnablesCompression()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;Compression=true");

        Assert.True(settings.Compress);
    }

    [Fact]
    public void Parse_WithCompressionMethodLz4_SetsLz4()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;CompressionMethod=lz4");

        Assert.Equal(CompressionMethod.Lz4, settings.CompressionMethod);
    }

    [Fact]
    public void Parse_WithCompressionMethodZstd_SetsZstd()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;CompressionMethod=zstd");

        Assert.Equal(CompressionMethod.Zstd, settings.CompressionMethod);
    }

    [Fact]
    public void Parse_WithCompressionMethodCaseInsensitive_Works()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;CompressionMethod=ZSTD");

        Assert.Equal(CompressionMethod.Zstd, settings.CompressionMethod);
    }

    [Fact]
    public void Parse_WithInvalidCompressionMethod_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() =>
            ClickHouseConnectionSettings.Parse("Host=localhost;CompressionMethod=invalid"));
    }

    [Fact]
    public void Parse_WithInvalidCompressValue_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() =>
            ClickHouseConnectionSettings.Parse("Host=localhost;Compress=maybe"));
    }

    // TLS Settings Tests

    [Fact]
    public void Builder_WithTls_EnablesTls()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithTls()
            .Build();

        Assert.True(settings.UseTls);
        Assert.Equal(9440, settings.TlsPort);
    }

    [Fact]
    public void Builder_WithoutTls_TlsDisabledByDefault()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .Build();

        Assert.False(settings.UseTls);
    }

    [Fact]
    public void Builder_WithTlsPort_SetsCustomPort()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithTls()
            .WithTlsPort(9443)
            .Build();

        Assert.True(settings.UseTls);
        Assert.Equal(9443, settings.TlsPort);
    }

    [Fact]
    public void Builder_WithAllowInsecureTls_AllowsInsecure()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithTls()
            .WithAllowInsecureTls()
            .Build();

        Assert.True(settings.AllowInsecureTls);
    }

    [Fact]
    public void Builder_DefaultAllowInsecureTls_IsFalse()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithTls()
            .Build();

        Assert.False(settings.AllowInsecureTls);
    }

    [Fact]
    public void Builder_WithTlsCaCertificate_SetsPath()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithTls()
            .WithTlsCaCertificate("/path/to/ca.crt")
            .Build();

        Assert.Equal("/path/to/ca.crt", settings.TlsCaCertificatePath);
    }

    [Fact]
    public void Builder_InvalidTlsPort_ThrowsArgumentOutOfRangeException()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            ClickHouseConnectionSettings.CreateBuilder()
                .WithTlsPort(0));

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            ClickHouseConnectionSettings.CreateBuilder()
                .WithTlsPort(65536));
    }

    [Fact]
    public void EffectivePort_WhenTlsEnabled_ReturnsTlsPort()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithPort(9000)
            .WithTls()
            .WithTlsPort(9440)
            .Build();

        Assert.Equal(9440, settings.EffectivePort);
    }

    [Fact]
    public void EffectivePort_WhenTlsDisabled_ReturnsPort()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithPort(9000)
            .Build();

        Assert.Equal(9000, settings.EffectivePort);
    }

    [Fact]
    public void Parse_WithUseTlsTrue_EnablesTls()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;UseTls=true");

        Assert.True(settings.UseTls);
    }

    [Fact]
    public void Parse_WithUseTlsFalse_DisablesTls()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;UseTls=false");

        Assert.False(settings.UseTls);
    }

    [Fact]
    public void Parse_WithTlsAlias_EnablesTls()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;Tls=true");

        Assert.True(settings.UseTls);
    }

    [Fact]
    public void Parse_WithSslAlias_EnablesTls()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;Ssl=true");

        Assert.True(settings.UseTls);
    }

    [Fact]
    public void Parse_WithSecureAlias_EnablesTls()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;Secure=1");

        Assert.True(settings.UseTls);
    }

    [Fact]
    public void Parse_WithTlsPort_SetsPort()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;UseTls=true;TlsPort=9443");

        Assert.Equal(9443, settings.TlsPort);
    }

    [Fact]
    public void Parse_WithSslPortAlias_SetsPort()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;SslPort=9443");

        Assert.Equal(9443, settings.TlsPort);
    }

    [Fact]
    public void Parse_WithAllowInsecureTls_AllowsInsecure()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;UseTls=true;AllowInsecureTls=true");

        Assert.True(settings.AllowInsecureTls);
    }

    [Fact]
    public void Parse_WithTrustServerCertificateAlias_AllowsInsecure()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;TrustServerCertificate=true");

        Assert.True(settings.AllowInsecureTls);
    }

    [Fact]
    public void Parse_WithTlsCaCertificate_SetsPath()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;TlsCaCertificate=/path/to/ca.crt");

        Assert.Equal("/path/to/ca.crt", settings.TlsCaCertificatePath);
    }

    [Fact]
    public void Parse_WithSslCaAlias_SetsPath()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;SslCa=/path/to/ca.crt");

        Assert.Equal("/path/to/ca.crt", settings.TlsCaCertificatePath);
    }

    [Theory]
    [InlineData("Host=localhost;TlsPort=0")]
    [InlineData("Host=localhost;TlsPort=-1")]
    [InlineData("Host=localhost;TlsPort=65536")]
    [InlineData("Host=localhost;TlsPort=abc")]
    public void Parse_InvalidTlsPort_ThrowsArgumentException(string connectionString)
    {
        Assert.Throws<ArgumentException>(() =>
            ClickHouseConnectionSettings.Parse(connectionString));
    }

    [Fact]
    public void Parse_WithInvalidUseTlsValue_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() =>
            ClickHouseConnectionSettings.Parse("Host=localhost;UseTls=maybe"));
    }

    [Fact]
    public void ToString_WithTls_IncludesTlsInfo()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithTls()
            .WithTlsPort(9440)
            .Build();

        var str = settings.ToString();

        Assert.Contains("UseTls=true", str);
        Assert.Contains("TlsPort=9440", str);
    }

    [Fact]
    public void ToString_WithTlsAndInsecure_IncludesInsecureInfo()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithTls()
            .WithAllowInsecureTls()
            .Build();

        var str = settings.ToString();

        Assert.Contains("AllowInsecureTls=true", str);
    }

    [Fact]
    public void ToString_WithoutTls_OmitsTlsInfo()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .Build();

        var str = settings.ToString();

        Assert.DoesNotContain("UseTls", str);
        Assert.DoesNotContain("TlsPort", str);
    }

    // String Materialization Tests

    [Fact]
    public void Builder_DefaultStringMaterialization_IsEager()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .Build();

        Assert.Equal(StringMaterialization.Eager, settings.StringMaterialization);
    }

    [Fact]
    public void Builder_WithStringMaterialization_SetsValue()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithStringMaterialization(StringMaterialization.Lazy)
            .Build();

        Assert.Equal(StringMaterialization.Lazy, settings.StringMaterialization);
    }

    [Fact]
    public void Parse_WithStringMaterializationLazy_SetsLazy()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;StringMaterialization=Lazy");

        Assert.Equal(StringMaterialization.Lazy, settings.StringMaterialization);
    }

    [Fact]
    public void Parse_WithStringMaterializationEager_SetsEager()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;StringMaterialization=Eager");

        Assert.Equal(StringMaterialization.Eager, settings.StringMaterialization);
    }

    [Fact]
    public void Parse_WithStringMaterializationCaseInsensitive_Works()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;StringMaterialization=lazy");

        Assert.Equal(StringMaterialization.Lazy, settings.StringMaterialization);
    }

    [Fact]
    public void Parse_WithInvalidStringMaterialization_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() =>
            ClickHouseConnectionSettings.Parse("Host=localhost;StringMaterialization=invalid"));
    }

    [Fact]
    public void Parse_WithoutStringMaterialization_DefaultsToEager()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost");

        Assert.Equal(StringMaterialization.Eager, settings.StringMaterialization);
    }
}
