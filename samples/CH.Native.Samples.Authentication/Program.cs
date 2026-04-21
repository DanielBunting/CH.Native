using System.Security.Cryptography.X509Certificates;
using CH.Native.Connection;

if (args.Length == 0 || args[0] is "-h" or "--help")
{
    PrintUsage();
    return 0;
}

var host = Environment.GetEnvironmentVariable("CH_HOST") ?? "localhost";
var port = int.TryParse(Environment.GetEnvironmentVariable("CH_PORT"), out var p) ? p : 9000;
var tlsPort = int.TryParse(Environment.GetEnvironmentVariable("CH_TLS_PORT"), out var tp) ? tp : 9440;

try
{
    var settings = args[0].ToLowerInvariant() switch
    {
        "password" => BuildPassword(args),
        "jwt"      => BuildJwt(args),
        "ssh"      => BuildSsh(args),
        "cert"     => BuildCert(args),
        _          => throw new ArgumentException($"unknown auth type: {args[0]}")
    };

    await using var connection = new ClickHouseConnection(settings);
    await connection.OpenAsync();

    var user = await connection.ExecuteScalarAsync<string>("SELECT currentUser()");
    var version = await connection.ExecuteScalarAsync<string>("SELECT version()");

    Console.WriteLine($"[{args[0]}] connected as '{user}' to ClickHouse {version}");
    return 0;
}
catch (Exception ex)
{
    Console.Error.WriteLine($"[{args[0]}] failed: {ex.GetType().Name}: {ex.Message}");
    return 1;
}

ClickHouseConnectionSettings BuildPassword(string[] args)
{
    // USAGE: password <user> <password>
    var user = args.ElementAtOrDefault(1) ?? "default";
    var pass = args.ElementAtOrDefault(2) ?? "";
    return ClickHouseConnectionSettings.CreateBuilder()
        .WithHost(host).WithPort(port)
        .WithUsername(user)
        .WithPassword(pass)
        .Build();
}

ClickHouseConnectionSettings BuildJwt(string[] args)
{
    // USAGE: jwt <token>
    // NOTE: OSS ClickHouse rejects JWT with "JWT is available only in ClickHouse Cloud".
    // Use against a Cloud endpoint or a build that supports JWT validation.
    var token = args.ElementAtOrDefault(1)
        ?? throw new ArgumentException("jwt: expected <token> argument");
    return ClickHouseConnectionSettings.CreateBuilder()
        .WithHost(host).WithPort(port)
        .WithJwt(token)
        .Build();
}

ClickHouseConnectionSettings BuildSsh(string[] args)
{
    // USAGE: ssh <user> <private-key-path> [passphrase]
    // Requires ClickHouse server >= 23.9 (protocol revision >= 54466) and the user
    // configured with an <ssh_keys><ssh_key><base64_key>...</base64_key></ssh_key></ssh_keys>
    // entry in users.xml / users.d/*.xml.
    var user = args.ElementAtOrDefault(1)
        ?? throw new ArgumentException("ssh: expected <user> argument");
    var keyPath = args.ElementAtOrDefault(2)
        ?? throw new ArgumentException("ssh: expected <private-key-path> argument");
    var passphrase = args.ElementAtOrDefault(3);

    return ClickHouseConnectionSettings.CreateBuilder()
        .WithHost(host).WithPort(port)
        .WithUsername(user)
        .WithSshKeyPath(keyPath, passphrase)
        .Build();
}

ClickHouseConnectionSettings BuildCert(string[] args)
{
    // USAGE: cert <user> <pfx-path> <pfx-password> [--insecure]
    // Requires the server to be configured with TLS and the user's
    // <ssl_certificates><common_name>...</common_name></ssl_certificates>
    // matching the cert's CN.
    var user = args.ElementAtOrDefault(1)
        ?? throw new ArgumentException("cert: expected <user> argument");
    var pfxPath = args.ElementAtOrDefault(2)
        ?? throw new ArgumentException("cert: expected <pfx-path> argument");
    var pfxPassword = args.ElementAtOrDefault(3)
        ?? throw new ArgumentException("cert: expected <pfx-password> argument");
    var allowInsecure = args.Contains("--insecure");

    var clientCert = new X509Certificate2(pfxPath, pfxPassword);

    var builder = ClickHouseConnectionSettings.CreateBuilder()
        .WithHost(host)
        .WithTls().WithTlsPort(tlsPort)
        .WithUsername(user)
        .WithTlsClientCertificate(clientCert)
        .WithAuthMethod(ClickHouseAuthMethod.TlsClientCertificate);

    if (allowInsecure) builder.WithAllowInsecureTls();

    return builder.Build();
}

static void PrintUsage()
{
    Console.WriteLine("""
        CH.Native authentication sample — demonstrates all four auth methods.

        Environment:
          CH_HOST       host (default: localhost)
          CH_PORT       native port (default: 9000)
          CH_TLS_PORT   secure native port (default: 9440)

        Usage:
          dotnet run -- password <user> <password>
          dotnet run -- jwt <token>
          dotnet run -- ssh <user> <private-key-path> [passphrase]
          dotnet run -- cert <user> <pfx-path> <pfx-password> [--insecure]

        Examples:
          dotnet run -- password default ""
          dotnet run -- jwt eyJhbGciOiJIUzI1NiJ9.e30.sig          # needs CH Cloud
          dotnet run -- ssh ssh_user  ~/.ssh/ch_rsa
          dotnet run -- cert cert_user ./client.pfx testpass --insecure

        Each run opens a connection, runs SELECT currentUser() + SELECT version(),
        and prints the principal the server resolved. See CLAUDE.md for the
        server-side users.xml config required for each auth method.
        """);
}
