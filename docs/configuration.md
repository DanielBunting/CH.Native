# Configuration

CH.Native supports configuration via connection strings or a fluent builder API.

## Connection String Format

```
Host=localhost;Port=9000;Database=default;Username=default;Password=secret
```

### Connection String Options

| Key | Aliases | Type | Default | Description |
|-----|---------|------|---------|-------------|
| **Host** | Server | string | *(required)* | Server hostname or IP address |
| **Port** | - | int | 9000 | Native protocol port |
| **Database** | Db | string | default | Database name |
| **Username** | User, Uid | string | default | Authentication username |
| **Password** | Pwd | string | *(empty)* | Authentication password |
| **Timeout** | ConnectTimeout | int | 30 | Connection timeout in seconds |
| **Compress** | Compression | bool | true | Enable compression |
| **CompressionMethod** | - | enum | Lz4 | Lz4 or Zstd |
| **UseTls** | Tls, Ssl, Secure | bool | false | Enable TLS encryption |
| **TlsPort** | SslPort, SecurePort | int | 9440 | TLS port number |
| **AllowInsecureTls** | TrustServerCertificate, Insecure | bool | false | Skip certificate validation |
| **TlsCaCertificate** | SslCa, CaCert | string | - | Path to CA certificate file |
| **Servers** | - | string | - | Comma-separated host:port list |
| **LoadBalancing** | LoadBalancer | enum | RoundRobin | RoundRobin, Random, FirstAvailable |
| **MaxRetries** | Retries | int | 3 | Maximum retry attempts |
| **RetryBaseDelay** | RetryDelay | int | 100 | Base retry delay in milliseconds |
| **RetryMaxDelay** | - | int | 30000 | Maximum retry delay in milliseconds |
| **CircuitBreakerThreshold** | CbThreshold | int | 5 | Failures before circuit opens |
| **CircuitBreakerDuration** | CbDuration | int | 30 | Circuit open duration in seconds |
| **HealthCheckInterval** | - | int | 10 | Health check interval in seconds |

## Programmatic Configuration

Use `ClickHouseConnectionSettingsBuilder` for fluent configuration:

```csharp
using CH.Native.Connection;

var settings = ClickHouseConnectionSettings.CreateBuilder()
    .WithHost("localhost")
    .WithPort(9000)
    .WithDatabase("mydb")
    .WithCredentials("myuser", "mypassword")
    .WithCompression(true)
    .WithCompressionMethod(CompressionMethod.Lz4)
    .WithConnectTimeout(TimeSpan.FromSeconds(30))
    .Build();

await using var connection = new ClickHouseConnection(settings);
```

### Builder Methods

| Method | Description |
|--------|-------------|
| `WithHost(host)` | Set server hostname |
| `WithPort(port)` | Set server port |
| `WithDatabase(name)` | Set database name |
| `WithCredentials(user, password)` | Set authentication credentials |
| `WithUsername(user)` | Set username only |
| `WithPassword(password)` | Set password only |
| `WithConnectTimeout(timeout)` | Set connection timeout |
| `WithCompression(enabled)` | Enable/disable compression |
| `WithCompressionMethod(method)` | Set compression method (Lz4/Zstd) |
| `WithClientName(name)` | Set client name for server identification |
| `WithReceiveBufferSize(size)` | Set socket receive buffer size |
| `WithSendBufferSize(size)` | Set socket send buffer size |

## TLS/SSL Configuration

### Basic TLS

```csharp
// Connection string
var connStr = "Host=clickhouse.example.com;UseTls=true;TlsPort=9440";

// Builder
var settings = ClickHouseConnectionSettings.CreateBuilder()
    .WithHost("clickhouse.example.com")
    .WithTls(true)
    .WithTlsPort(9440)
    .Build();
```

### Custom CA Certificate

```csharp
var settings = ClickHouseConnectionSettings.CreateBuilder()
    .WithHost("clickhouse.example.com")
    .WithTls(true)
    .WithTlsCaCertificate("/path/to/ca.crt")
    .Build();
```

### Client Certificate (mTLS)

```csharp
var clientCert = new X509Certificate2("/path/to/client.pfx", "password");

var settings = ClickHouseConnectionSettings.CreateBuilder()
    .WithHost("clickhouse.example.com")
    .WithTls(true)
    .WithTlsClientCertificate(clientCert)
    .Build();
```

### Development Mode (Skip Validation)

```csharp
// WARNING: Only use for development/testing
var settings = ClickHouseConnectionSettings.CreateBuilder()
    .WithHost("localhost")
    .WithTls(true)
    .WithAllowInsecureTls(true) // Skips certificate validation
    .Build();
```

## Multi-Server Setup

Configure multiple servers for high availability:

### Connection String

```
Servers=host1:9000,host2:9000,host3:9000;LoadBalancing=RoundRobin
```

### Builder

```csharp
var settings = ClickHouseConnectionSettings.CreateBuilder()
    .WithServer("host1", 9000)
    .WithServer("host2", 9000)
    .WithServer("host3", 9000)
    .WithLoadBalancing(LoadBalancingStrategy.RoundRobin)
    .Build();
```

### Load Balancing Strategies

| Strategy | Description |
|----------|-------------|
| **RoundRobin** | Cycles through healthy servers in order |
| **Random** | Randomly selects a healthy server |
| **FirstAvailable** | Always uses the first healthy server |

## Resilience Configuration

Configure retry policies and circuit breakers via connection string or builder:

### Connection String

```
Host=localhost;MaxRetries=5;RetryBaseDelay=200;CircuitBreakerThreshold=10;CircuitBreakerDuration=60
```

### Builder

```csharp
var settings = ClickHouseConnectionSettings.CreateBuilder()
    .WithHost("localhost")
    .WithResilience(opts =>
    {
        opts.WithRetry(new RetryOptions
        {
            MaxRetries = 5,
            BaseDelay = TimeSpan.FromMilliseconds(200),
            MaxDelay = TimeSpan.FromSeconds(30),
            BackoffMultiplier = 2.0
        });
        opts.WithCircuitBreaker(new CircuitBreakerOptions
        {
            FailureThreshold = 10,
            OpenDuration = TimeSpan.FromSeconds(60)
        });
        opts.WithHealthCheckInterval(TimeSpan.FromSeconds(15));
    })
    .Build();
```

See [Resilience](resilience.md) for more details.

## Configuration Examples

### Development

```csharp
var settings = ClickHouseConnectionSettings.CreateBuilder()
    .WithHost("localhost")
    .WithPort(9000)
    .WithCompression(false) // Faster for small queries
    .Build();
```

### Production (Single Server)

```csharp
var settings = ClickHouseConnectionSettings.CreateBuilder()
    .WithHost("clickhouse.internal")
    .WithPort(9000)
    .WithDatabase("production")
    .WithCredentials("app_user", Environment.GetEnvironmentVariable("CH_PASSWORD")!)
    .WithCompression(true)
    .WithCompressionMethod(CompressionMethod.Lz4)
    .WithConnectTimeout(TimeSpan.FromSeconds(10))
    .WithResilience(ResilienceOptions.WithAllDefaults())
    .WithTelemetry(TelemetrySettings.Default)
    .Build();
```

### High Availability

```csharp
var settings = ClickHouseConnectionSettings.CreateBuilder()
    .WithServer("ch-node1.internal", 9000)
    .WithServer("ch-node2.internal", 9000)
    .WithServer("ch-node3.internal", 9000)
    .WithLoadBalancing(LoadBalancingStrategy.RoundRobin)
    .WithDatabase("production")
    .WithCredentials("app_user", Environment.GetEnvironmentVariable("CH_PASSWORD")!)
    .WithResilience(opts =>
    {
        opts.WithRetry(new RetryOptions { MaxRetries = 3 });
        opts.WithCircuitBreaker();
        opts.WithHealthCheckInterval(TimeSpan.FromSeconds(10));
    })
    .Build();
```

### Secure Connection

```csharp
var settings = ClickHouseConnectionSettings.CreateBuilder()
    .WithHost("clickhouse.example.com")
    .WithTls(true)
    .WithTlsPort(9440)
    .WithTlsCaCertificate("/etc/ssl/certs/clickhouse-ca.crt")
    .WithCredentials("secure_user", Environment.GetEnvironmentVariable("CH_PASSWORD")!)
    .Build();
```

## See Also

- [Quick Start](quickstart.md)
- [Resilience](resilience.md) - Detailed retry and circuit breaker configuration
- [Telemetry](telemetry.md) - Observability configuration
