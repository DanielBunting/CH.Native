# Resilience

CH.Native includes production-ready resilience patterns: retry policies, circuit breakers, health checking, and load balancing.

## Overview

Resilience features help your application handle transient failures gracefully:

- **Retry Policy** - Automatically retry failed operations with exponential backoff
- **Circuit Breaker** - Stop sending requests to failing servers to allow recovery
- **Health Checking** - Background monitoring of server health
- **Load Balancing** - Distribute requests across multiple servers

## Quick Setup

Enable all resilience features with defaults:

```csharp
var settings = ClickHouseConnectionSettings.CreateBuilder()
    .WithHost("clickhouse.internal")
    .WithResilience(ResilienceOptions.WithAllDefaults())
    .Build();
```

Or via connection string:

```
Host=localhost;MaxRetries=3;CircuitBreakerThreshold=5
```

## Retry Policy

Automatically retry failed operations with exponential backoff and jitter.

### Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| MaxRetries | int | 3 | Maximum retry attempts |
| BaseDelay | TimeSpan | 100ms | Initial delay between retries |
| BackoffMultiplier | double | 2.0 | Exponential backoff multiplier |
| MaxDelay | TimeSpan | 30s | Maximum delay between retries |
| ShouldRetry | Func | null | Custom retry predicate |

### Example

```csharp
var settings = ClickHouseConnectionSettings.CreateBuilder()
    .WithHost("localhost")
    .WithResilience(opts =>
    {
        opts.WithRetry(new RetryOptions
        {
            MaxRetries = 5,
            BaseDelay = TimeSpan.FromMilliseconds(200),
            BackoffMultiplier = 2.0,
            MaxDelay = TimeSpan.FromSeconds(30)
        });
    })
    .Build();
```

### Exponential Backoff

Delays increase exponentially between retries:

| Attempt | Delay (base=100ms, multiplier=2.0) |
|---------|-----------------------------------|
| 1 | 100ms |
| 2 | 200ms |
| 3 | 400ms |
| 4 | 800ms |
| 5 | 1600ms |

A random jitter of 0-25% is added to prevent thundering herd problems.

### Transient Errors

The retry policy automatically identifies transient errors that are worth retrying:

**Exception Types:**
- `SocketException`
- `TimeoutException`
- `IOException`
- `ClickHouseConnectionException`

**ClickHouse Error Codes:**
- 159: TIMEOUT_EXCEEDED
- 164: READONLY
- 209: SOCKET_TIMEOUT
- 210: NETWORK_ERROR
- 242: TOO_MANY_SIMULTANEOUS_QUERIES
- 252: TOO_SLOW

### Custom Retry Logic

```csharp
var retryOptions = new RetryOptions
{
    MaxRetries = 3,
    ShouldRetry = ex =>
    {
        // Custom logic to determine if we should retry
        if (ex is ClickHouseServerException serverEx)
        {
            return serverEx.ErrorCode == 242; // Only retry TOO_MANY_SIMULTANEOUS_QUERIES
        }
        return false;
    }
};
```

### Retry Events

Monitor retry attempts:

```csharp
var retryPolicy = new RetryPolicy(options);
retryPolicy.OnRetry += (sender, args) =>
{
    Console.WriteLine($"Retry {args.Attempt} after {args.Delay}: {args.Exception.Message}");
};
```

## Circuit Breaker

Prevents cascading failures by stopping requests to unhealthy servers.

### Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| FailureThreshold | int | 5 | Failures before circuit opens |
| OpenDuration | TimeSpan | 30s | How long circuit stays open |
| FailureWindow | TimeSpan | 1min | Time window for counting failures |

### States

```
   [Closed] ──(failures >= threshold)──> [Open]
      ↑                                     │
      │                                     │
(success)                          (after OpenDuration)
      │                                     │
      └──────── [Half-Open] <───────────────┘
                    │
              (failure) ──> [Open]
```

| State | Behavior |
|-------|----------|
| **Closed** | Normal operation. Requests pass through. Failures are counted. |
| **Open** | Circuit tripped. Requests fail immediately without attempting. |
| **Half-Open** | Testing recovery. One request allowed through. Success closes circuit, failure reopens it. |

### Example

```csharp
var settings = ClickHouseConnectionSettings.CreateBuilder()
    .WithHost("localhost")
    .WithResilience(opts =>
    {
        opts.WithCircuitBreaker(new CircuitBreakerOptions
        {
            FailureThreshold = 5,
            OpenDuration = TimeSpan.FromSeconds(30),
            FailureWindow = TimeSpan.FromMinutes(1)
        });
    })
    .Build();
```

### Circuit Breaker Events

Monitor state transitions:

```csharp
var circuitBreaker = new CircuitBreaker(options);
circuitBreaker.OnStateChanged += (sender, args) =>
{
    Console.WriteLine($"Circuit breaker: {args.OldState} -> {args.NewState}");
};
```

## Load Balancing

Distribute requests across multiple ClickHouse servers.

### Strategies

| Strategy | Description | Use Case |
|----------|-------------|----------|
| **RoundRobin** | Cycles through healthy servers | Even distribution |
| **Random** | Random healthy server selection | Simple load distribution |
| **FirstAvailable** | Always uses first healthy server | Primary/failover setup |

### Multi-Server Configuration

```csharp
var settings = ClickHouseConnectionSettings.CreateBuilder()
    .WithServer("ch-node1.internal", 9000)
    .WithServer("ch-node2.internal", 9000)
    .WithServer("ch-node3.internal", 9000)
    .WithLoadBalancing(LoadBalancingStrategy.RoundRobin)
    .Build();
```

Or via connection string:

```
Servers=ch-node1:9000,ch-node2:9000,ch-node3:9000;LoadBalancing=RoundRobin
```

### Server Health Tracking

Servers are automatically marked unhealthy after consecutive failures:

- After 3 consecutive failures, server is marked unhealthy
- Unhealthy servers are excluded from load balancing
- Health checks restore servers to healthy status

## Health Checking

Background monitoring of server health.

### Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| HealthCheckInterval | TimeSpan | 10s | Interval between health checks |

### Example

```csharp
var settings = ClickHouseConnectionSettings.CreateBuilder()
    .WithServer("ch-node1.internal", 9000)
    .WithServer("ch-node2.internal", 9000)
    .WithResilience(opts =>
    {
        opts.WithHealthCheckInterval(TimeSpan.FromSeconds(15));
    })
    .Build();
```

Health checks execute `SELECT 1` against each server to verify connectivity.

## Using ResilientConnection

For advanced scenarios, use `ResilientConnection` directly:

```csharp
using CH.Native.Resilience;

var settings = ClickHouseConnectionSettings.CreateBuilder()
    .WithServer("ch-node1.internal", 9000)
    .WithServer("ch-node2.internal", 9000)
    .WithLoadBalancing(LoadBalancingStrategy.RoundRobin)
    .WithResilience(ResilienceOptions.WithAllDefaults())
    .Build();

await using var connection = new ResilientConnection(settings);
var result = await connection.ExecuteScalarAsync<int>("SELECT 1");
```

## Production Configuration

Recommended settings for production:

```csharp
var settings = ClickHouseConnectionSettings.CreateBuilder()
    // Multiple servers for HA
    .WithServer("ch-node1.internal", 9000)
    .WithServer("ch-node2.internal", 9000)
    .WithServer("ch-node3.internal", 9000)
    .WithLoadBalancing(LoadBalancingStrategy.RoundRobin)

    // Resilience
    .WithResilience(opts =>
    {
        // Retry transient failures
        opts.WithRetry(new RetryOptions
        {
            MaxRetries = 3,
            BaseDelay = TimeSpan.FromMilliseconds(100),
            MaxDelay = TimeSpan.FromSeconds(10)
        });

        // Circuit breaker for failing servers
        opts.WithCircuitBreaker(new CircuitBreakerOptions
        {
            FailureThreshold = 5,
            OpenDuration = TimeSpan.FromSeconds(30)
        });

        // Health monitoring
        opts.WithHealthCheckInterval(TimeSpan.FromSeconds(10));
    })

    // Other settings
    .WithDatabase("production")
    .WithCredentials("app_user", Environment.GetEnvironmentVariable("CH_PASSWORD")!)
    .WithConnectTimeout(TimeSpan.FromSeconds(10))
    .Build();
```

## Metrics

Resilience events are recorded as metrics when telemetry is enabled:

- `ch.native.retries` - Retry attempts
- `ch.native.circuit_breaker.transitions` - Circuit breaker state changes

See [Telemetry](telemetry.md) for more details.

## See Also

- [Configuration](configuration.md) - Connection settings
- [Telemetry](telemetry.md) - Monitoring and observability
