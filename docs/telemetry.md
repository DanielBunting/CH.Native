# Telemetry

CH.Native includes built-in observability features compatible with OpenTelemetry for tracing, metrics, and logging.

## Overview

| Feature | Implementation | Identifier |
|---------|---------------|------------|
| Distributed Tracing | System.Diagnostics.ActivitySource | `CH.Native` |
| Metrics | System.Diagnostics.Metrics.Meter | `CH.Native` |
| Logging | Microsoft.Extensions.Logging | ILogger |

## Quick Setup

Enable all telemetry with default settings:

```csharp
var settings = ClickHouseConnectionSettings.CreateBuilder()
    .WithHost("localhost")
    .WithTelemetry(TelemetrySettings.Default)
    .Build();
```

## Configuration

### TelemetrySettings

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| EnableTracing | bool | true | Enable distributed tracing |
| EnableMetrics | bool | true | Enable metrics collection |
| IncludeSqlInTraces | bool | true | Include SQL in trace spans (sanitized) |
| LoggerFactory | ILoggerFactory? | null | Logger factory for structured logging |

### Custom Configuration

```csharp
var telemetry = new TelemetrySettings
{
    EnableTracing = true,
    EnableMetrics = true,
    IncludeSqlInTraces = false, // Don't include SQL in traces
};

var settings = ClickHouseConnectionSettings.CreateBuilder()
    .WithHost("localhost")
    .WithTelemetry(telemetry)
    .Build();
```

## Distributed Tracing

CH.Native creates spans for database operations using `System.Diagnostics.ActivitySource`.

### Activity Source

- **Name:** `CH.Native`
- **Operations:** `clickhouse.query`, `clickhouse.connect`, `clickhouse.bulk_insert`, `clickhouse.cancel`

### Span Attributes

| Attribute | Description |
|-----------|-------------|
| `db.system` | `clickhouse` |
| `db.name` | Database name |
| `db.statement` | SQL query (if enabled, sanitized) |
| `db.operation` | Operation type (query, insert, etc.) |
| `server.address` | Server hostname |
| `server.port` | Server port |
| `db.response.rows` | Number of rows returned |

### OpenTelemetry Integration

```csharp
using OpenTelemetry;
using OpenTelemetry.Trace;

// Configure OpenTelemetry to collect CH.Native traces
var tracerProvider = Sdk.CreateTracerProviderBuilder()
    .AddSource("CH.Native") // Subscribe to CH.Native activities
    .AddConsoleExporter()
    .AddOtlpExporter()
    .Build();

// Use CH.Native as normal - traces are collected automatically
var settings = ClickHouseConnectionSettings.CreateBuilder()
    .WithHost("localhost")
    .WithTelemetry(TelemetrySettings.Default)
    .Build();

await using var connection = new ClickHouseConnection(settings);
await connection.OpenAsync();
var result = await connection.ExecuteScalarAsync<int>("SELECT 1");
```

### SQL Sanitization

When `IncludeSqlInTraces` is enabled, SQL statements are sanitized to remove literal values:

```sql
-- Original
SELECT * FROM users WHERE id = 123 AND name = 'Alice'

-- Sanitized (in trace)
SELECT * FROM users WHERE id = ? AND name = ?
```

## Metrics

CH.Native records metrics using `System.Diagnostics.Metrics.Meter`.

### Meter

- **Name:** `CH.Native`

### Available Metrics

Instrument names follow OpenTelemetry/Prometheus snake_case conventions; `Counter` instruments end in `_total`. Durations are recorded in seconds (OTel-canonical `s`).

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `ch_native_queries_total` | Counter | `{queries}` | Total queries executed |
| `ch_native_query_duration` | Histogram | `s` | Query execution duration |
| `ch_native_rows_read_total` | Counter | `{rows}` | Total rows read from server |
| `ch_native_rows_written_total` | Counter | `{rows}` | Total rows written to server |
| `ch_native_bytes_sent_total` | Counter | `By` | Bytes sent to server |
| `ch_native_bytes_received_total` | Counter | `By` | Bytes received from server |
| `ch_native_errors_total` | Counter | `{errors}` | Total errors |
| `ch_native_connect_duration` | Histogram | `s` | Connection establishment duration |
| `ch_native_connections_active` | ObservableGauge | `{connections}` | Currently active connections |
| `ch_native_retry_attempts_total` | Counter | `{attempts}` | Retry attempts (tagged with `attempt`, `error.type`) |
| `ch_native_retry_delay` | Histogram | `s` | Delay before retry attempt |
| `ch_native_circuit_breaker_state_changes_total` | Counter | `{changes}` | Circuit breaker state transitions |
| `ch_native_circuit_breaker_state` | ObservableGauge | `{state}` | Current state per server (0=Closed, 1=HalfOpen, 2=Open) |

Tags emitted on query metrics: `db.name`, `status` (`success` / `error`). Retry metrics bucket the triggering exception into a fixed `error.type` set (`network`, `timeout`, `server`, `cancelled`, `client`, `other`) to bound series cardinality. Circuit-breaker metrics carry `server.address`.

### OpenTelemetry Integration

```csharp
using OpenTelemetry;
using OpenTelemetry.Metrics;

var meterProvider = Sdk.CreateMeterProviderBuilder()
    .AddMeter("CH.Native") // Subscribe to CH.Native metrics
    .AddConsoleExporter()
    .AddPrometheusExporter()
    .Build();
```

### Prometheus Example

With OpenTelemetry Prometheus exporter (durations are seconds):

```
# HELP ch_native_queries_total Total queries executed
# TYPE ch_native_queries_total counter
ch_native_queries_total{db_name="default",status="success"} 1234

# HELP ch_native_query_duration_seconds Query execution duration
# TYPE ch_native_query_duration_seconds histogram
ch_native_query_duration_seconds_bucket{le="0.01"} 500
ch_native_query_duration_seconds_bucket{le="0.05"} 900
ch_native_query_duration_seconds_bucket{le="0.1"} 1100
ch_native_query_duration_seconds_bucket{le="+Inf"} 1234
```

(The OpenTelemetry Prometheus exporter appends the unit suffix `_seconds` to histograms with unit `s`.)

## Logging

CH.Native supports structured logging via `Microsoft.Extensions.Logging`.

### Setup

```csharp
using Microsoft.Extensions.Logging;

var loggerFactory = LoggerFactory.Create(builder =>
{
    builder
        .SetMinimumLevel(LogLevel.Debug)
        .AddConsole();
});

var settings = ClickHouseConnectionSettings.CreateBuilder()
    .WithHost("localhost")
    .WithLoggerFactory(loggerFactory)
    .Build();
```

### Log Levels

| Level | Events |
|-------|--------|
| Trace | Wire-level protocol details |
| Debug | Query execution, connection events |
| Information | Connection open/close, bulk insert complete |
| Warning | Retries, circuit breaker state changes |
| Error | Failed operations, exceptions |

### Example Log Output

```
dbug: CH.Native.Connection[0]
      Opening connection to localhost:9000
info: CH.Native.Connection[0]
      Connected to ClickHouse 24.1.5 (protocol 54467)
dbug: CH.Native.Query[0]
      Executing: SELECT count() FROM users
dbug: CH.Native.Query[0]
      Query completed in 12ms, 1 rows
warn: CH.Native.Resilience[0]
      Retry 1/3 after 100ms: Connection refused
```

## Wire Dump Debugging

For low-level protocol debugging, enable wire dump logging:

```bash
CH_WIRE_DUMP=1 dotnet run
```

This writes hex dumps to `/tmp/ch_wire_dump.log`:

```
[2024-01-15 10:30:45.123] SEND (23 bytes):
00000000  00 0a 43 48 2e 4e 61 74  69 76 65 00 15 d4 00 00  |..CH.Native.....|
00000010  07 64 65 66 61 75 6c 74                           |.default|

[2024-01-15 10:30:45.125] RECV (45 bytes):
00000000  00 0a 43 6c 69 63 6b 48  6f 75 73 65 18 2e 31 2e  |..ClickHouse.1.|
...
```

## Full Observability Example

Complete setup with OpenTelemetry exporters:

```csharp
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using OpenTelemetry.Trace;
using OpenTelemetry.Metrics;
using CH.Native.Connection;

// Configure logging
var loggerFactory = LoggerFactory.Create(builder =>
{
    builder
        .SetMinimumLevel(LogLevel.Information)
        .AddConsole();
});

// Configure OpenTelemetry tracing
var tracerProvider = Sdk.CreateTracerProviderBuilder()
    .AddSource("CH.Native")
    .AddOtlpExporter(opts =>
    {
        opts.Endpoint = new Uri("http://localhost:4317");
    })
    .Build();

// Configure OpenTelemetry metrics
var meterProvider = Sdk.CreateMeterProviderBuilder()
    .AddMeter("CH.Native")
    .AddPrometheusExporter()
    .Build();

// Configure CH.Native with telemetry
var settings = ClickHouseConnectionSettings.CreateBuilder()
    .WithHost("localhost")
    .WithTelemetry(new TelemetrySettings
    {
        EnableTracing = true,
        EnableMetrics = true,
        IncludeSqlInTraces = true
    })
    .WithLoggerFactory(loggerFactory)
    .Build();

// Use connection - telemetry is automatic
await using var connection = new ClickHouseConnection(settings);
await connection.OpenAsync();

var count = await connection.ExecuteScalarAsync<long>("SELECT count() FROM users");
Console.WriteLine($"Users: {count}");
```

## Grafana Dashboard

Example Grafana dashboard queries for CH.Native metrics:

**Query Rate:**
```promql
rate(ch_native_queries_total[5m])
```

**Average Query Duration (seconds):**
```promql
rate(ch_native_query_duration_seconds_sum[5m]) / rate(ch_native_query_duration_seconds_count[5m])
```

**Error Rate:**
```promql
rate(ch_native_errors_total[5m])
```

**Active Connections:**
```promql
ch_native_connections_active
```

## See Also

- [Configuration](configuration.md) - Connection settings
- [Resilience](resilience.md) - Retry and circuit breaker metrics
