# CH.Native

A high-performance modern .NET client for ClickHouse using the native binary TCP protocol.

[![NuGet](https://img.shields.io/nuget/v/CH.Native.svg)](https://www.nuget.org/packages/CH.Native)
[![NuGet Downloads](https://img.shields.io/nuget/dt/CH.Native.svg)](https://www.nuget.org/packages/CH.Native)
[![.NET](https://img.shields.io/badge/.NET-8.0%20|%209.0%20|%2010.0-512BD4)](https://dotnet.microsoft.com/)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

## Quick Start

```bash
dotnet add package CH.Native
```

```csharp
await using var connection = new ClickHouseConnection("Host=localhost;Port=9000");
await connection.OpenAsync();

var result = await connection.ExecuteScalarAsync<int>("SELECT 1");
Console.WriteLine(result); // 1
```

See the [Getting Started Guide](docs/quickstart.md) for more examples, or run
[`samples/CH.Native.Samples.QuickStart`](samples/CH.Native.Samples.QuickStart/) for the same flow as a runnable project.

## Features

- **Native Binary Protocol** - Direct TCP communication on port 9000 for optimal performance
- **Full Async/Await** - All operations are async with streaming support
- **ADO.NET Provider** - Standard `DbConnection` implementation with Dapper compatibility
- **Bulk Insert** - High-performance batched inserts with typed mapping
- **Compression** - LZ4 (default) and Zstd compression support
- **Resilience** - Built-in retry policies, circuit breakers, and health checking
- **Load Balancing** - Multi-server support with round-robin, random, or first-available strategies
- **TLS/SSL** - Secure connections with certificate validation
- **Telemetry** - OpenTelemetry-compatible tracing, metrics, and logging
- **Type Safety** - Comprehensive ClickHouse type coverage with strongly-typed .NET mapping

## Supported ClickHouse Types

CH.Native supports the ClickHouse type system across read, write, and bulk-insert paths. See [Data Types](docs/data-types.md) for the complete CLR-mapping reference and its edge cases.

| Category | ClickHouse Types | .NET Mapping |
|---|---|---|
| **Signed integers** | `Int8`, `Int16`, `Int32`, `Int64`, `Int128`, `Int256` | `sbyte`, `short`, `int`, `long`, `Int128`, `BigInteger` |
| **Unsigned integers** | `UInt8`, `UInt16`, `UInt32`, `UInt64`, `UInt128`, `UInt256` | `byte`, `ushort`, `uint`, `ulong`, `UInt128`, `BigInteger` |
| **Floating point** | `Float32`, `Float64`, `BFloat16` | `float`, `double`, `float` |
| **Fixed-point** | `Decimal32(S)`, `Decimal64(S)`, `Decimal128(S)`, `Decimal256(S)` | `decimal` (or `ClickHouseDecimal` for D128/256 wide range) |
| **Boolean** | `Bool` | `bool` |
| **Date / Time** | `Date`, `Date32`, `DateTime`, `DateTime('Tz')`, `DateTime64(P)`, `Time`, `Time64(P)` | `DateOnly`, `DateTime`, `DateTimeOffset`, `TimeOnly` |
| **String** | `String`, `FixedString(N)` | `string`, `byte[]` |
| **Network / IDs** | `UUID`, `IPv4`, `IPv6` | `Guid`, `IPAddress` |
| **Enums** | `Enum8`, `Enum16` | `sbyte`, `short` (or your enum via cast) |
| **Composites** | `Nullable(T)`, `Array(T)`, `Map(K,V)`, `Tuple(...)`, `Nested(...)`, `LowCardinality(T)` | `T?`, `T[]`, `Dictionary<K,V>`, `object[]`, `object[][]`, `T` (transparent) |
| **Geospatial** | `Point`, `Ring`, `LineString`, `Polygon`, `MultiLineString`, `MultiPolygon` | `Point`, `Point[]`, `Point[][]`, `Point[][][]` |
| **Semi-structured** | `JSON`, `Dynamic`, `Variant(T0, T1, ...)` | `string` / `JsonDocument`, `ClickHouseDynamic`, `VariantValue<T0, T1>` (boxing-free 2-arm) or `ClickHouseVariant` (boxed N-arm) |

These types round-trip through both the read path (`ExecuteReaderAsync`, `QueryStreamAsync<T>`) and the bulk-insert path (`CreateBulkInserter<T>`), give or take a few documented precision caveats (see [gotchas](docs/data-types.md#gotchas)). `Nullable(...)` wraps any non-composite type. Composites compose freely (e.g. `Array(Nullable(LowCardinality(String)))`).

## Installation

```bash
dotnet add package CH.Native
```

## Basic Usage

### Execute a Query

```csharp
await using var connection = new ClickHouseConnection("Host=localhost;Port=9000");
await connection.OpenAsync();

// Scalar query
var count = await connection.ExecuteScalarAsync<long>("SELECT count() FROM users");

// DDL/DML
await connection.ExecuteNonQueryAsync("CREATE TABLE IF NOT EXISTS users (id UInt32, name String) ENGINE = Memory");
```

### Query with Parameters

```csharp
var users = await connection.QueryStreamAsync<User>(
    "SELECT * FROM users WHERE age > @minAge",
    new { minAge = 18 }
).ToListAsync();
```

### Typed Results

```csharp
public class User
{
    public uint Id { get; set; }
    public string Name { get; set; }
    public int Age { get; set; }
}

await foreach (var user in connection.QueryStreamAsync<User>("SELECT * FROM users"))
{
    Console.WriteLine($"{user.Id}: {user.Name}");
}
```

### Bulk Insert

```csharp
var users = new List<User> { /* ... */ };

await using var inserter = connection.CreateBulkInserter<User>("users");
await inserter.InitAsync();
await inserter.AddRangeAsync(users);
await inserter.CompleteAsync();
```

### ADO.NET / Dapper

```csharp
// Use CH.Native.Dapper for the fast-path Dapper-shaped API on top of CH.Native.
using CH.Native.Dapper;

await using var connection = new ClickHouseDbConnection("Host=localhost;Port=9000");
await connection.OpenAsync();

// QueryAsync<T> here is CH.Native.Dapper's fast-path extension — routes
// through CH.Native's typed-accessor mapper, no boxing tax.
var users = await connection.QueryAsync<User>("SELECT * FROM users");
```

For DI scenarios where the connection is typed as `IDbConnection`, replace
`using Dapper;` with `using CH.Native.Dapper;` to opt into the fast path
automatically. See [ADO.NET & Dapper](docs/ado-net-dapper.md) for the full
story.

## Documentation

| Guide | Description |
|-------|-------------|
| [Quick Start](docs/quickstart.md) | Get up and running in minutes |
| [Configuration](docs/configuration.md) | Connection strings, settings, TLS, multi-server |
| [Authentication](docs/authentication.md) | Password, JWT, SSH key, mTLS, role activation |
| [Connection Pooling](docs/connection-pooling.md) | `ClickHouseDataSource`, sizing, observability |
| [Dependency Injection](docs/dependency-injection.md) | `AddClickHouse`, keyed services, providers, health checks |
| [Data Types](docs/data-types.md) | ClickHouse to .NET type mapping reference, including [gotchas](docs/data-types.md#gotchas) |
| [Resilience](docs/resilience.md) | Retry policies, circuit breakers, load balancing |
| [Bulk Insert](docs/bulk-insert.md) | High-performance data loading |
| [ADO.NET & Dapper](docs/ado-net-dapper.md) | Standard provider and ORM integration |
| [LINQ Provider](docs/linq-provider.md) | `connection.Table<T>()`, operators, modifiers |
| [Telemetry](docs/telemetry.md) | Tracing, metrics, and logging |
| [Performance Comparison](docs/performance-comparison.md) | Full benchmark matrix vs `ClickHouse.Driver` and `Octonica` |

## Samples

End-to-end runnable console projects under [`samples/`](samples/). Each picks a flavour by CLI arg, creates a temp table, runs the demo, and drops the table.

| Project | What it covers |
|---------|----------------|
| [QuickStart](samples/CH.Native.Samples.QuickStart/) | Runnable mirror of [`docs/quickstart.md`](docs/quickstart.md) — open a connection, scalar query, bulk insert, typed read. Start here. |
| [Queries](samples/CH.Native.Samples.Queries/README.md) | Every read path: scalar, reader, raw rows, typed, LINQ, ADO.NET, Dapper, pooled, resilient, progress, log analytics |
| [Insert](samples/CH.Native.Samples.Insert/README.md) | Every write path: single, collection, async stream, one-shot bulk, long-lived, dynamic, pooled, cross-database, plain SQL |
| [Hosting](samples/CH.Native.Samples.Hosting/README.md) | ASP.NET host wiring: `AddClickHouse`, keyed DataSources, all four auth methods (password / JWT / SSH / mTLS) against a docker overlay, credential providers, per-request role activation, health checks, bulk insert |

## Performance

Three-way comparison against [ClickHouse.Driver](https://github.com/ClickHouse/clickhouse-cs) (HTTP) and [Octonica](https://github.com/Octonica/ClickHouseClient) (native TCP). Each row shows **time / memory**; the **best** value in each row is bold.

The tables below cover the headline workloads. **For the full benchmark matrix** — every row-count scaling for reads/inserts, complex queries (GROUP BY / JOIN / WHERE / ORDER BY), compression algorithms (LZ4 / Zstd), connection-establishment latency, and specialised type comparisons (Geo, Variant, JSON) — see **[docs/performance-comparison.md](docs/performance-comparison.md)**.

### Small queries (latency)

| Workload | CH.Native | ClickHouse.Driver | Octonica |
|---|---|---|---|
| `SELECT 1` | **586 μs** | 978 μs | 878 μs |
| `SELECT count(*) FROM <1M>` | **992 μs** | 1,408 μs | 1,041 μs |
| `SELECT 100 rows` | **660 μs** | 1,183 μs | 710 μs |

### Streaming reads — 1M rows

CH.Native ships an opt-in `StringMaterialization=Lazy` mode that defers UTF-8 string decode until each field is accessed. The "lazy / default" cells below show both modes; the default mode also benefits from the typed-accessor read path.

| Workload | CH.Native (lazy / default) | ClickHouse.Driver | Octonica |
|---|---|---|---|
| Stream `id` only | **79 ms / 0.13 MB** · 115 ms / 101 MB | 262 ms / 195 MB | 79 ms / 80 MB |
| Stream `id + name (string)` | **58 ms / 54 MB** · 76 ms / 54 MB | 86 ms / 78 MB | 73 ms / 91 MB |
| Materialize to POCO (typed mapper) | 181 ms / 171 MB · **192 ms / 171 MB** | 475 ms / 265 MB | 210 ms / 251 MB |

### Dapper-style API — 1M rows

`CH.Native.Dapper` provides Dapper-shaped extensions that route through CH.Native's typed-accessor mapper, sidestepping the per-value boxing Dapper's compiled mapper pays. Compared row-for-row against Dapper on top of `ClickHouse.Driver`:

| Workload | CH.Native.Dapper | Driver + Dapper | CH.Native is |
|---|---|---|---|
| `QueryStreamAsync<T>` (unbuffered) | **104 ms / 101 MB** | 209 ms / 172 MB | **2.0× faster, 41% less mem** |
| `QueryAsync<T>` (buffered) | **143 ms / 117 MB** | 302 ms / 188 MB | **2.1× faster, 38% less mem** |
| `QueryAsync<T>` via `IDbConnection` (DI) | 151 ms / 117 MB | 302 ms / 188 MB | 2.0× faster, 38% less mem |

### Bulk insert — 1M rows

| Workload | CH.Native | ClickHouse.Driver | Octonica |
|---|---|---|---|
| Bulk insert 1M rows | **99 ms / 0.4 MB** | 929 ms / 97 MB | n/a (errored) |

### Test conditions

- Apple M5, .NET 10.0, ClickHouse `26.5.1` (Docker image `clickhouse/clickhouse-server:latest` resolved at run time).
- Driver: `ClickHouse.Driver 1.2.0` (HTTP); Octonica: `Octonica.ClickHouseClient 3.1.8` (native TCP).
- Reproduce with: `dotnet run --project benchmarks/CH.Native.Benchmarks -c Release -- large` (or `dapper`, `insert`, `simple`).

## Requirements

- .NET 8.0, 9.0, or 10.0
- ClickHouse server with native protocol enabled (port 9000)

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Submit a pull request

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
