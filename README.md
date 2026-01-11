# CH.Native

A high-performance .NET 8 client for ClickHouse using the native binary TCP protocol.

[![NuGet](https://img.shields.io/nuget/v/CH.Native.svg)](https://www.nuget.org/packages/CH.Native)
[![.NET](https://img.shields.io/badge/.NET-8.0-512BD4)](https://dotnet.microsoft.com/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

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

See the [Getting Started Guide](docs/quickstart.md) for more examples.

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
- **Type Safety** - Full support for all ClickHouse types with .NET mapping

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
var users = await connection.QueryAsync<User>(
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

await foreach (var user in connection.QueryAsync<User>("SELECT * FROM users"))
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
await using var connection = new ClickHouseDbConnection("Host=localhost;Port=9000");
await connection.OpenAsync();

// Dapper
var users = await connection.QueryAsync<User>("SELECT * FROM users");
```

## Documentation

| Guide | Description |
|-------|-------------|
| [Quick Start](docs/quickstart.md) | Get up and running in minutes |
| [Configuration](docs/configuration.md) | Connection strings, settings, TLS, multi-server |
| [Data Types](docs/data-types.md) | ClickHouse to .NET type mapping reference |
| [Resilience](docs/resilience.md) | Retry policies, circuit breakers, load balancing |
| [Bulk Insert](docs/bulk-insert.md) | High-performance data loading |
| [ADO.NET & Dapper](docs/ado-net-dapper.md) | Standard provider and ORM integration |
| [Telemetry](docs/telemetry.md) | Tracing, metrics, and logging |

## Performance

CH.Native uses the native binary protocol (port 9000) instead of HTTP, resulting in lower latency and significantly reduced memory allocations.

### Query Performance

| Benchmark | Native TCP | HTTP | Improvement |
|-----------|-----------|------|-------------|
| SELECT 1 | 544 μs | 733 μs | 1.3x faster |
| SELECT 100 rows | 668 μs | 1,036 μs | 1.6x faster |
| COUNT(*) 1M rows | 948 μs | 1,170 μs | 1.2x faster |
| Read 1M rows (streaming) | 123 ms | 296 ms | **2.4x faster** |
| Bulk insert 1M rows | 93 ms | 180 ms | 1.9x faster |

### Memory Efficiency

| Benchmark | Native TCP | HTTP | Improvement |
|-----------|-----------|------|-------------|
| SELECT 1 | 266 KB | 545 KB | 2x less |
| Read 1M rows | 143 MB | 191 MB | 25% less |
| Bulk insert 1M rows | 44 MB | 120 MB | **2.7x less** |

*Benchmarks run on Apple M5, .NET 8.0, ClickHouse 24.8*

## Requirements

- .NET 8.0 or later
- ClickHouse server with native protocol enabled (port 9000)

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Submit a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
