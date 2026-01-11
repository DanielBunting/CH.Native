# Quick Start

Get up and running with CH.Native in minutes.

## Prerequisites

- [.NET 8.0 SDK](https://dotnet.microsoft.com/download) or later
- ClickHouse server running with native protocol (port 9000)

**Start ClickHouse with Docker:**

```bash
docker run -d -p 9000:9000 -p 8123:8123 clickhouse/clickhouse-server
```

## Installation

```bash
dotnet add package CH.Native
```

## Examples

### Execute a Query

```csharp
using CH.Native.Connection;

await using var connection = new ClickHouseConnection("Host=localhost;Port=9000");
await connection.OpenAsync();

// Scalar query
var result = await connection.ExecuteScalarAsync<int>("SELECT 1 + 1");
Console.WriteLine(result); // 2

// Create a table
await connection.ExecuteNonQueryAsync(@"
    CREATE TABLE IF NOT EXISTS users (
        id UInt32,
        name String,
        created DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY id
");
```

### Query Typed Results

```csharp
using CH.Native.Connection;

public class User
{
    public uint Id { get; set; }
    public string Name { get; set; } = "";
    public DateTime Created { get; set; }
}

await using var connection = new ClickHouseConnection("Host=localhost;Port=9000");
await connection.OpenAsync();

// Query with automatic mapping
await foreach (var user in connection.QueryAsync<User>("SELECT id, name, created FROM users"))
{
    Console.WriteLine($"{user.Id}: {user.Name} (created {user.Created})");
}

// Query with parameters
var activeUsers = await connection.QueryAsync<User>(
    "SELECT * FROM users WHERE created > @since",
    new { since = DateTime.UtcNow.AddDays(-7) }
).ToListAsync();
```

### Bulk Insert Data

```csharp
using CH.Native.Connection;
using CH.Native.BulkInsert;

var users = new List<User>
{
    new() { Id = 1, Name = "Alice" },
    new() { Id = 2, Name = "Bob" },
    new() { Id = 3, Name = "Charlie" }
};

await using var connection = new ClickHouseConnection("Host=localhost;Port=9000");
await connection.OpenAsync();

await using var inserter = connection.CreateBulkInserter<User>("users");
await inserter.InitAsync();
await inserter.AddRangeAsync(users);
await inserter.CompleteAsync();

Console.WriteLine("Inserted 3 users");
```

## Next Steps

- [Configuration](configuration.md) - Connection strings, TLS, multi-server setup
- [Data Types](data-types.md) - ClickHouse to .NET type mapping
- [Bulk Insert](bulk-insert.md) - High-performance data loading
- [ADO.NET & Dapper](ado-net-dapper.md) - Standard provider integration
- [Resilience](resilience.md) - Retry policies and circuit breakers
- [Telemetry](telemetry.md) - Observability and debugging
