# Bulk Insert

CH.Native provides high-performance bulk insert capabilities optimized for loading large amounts of data into ClickHouse.

## Overview

Bulk insert uses the native protocol's block-based format, which is significantly faster than row-by-row inserts:

- **Batched writes** - Data is buffered and sent in configurable batch sizes
- **Type-safe mapping** - Properties are automatically mapped to columns
- **Streaming support** - Process `IAsyncEnumerable<T>` without loading all data into memory
- **Minimal allocations** - Optimized for low GC pressure

## Basic Usage

### Using BulkInserter

```csharp
public class Event
{
    public DateTime Timestamp { get; set; }
    public string EventType { get; set; } = "";
    public string Data { get; set; } = "";
}

await using var connection = new ClickHouseConnection("Host=localhost;Port=9000");
await connection.OpenAsync();

// Create the bulk inserter
await using var inserter = connection.CreateBulkInserter<Event>("events");

// Initialize - sends INSERT query, receives schema from server
await inserter.InitAsync();

// Add items
await inserter.AddAsync(new Event
{
    Timestamp = DateTime.UtcNow,
    EventType = "click",
    Data = "{}"
});

// Or add multiple items
var events = new List<Event> { /* ... */ };
await inserter.AddRangeAsync(events);

// Flush any remaining data and finalize
await inserter.CompleteAsync();
```

### Simplified API

For simple cases, use the extension method:

```csharp
var events = new List<Event> { /* ... */ };
await connection.BulkInsertAsync("events", events);
```

## Lifecycle

The bulk insert lifecycle:

```
CreateBulkInserter<T>() -> InitAsync() -> Add/AddRange -> [FlushAsync] -> CompleteAsync()
```

| Method | Description |
|--------|-------------|
| `CreateBulkInserter<T>(table)` | Creates inserter for table |
| `InitAsync()` | Sends INSERT query, receives column schema |
| `AddAsync(item)` | Adds single item to buffer |
| `AddRangeAsync(items)` | Adds multiple items to buffer |
| `FlushAsync()` | Sends current buffer to server (automatic at batch size) |
| `CompleteAsync()` | Sends final block, receives confirmation |

## Configuration

### Batch Size

Control how many rows are buffered before sending:

```csharp
var options = new BulkInsertOptions
{
    BatchSize = 10000 // Default varies by type
};

await using var inserter = connection.CreateBulkInserter<Event>("events", options);
```

Larger batches are more efficient but use more memory. The default batch size is optimized for typical use cases.

## Property Mapping

### Automatic Mapping

By default, properties are mapped to columns by name (case-insensitive):

```csharp
public class User
{
    public uint Id { get; set; }        // -> id column
    public string Name { get; set; }    // -> name column
    public DateTime Created { get; set; } // -> created column
}
```

### Custom Column Names

Use `ClickHouseColumnAttribute` to customize mapping:

```csharp
using CH.Native.Mapping;

public class User
{
    [ClickHouseColumn(Name = "user_id")]
    public uint Id { get; set; }

    [ClickHouseColumn(Name = "full_name")]
    public string Name { get; set; } = "";

    [ClickHouseColumn(Name = "created_at")]
    public DateTime Created { get; set; }
}
```

### Column Order

For tables where column order matters, use the `Order` property:

```csharp
public class Event
{
    [ClickHouseColumn(Order = 0)]
    public DateTime Timestamp { get; set; }

    [ClickHouseColumn(Order = 1)]
    public string EventType { get; set; } = "";

    [ClickHouseColumn(Order = 2)]
    public string Data { get; set; } = "";
}
```

### Ignoring Properties

Exclude properties from mapping:

```csharp
public class User
{
    public uint Id { get; set; }
    public string Name { get; set; } = "";

    [ClickHouseIgnore]
    public string TempData { get; set; } = ""; // Not inserted
}
```

## Streaming Insert

Process large datasets without loading everything into memory:

```csharp
// From IAsyncEnumerable
async IAsyncEnumerable<Event> GenerateEvents()
{
    for (int i = 0; i < 1_000_000; i++)
    {
        yield return new Event
        {
            Timestamp = DateTime.UtcNow,
            EventType = "generated",
            Data = $"{{\"index\": {i}}}"
        };
    }
}

await connection.BulkInsertAsync("events", GenerateEvents());
```

Or with manual control:

```csharp
await using var inserter = connection.CreateBulkInserter<Event>("events");
await inserter.InitAsync();

await foreach (var evt in GenerateEvents())
{
    await inserter.AddAsync(evt);
}

await inserter.CompleteAsync();
```

## Supported Types

All standard ClickHouse types are supported for bulk insert:

| .NET Type | ClickHouse Type |
|-----------|-----------------|
| sbyte, short, int, long | Int8, Int16, Int32, Int64 |
| byte, ushort, uint, ulong | UInt8, UInt16, UInt32, UInt64 |
| Int128, UInt128 | Int128, UInt128 |
| float, double | Float32, Float64 |
| decimal | Decimal32/64/128/256 |
| bool | Bool (UInt8) |
| string | String |
| byte[] | FixedString(N) |
| DateTime | DateTime, DateTime64 |
| DateOnly | Date, Date32 |
| DateTimeOffset | DateTime with timezone |
| Guid | UUID |
| IPAddress | IPv4, IPv6 |
| T? | Nullable(T) |
| T[] | Array(T) |
| Dictionary<K,V> | Map(K, V) |

## Best Practices

### Optimal Batch Sizes

- **Small rows** (few columns, small types): 50,000-100,000 rows per batch
- **Medium rows** (10-20 columns): 10,000-50,000 rows per batch
- **Large rows** (many columns, strings): 1,000-10,000 rows per batch

### Error Handling

Errors are thrown from `CompleteAsync()`:

```csharp
try
{
    await using var inserter = connection.CreateBulkInserter<Event>("events");
    await inserter.InitAsync();
    await inserter.AddRangeAsync(events);
    await inserter.CompleteAsync();
}
catch (ClickHouseServerException ex)
{
    Console.WriteLine($"Insert failed: {ex.Message}");
    // Handle error - data was NOT committed
}
```

### Memory Efficiency

For very large imports, stream data instead of collecting:

```csharp
// Good - streams data
await connection.BulkInsertAsync("events", ReadEventsFromFile(path));

// Avoid - loads all into memory first
var allEvents = await ReadEventsFromFile(path).ToListAsync();
await connection.BulkInsertAsync("events", allEvents);
```

### Transaction Semantics

ClickHouse bulk inserts are atomic at the block level:

- Each batch/block is inserted atomically
- If an error occurs mid-insert, previously sent blocks are committed
- For all-or-nothing semantics, use a single batch or staging tables

## Example: Large Data Import

Complete example importing a large CSV file:

```csharp
using System.Globalization;
using CsvHelper;
using CH.Native.Connection;

public class LogEntry
{
    [ClickHouseColumn(Order = 0)]
    public DateTime Timestamp { get; set; }

    [ClickHouseColumn(Order = 1)]
    public string Level { get; set; } = "";

    [ClickHouseColumn(Order = 2)]
    public string Message { get; set; } = "";

    [ClickHouseColumn(Order = 3)]
    public string Source { get; set; } = "";
}

async Task ImportLogs(string csvPath, string connectionString)
{
    await using var connection = new ClickHouseConnection(connectionString);
    await connection.OpenAsync();

    // Create table if needed
    await connection.ExecuteNonQueryAsync(@"
        CREATE TABLE IF NOT EXISTS logs (
            timestamp DateTime64(3),
            level LowCardinality(String),
            message String,
            source LowCardinality(String)
        ) ENGINE = MergeTree()
        ORDER BY timestamp
    ");

    // Stream from CSV
    await connection.BulkInsertAsync("logs", ReadCsv(csvPath));

    Console.WriteLine("Import complete");
}

async IAsyncEnumerable<LogEntry> ReadCsv(string path)
{
    using var reader = new StreamReader(path);
    using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);

    await foreach (var record in csv.GetRecordsAsync<LogEntry>())
    {
        yield return record;
    }
}
```

## See Also

- [Data Types](data-types.md) - Type mapping reference
- [Quick Start](quickstart.md) - Getting started
- [ADO.NET & Dapper](ado-net-dapper.md) - Alternative insert methods
