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

// For very large or async sources, stream them so only one batch is in memory
// at a time (see Streaming Insert below):
//   await inserter.AddRangeStreamingAsync(GenerateEvents());

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
CreateBulkInserter<T>() -> InitAsync() -> Add / AddRange / AddRangeStreaming -> [FlushAsync] -> CompleteAsync()
```

| Method | Description |
|--------|-------------|
| `CreateBulkInserter<T>(table)` | Creates inserter for table |
| `InitAsync()` | Sends INSERT query, receives column schema |
| `AddAsync(item)` | Adds single item to buffer |
| `AddRangeAsync(IEnumerable<T>)` | Adds multiple items to buffer (whole sequence is held by the caller) |
| `AddRangeStreamingAsync(IAsyncEnumerable<T>)` | Pulls items from an async source, flushing per batch — preferred for large or unbounded inputs |
| `AddRangeStreamingAsync(IEnumerable<T>)` | Same streaming-with-flush behaviour for synchronous sources (lazy iterators, file readers) |
| `FlushAsync()` | Sends current buffer to server (automatic at batch size) |
| `CompleteAsync()` | Sends final block, receives confirmation |

`AddRangeAsync` accumulates inside the inserter's buffer and only flushes when the batch fills up. `AddRangeStreamingAsync` is the right call when you don't want the source to be held alongside the buffer — it consumes one batch at a time.

## Configuration

### Batch Size

Control how many rows are buffered before sending:

```csharp
var options = new BulkInsertOptions
{
    BatchSize = 50_000 // Default 10,000
};

await using var inserter = connection.CreateBulkInserter<Event>("events", options);
```

Larger batches are more efficient but use more memory. The default of 10,000 rows is a reasonable starting point — see [Optimal Batch Sizes](#optimal-batch-sizes) below for tuning guidance.

## Property Mapping

### Automatic Mapping

By default, the bulk inserter uses each property's name (or the `[ClickHouseColumn(Name=…)]` override) as the column identifier in the emitted `INSERT INTO table (col, …) VALUES` statement. Identifier comparison on the server is **case-sensitive**, so the property name has to match the column name exactly:

```csharp
// Table created as: CREATE TABLE users (Id UInt32, Name String, Created DateTime) …
public class User
{
    public uint Id { get; set; }          // matches column `Id`
    public string Name { get; set; }      // matches column `Name`
    public DateTime Created { get; set; } // matches column `Created`
}
```

If your tables follow snake_case or any convention that doesn't match C# property casing, pin the column name with `[ClickHouseColumn(Name = "…")]` — see [Custom Column Names](#custom-column-names) below. Once the server has accepted the INSERT and replied with its schema block, the inserter does map property → column case-insensitively for the *value extraction* step, but that fallback only kicks in if the INSERT statement itself parsed.

### Custom Column Names

Use `ClickHouseColumnAttribute` to customize mapping. This is the right tool when the C# convention (PascalCase) doesn't match the table's column convention (snake_case, lowercase, etc.):

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

Exclude properties from mapping using `Ignore = true`:

```csharp
public class User
{
    public uint Id { get; set; }
    public string Name { get; set; } = "";

    [ClickHouseColumn(Ignore = true)]
    public string TempData { get; set; } = "";
}
```

`Ignore = true` is **bidirectional** — the property is invisible to both the bulk-insert writer *and* the typed reader. On insert the column is omitted from the emitted SQL (so a server-side `DEFAULT` gets to fire). On read, the property keeps its C# default value regardless of what the server sends back, even if the projection includes the column. Use this when the property is purely transient client state; don't reach for it as a "read but don't write" toggle.

## Qualified table names

Both inserters accept a `database.table` qualified name and split it on the single dot, rendering it as `` `db`.`table` `` in the emitted INSERT. The unqualified form continues to resolve against the connection's default database:

```csharp
// Unqualified — resolves against the connection's default database.
await connection.BulkInsertAsync("events", rows);

// Qualified — addresses analytics.events explicitly.
await connection.BulkInsertAsync("analytics.events", rows);
```

If the table name itself contains a dot, use the explicit `(database, tableName)` overload as the escape hatch:

```csharp
await connection.BulkInsertAsync(
    database: "analytics",
    tableName: "raw.events", // a real table name with a dot in it
    rows);
```

The same `(database, tableName)` pair is available on `BulkInserter<T>`, `DynamicBulkInserter`, `ClickHouseConnection.CreateBulkInserter` / `BulkInsertAsync`, `ClickHouseDataSource.CreateBulkInserterAsync`, and `ResilientConnection.BulkInsertAsync`. The per-connection schema cache keys on `(Database, Table, ColumnListFingerprint)`, so two different tables in different databases never collide.

## Dynamic (POCO-less) bulk insert

When the row shape isn't known at compile time — generic ETL, schema-driven imports, or anything that can't easily be expressed as a `T` — use `DynamicBulkInserter`. Rows are supplied as `object?[]` arrays whose element order matches a caller-supplied `columnNames` list:

```csharp
var columns = new[] { "timestamp", "event_type", "data" };

await using var inserter = connection.CreateBulkInserter("events", columns);
await inserter.InitAsync();

await inserter.AddAsync(new object?[] { DateTime.UtcNow, "click", "{}" });
await inserter.AddRangeAsync(rows); // IEnumerable<object?[]> or IAsyncEnumerable<object?[]>

await inserter.CompleteAsync();
```

The lifecycle (`InitAsync` → `AddAsync` / `AddRangeAsync` / `FlushAsync` → `CompleteAsync`) and `BatchSize` semantics are identical to `BulkInserter<T>`. A one-shot overload mirrors the typed path:

```csharp
await connection.BulkInsertAsync("events", columns, rows);
```

### Skipping the schema probe with `ColumnTypes`

By default, `DynamicBulkInserter.InitAsync` round-trips to the server to fetch the column schema. When you already know the types — usually because you generated the inserter from a schema you control — pass them via `BulkInsertOptions.ColumnTypes` to skip that probe:

```csharp
var options = new BulkInsertOptions
{
    ColumnTypes = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
    {
        ["timestamp"]  = "DateTime64(3)",
        ["event_type"] = "LowCardinality(String)",
        ["data"]       = "String",
    },
};

await using var inserter = connection.CreateBulkInserter("events", columns, options);
await inserter.InitAsync(); // no schema probe — types come from ColumnTypes
```

The probe is skipped only when every column in `columnNames` has a matching entry; missing entries fall back to the probe. This is a `DynamicBulkInserter`-only optimisation — the typed `BulkInserter<T>` path always probes.

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

Or with manual control via `AddRangeStreamingAsync` — this is what `BulkInsertAsync` uses internally and is the path you want for large or unbounded sources:

```csharp
await using var inserter = connection.CreateBulkInserter<Event>("events");
await inserter.InitAsync();

await inserter.AddRangeStreamingAsync(GenerateEvents());

await inserter.CompleteAsync();
```

`AddRangeStreamingAsync` flushes each batch as soon as it fills, so memory stays bounded at one `BatchSize`-worth of rows regardless of how long the source runs.

## Supported Types

Bulk insert covers the same type system as the read path. The common .NET → ClickHouse mappings:

| .NET Type | ClickHouse Type |
|-----------|-----------------|
| sbyte, short, int, long | Int8, Int16, Int32, Int64 |
| Int128, BigInteger | Int128, Int256 |
| byte, ushort, uint, ulong | UInt8, UInt16, UInt32, UInt64 |
| UInt128, BigInteger | UInt128, UInt256 |
| float, double | Float32, Float64 |
| float | BFloat16 (mantissa truncated; ClickHouse 24.12+) |
| decimal, ClickHouseDecimal | Decimal32/64/128/256 |
| bool | Bool (UInt8) |
| string | String |
| byte[] | FixedString(N) |
| DateTime | DateTime, DateTime64 |
| DateTimeOffset | DateTime('TZ') |
| DateOnly | Date, Date32 |
| TimeOnly | Time, Time64(P) (ClickHouse 25.10+) |
| Guid | UUID |
| IPAddress | IPv4, IPv6 |
| T? | Nullable(T) |
| T[] | Array(T) |
| Dictionary<K,V> | Map(K, V) |
| string / JsonDocument | JSON (ClickHouse 25.6+) |
| ClickHouseDynamic | Dynamic |
| ClickHouseVariant, VariantValue<T0,T1> | Variant(T0, T1, …) |
| Point, Point[], Point[][], Point[][][] | Point, Ring/LineString, Polygon/MultiLineString, MultiPolygon |

`Nullable(...)` wraps any non-composite type, and composites compose freely (`Array(Nullable(LowCardinality(String)))` round-trips). See [Data Types](data-types.md) for the full reference.

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
