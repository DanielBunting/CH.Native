# Data Types

CH.Native provides full support for all ClickHouse data types with automatic mapping to .NET types.

## Type Mapping Overview

When reading data, CH.Native automatically converts ClickHouse types to their .NET equivalents. When writing data (bulk insert), .NET types are converted back to ClickHouse format.

## Quick Reference

| ClickHouse Type | .NET Type | Notes |
|-----------------|-----------|-------|
| Int8 | sbyte | |
| Int16 | short | |
| Int32 | int | |
| Int64 | long | |
| Int128 | Int128 | .NET 8+ |
| Int256 | BigInteger | |
| UInt8 | byte | |
| UInt16 | ushort | |
| UInt32 | uint | |
| UInt64 | ulong | |
| UInt128 | UInt128 | .NET 8+ |
| UInt256 | BigInteger | |
| Float32 | float | |
| Float64 | double | |
| Bool | bool | Stored as UInt8 |
| Decimal32(S) | decimal | S = scale (decimal places) |
| Decimal64(S) | decimal | |
| Decimal128(S) | decimal | |
| Decimal256(S) | decimal | |
| Date | DateOnly | Days since 1970-01-01 |
| Date32 | DateOnly | Supports dates before 1970 |
| DateTime | DateTime | Unix timestamp (UTC) |
| DateTime64(P) | DateTime | P = precision (0-9) |
| String | string | UTF-8 encoded |
| FixedString(N) | byte[] | Fixed N bytes |
| UUID | Guid | |
| IPv4 | IPAddress | |
| IPv6 | IPAddress | |
| Enum8 | sbyte | |
| Enum16 | short | |
| Nullable(T) | T? | Nullable wrapper |
| Array(T) | T[] | |
| Map(K, V) | Dictionary<K, V> | |
| Tuple(...) | object[] | |
| LowCardinality(T) | T | Dictionary encoded |
| JSON | JsonDocument | ClickHouse 25.6+ required |

## Numeric Types

### Signed Integers

| Type | .NET | Range |
|------|------|-------|
| Int8 | sbyte | -128 to 127 |
| Int16 | short | -32,768 to 32,767 |
| Int32 | int | -2.1B to 2.1B |
| Int64 | long | -9.2E18 to 9.2E18 |
| Int128 | Int128 | 128-bit signed |
| Int256 | BigInteger | 256-bit signed |

### Unsigned Integers

| Type | .NET | Range |
|------|------|-------|
| UInt8 | byte | 0 to 255 |
| UInt16 | ushort | 0 to 65,535 |
| UInt32 | uint | 0 to 4.3B |
| UInt64 | ulong | 0 to 18.4E18 |
| UInt128 | UInt128 | 128-bit unsigned |
| UInt256 | BigInteger | 256-bit unsigned |

### Floating Point

| Type | .NET | Precision |
|------|------|-----------|
| Float32 | float | ~7 decimal digits |
| Float64 | double | ~15-17 decimal digits |

### Decimal

Decimal types store fixed-point numbers with explicit scale:

```sql
-- S = number of decimal places
Decimal32(4)   -- Up to 9 total digits, 4 after decimal
Decimal64(6)   -- Up to 18 total digits
Decimal128(8)  -- Up to 38 total digits
Decimal256(10) -- Up to 76 total digits

-- Generic form auto-selects size
Decimal(18, 4) -- P=precision, S=scale
```

All decimal types map to .NET `decimal`.

## Date and Time Types

### Date Types

| Type | .NET | Range | Notes |
|------|------|-------|-------|
| Date | DateOnly | 1970-01-01 to 2149-06-06 | Days since epoch |
| Date32 | DateOnly | 1900-01-01 to 2299-12-31 | Supports dates before 1970 |

### DateTime Types

| Type | .NET | Precision | Notes |
|------|------|-----------|-------|
| DateTime | DateTime | Seconds | Unix timestamp, returned as UTC |
| DateTime('TZ') | DateTimeOffset | Seconds | With timezone |
| DateTime64(0) | DateTime | Seconds | |
| DateTime64(3) | DateTime | Milliseconds | |
| DateTime64(6) | DateTime | Microseconds | |
| DateTime64(9) | DateTime | Nanoseconds | |

**Example:**

```sql
DateTime64(3, 'America/New_York')  -- Milliseconds with timezone
```

## String Types

### String

Variable-length UTF-8 encoded strings:

```csharp
// ClickHouse String -> .NET string
var name = await connection.ExecuteScalarAsync<string>("SELECT 'Hello'");
```

### FixedString(N)

Fixed-length binary data, padded with null bytes:

```csharp
// ClickHouse FixedString(16) -> .NET byte[]
var data = await connection.ExecuteScalarAsync<byte[]>("SELECT toFixedString('test', 16)");
// Returns 16 bytes: ['t', 'e', 's', 't', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
```

## Special Types

### UUID

Maps to `System.Guid`:

```csharp
var id = await connection.ExecuteScalarAsync<Guid>("SELECT generateUUIDv4()");
```

### IPv4 / IPv6

Maps to `System.Net.IPAddress`:

```csharp
var ip = await connection.ExecuteScalarAsync<IPAddress>("SELECT toIPv4('192.168.1.1')");
```

### Enum8 / Enum16

Enums store integer values with named mappings:

```sql
CREATE TABLE events (
    status Enum8('pending' = 0, 'active' = 1, 'completed' = 2)
) ENGINE = Memory
```

```csharp
// Read as the underlying integer type
var status = await connection.ExecuteScalarAsync<sbyte>("SELECT status FROM events LIMIT 1");
```

## Complex Types

### Nullable(T)

Wraps any type to allow NULL values:

```sql
CREATE TABLE users (
    email Nullable(String)
) ENGINE = Memory
```

```csharp
// Returns null for NULL values
var email = await connection.ExecuteScalarAsync<string?>("SELECT email FROM users LIMIT 1");
```

### Array(T)

Fixed-type arrays:

```sql
CREATE TABLE events (
    tags Array(String)
) ENGINE = Memory
```

```csharp
var tags = await connection.ExecuteScalarAsync<string[]>("SELECT tags FROM events LIMIT 1");
```

Nested arrays are supported:

```csharp
var matrix = await connection.ExecuteScalarAsync<int[][]>("SELECT [[1,2],[3,4]]");
```

### Map(K, V)

Key-value maps:

```sql
CREATE TABLE config (
    settings Map(String, String)
) ENGINE = Memory
```

```csharp
var settings = await connection.ExecuteScalarAsync<Dictionary<string, string>>(
    "SELECT settings FROM config LIMIT 1"
);
```

### Tuple(...)

Fixed-size tuples with heterogeneous types:

```sql
SELECT tuple(1, 'hello', 3.14)  -- Tuple(UInt8, String, Float64)
```

```csharp
var tuple = await connection.ExecuteScalarAsync<object[]>("SELECT tuple(1, 'hello', 3.14)");
// tuple[0] = 1 (byte)
// tuple[1] = "hello" (string)
// tuple[2] = 3.14 (double)
```

Named tuples preserve field names:

```sql
SELECT tuple(id = 1, name = 'Alice')
```

### LowCardinality(T)

Dictionary-encoded type for columns with few unique values. Transparent to the application:

```sql
CREATE TABLE logs (
    level LowCardinality(String)  -- 'INFO', 'WARN', 'ERROR'
) ENGINE = Memory
```

```csharp
// Reads as regular string - encoding is transparent
var level = await connection.ExecuteScalarAsync<string>("SELECT level FROM logs LIMIT 1");
```

### JSON

**Requires ClickHouse 25.6+**

The JSON type stores semi-structured data and maps to `System.Text.Json.JsonDocument`:

| ClickHouse Type | .NET Type | Notes |
|-----------------|-----------|-------|
| JSON | JsonDocument | Requires disposal |
| Nullable(JSON) | JsonDocument? | |
| Array(JSON) | JsonDocument[] | |

**Basic Usage:**

```csharp
// Read JSON column
var doc = await connection.ExecuteScalarAsync<JsonDocument>(
    "SELECT '{\"name\":\"Alice\",\"age\":30}'::JSON");

using (doc)
{
    var name = doc.RootElement.GetProperty("name").GetString();
    var age = doc.RootElement.GetProperty("age").GetInt32();
}
```

**Important:** Always dispose `JsonDocument` to avoid memory leaks.

**Nested JSON Access:**

```csharp
// Client-side traversal
var city = doc.RootElement
    .GetProperty("user")
    .GetProperty("address")
    .GetProperty("city")
    .GetString();

// Server-side path extraction (more efficient)
await foreach (var row in connection.QueryAsync(
    "SELECT data.user.address.city::String as city FROM table"))
{
    var city = row.GetFieldValue<string>("city");
}
```

**Server Settings:**

For best compatibility, add this setting to queries involving JSON columns:

```csharp
// Ensures JSON is transmitted as string (most compatible format)
var query = "SELECT data FROM table SETTINGS output_format_native_write_json_as_string=1";
```

**Writing JSON:**

```csharp
// From JsonDocument
await connection.ExecuteNonQueryAsync(
    "INSERT INTO table VALUES (@id, @data)",
    new { id = 1, data = JsonDocument.Parse("{\"key\":\"value\"}") });

// From string
await connection.ExecuteNonQueryAsync(
    "INSERT INTO table VALUES (1, '{\"key\":\"value\"}'::JSON)");
```

**Nullable JSON:**

```csharp
var data = row.GetFieldValue<JsonDocument?>("data");
if (data != null)
{
    using (data)
    {
        // Process JSON
    }
}
```

## Custom Type Mapping

Use attributes to customize how your classes map to ClickHouse tables:

### ClickHouseColumnAttribute

```csharp
using CH.Native.Mapping;

public class User
{
    [ClickHouseColumn(Name = "user_id", Order = 0)]
    public uint Id { get; set; }

    [ClickHouseColumn(Name = "full_name", Order = 1)]
    public string Name { get; set; } = "";

    [ClickHouseColumn(Order = 2)]
    public DateTime CreatedAt { get; set; }

    [ClickHouseIgnore]
    public string TempData { get; set; } = ""; // Not mapped
}
```

### Attribute Reference

| Attribute | Target | Properties |
|-----------|--------|------------|
| `ClickHouseColumn` | Property | `Name` (column name), `Order` (column order), `ClickHouseType` (type override) |
| `ClickHouseIgnore` | Property | Excludes property from mapping |
| `ClickHouseTable` | Class | `TableName` (table name for source generators) |

### Column Order

For bulk inserts, column order matters. Use the `Order` property:

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

## See Also

- [Quick Start](quickstart.md) - Basic examples
- [Bulk Insert](bulk-insert.md) - Type mapping for inserts
- [Configuration](configuration.md) - Connection settings
