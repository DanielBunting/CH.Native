# Data Types

CH.Native provides comprehensive support for ClickHouse data types, with automatic mapping to .NET types.

## Contents

- [Quick Reference](#quick-reference) — the full type table at a glance
- [Numeric Types](#numeric-types)
- [Date and Time Types](#date-and-time-types)
- [String Types](#string-types)
- [Special Types](#special-types) — UUID, IP, Enum
- [Complex Types](#complex-types) — Nullable, Array, Map, Tuple, JSON, Dynamic/Variant, aggregates, geo
- [Custom Type Mapping](#custom-type-mapping) — attribute-based POCO mapping
- [Gotchas](#gotchas) — where the .NET and ClickHouse type systems don't line up one-to-one

## Type Mapping Overview

When reading data, CH.Native automatically converts ClickHouse types to their .NET equivalents. When writing data (bulk insert), .NET types are converted back to ClickHouse format.

> Reading via the Arrow [ADBC driver](adbc.md)? That path maps ClickHouse types to **Arrow** types instead of CLR types — see the [ClickHouse → Arrow table](adbc.md#type-mapping-clickhouse--arrow).

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
| BFloat16 | float | 16-bit brain-float; ClickHouse 24.12+ |
| Bool | bool | Stored as UInt8 |
| Decimal32(S) | decimal | S = scale (decimal places) |
| Decimal64(S) | decimal | |
| Decimal128(S) | ClickHouseDecimal | 38 digits exceed .NET `decimal`'s 28-29 |
| Decimal256(S) | ClickHouseDecimal | 76 digits exceed .NET `decimal`'s 28-29 |
| Date | DateOnly | Days since 1970-01-01 |
| Date32 | DateOnly | Supports dates before 1970 (full range to 2299-12-31) |
| DateTime | DateTime | Unix timestamp (UTC) |
| DateTime('TZ') | DateTimeOffset | Instant preserved; offset reflects the column zone |
| DateTime64(P) | DateTime | P = precision (0-9); P=8/9 also readable as `long` — see [DateTime Types](#datetime-types) |
| Time | TimeOnly | Time-of-day; ClickHouse 25.10+ |
| Time64(P) | TimeOnly | Sub-second time-of-day; ClickHouse 25.10+ |
| IntervalNanosecond … IntervalYear | ClickHouseInterval | All 11 units; calendar units (Month/Quarter/Year) have no fixed duration |
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
| Nested(...) | object[][] | Equivalent to parallel `Array(T)` columns |
| LowCardinality(T) | T | Dictionary encoded |
| JSON | JsonDocument / string | ClickHouse 25.6+ required |
| Dynamic | ClickHouseDynamic | Per-row type discriminator |
| Variant(T0, T1, ...) | VariantValue<T0, T1> / ClickHouseVariant | Boxing-free up to 2 arms |
| SimpleAggregateFunction(fn, T) | CLR type of T | Transparent — wire format is identical to T |
| AggregateFunction(fn, T...) | — (not read client-side) | Opaque state; query via `finalizeAggregation()`, or `hex()` for raw bytes |
| Point | (X, Y) record struct | |
| Ring, LineString | Point[] | |
| Polygon, MultiLineString | Point[][] | |
| MultiPolygon | Point[][][] | |
| Nothing | null | Produced by bare `SELECT NULL` / `SELECT []` |

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

`UInt64` values arrive as `ulong` with the logical value — map them to `ulong`
properties, not `long`, or values above `long.MaxValue` will overflow.

### Floating Point

| Type | .NET | Precision |
|------|------|-----------|
| Float32 | float | ~7 decimal digits |
| Float64 | double | ~15-17 decimal digits |
| BFloat16 | float | ~3 decimal digits; 8-bit exponent |

`BFloat16` (brain-float) is a 16-bit IEEE-754 variant — same exponent range as `Float32` but with only 7 mantissa bits. Common in ML workloads. Requires ClickHouse 24.12+. On read, the high 16 bits are zero-extended to a `float`. On write, the low 16 mantissa bits are truncated (matching the ClickHouse server-side cast and clickhouse-cs):

```csharp
var v = await connection.ExecuteScalarAsync<float>("SELECT toBFloat16(3.14)");
// Returns ~3.140625 — low mantissa bits are lost
```

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

`Decimal32/64` map to .NET `decimal`. `Decimal128` and `Decimal256` map to
`ClickHouseDecimal` (`CH.Native.Numerics`) because their 38 and 76 significant digits
exceed `decimal`'s 28–29; all digits are preserved. `ClickHouseDecimal` also works for
bulk-inserting full-precision `Decimal128/256` values.

Writing a value with more fractional digits than the column's scale **truncates toward
zero** (no rounding, no error): `1.23456` into `Decimal64(4)` stores `1.2345`. Round in
application code if that isn't what you want.

The column's **precision** (total digit count, `P`) is **not enforced on the native
insert path**. ClickHouse validates `Decimal(P, S)` precision only when parsing SQL text
(`INSERT … VALUES`), which rejects an over-precision value with error 69 (`Too many
digits`). The native binary protocol carries the pre-scaled backing integer, which the
server stores verbatim with no precision check — so `12345.67` inserts cleanly into
`Decimal(4, 2)` and round-trips as `12345.67`, even though the declared precision is 4
(max `99.99`). This is ClickHouse server behavior, identical across native-protocol
clients. Keep values within the declared precision in application code if you rely on it.

## Date and Time Types

### Date Types

| Type | .NET | Range | Notes |
|------|------|-------|-------|
| Date | DateOnly | 1970-01-01 to 2149-06-06 | Days since epoch |
| Date32 | DateOnly | 1900-01-01 to 2299-12-31 | Supports dates before 1970 |

### DateTime Types

| Type | .NET | Precision | Range | Notes |
|------|------|-----------|-------|-------|
| DateTime | DateTime | Seconds | 1970-01-01 00:00:00 to 2106-02-07 06:28:15 | Unix timestamp (UInt32 seconds), returned as UTC |
| DateTime('TZ') | DateTimeOffset | Seconds | 1970-01-01 00:00:00 to 2106-02-07 06:28:15 | With timezone |
| DateTime64(0) | DateTime | Seconds | 1900-01-01 to 2299-12-31 | |
| DateTime64(3) | DateTime | Milliseconds | 1900-01-01 to 2299-12-31 | |
| DateTime64(6) | DateTime | Microseconds | 1900-01-01 to 2299-12-31 | |
| DateTime64(9) | DateTime | Nanoseconds | 1900-01-01 to 2262-04-11 | DateTime view truncated to 100-ns ticks |

Ranges are the server's: `DateTime` is a UInt32 Unix timestamp; `DateTime64(0–8)` spans
1900-01-01 to 2299-12-31; `DateTime64(9)` tops out at 2262-04-11 23:47:16.854775807
because the wire value is an Int64 count of nanoseconds since epoch.

`System.DateTime` ticks are 100 ns, so the `DateTime` view of scales 8–9 truncates the
last one or two fractional digits (`…00.123456789` reads as `…00.1234567`; scales 0–7
are tick-exact). To read the exact value, ask for `long` instead — you get the raw unit
count since epoch (for scale 9, identical to `toUnixTimestamp64Nano`):

```csharp
// Streaming row or ADO reader:
long nanos = row.GetFieldValue<long>(0);

// Scalar:
long nanos = await connection.ExecuteScalarAsync<long>("SELECT ts FROM t");

// Typed row mapping — declare the property as long:
public class Row { public long Ts { get; set; } }
```

The `long` path is not wired for `Nullable(DateTime64(8/9))` — select
`toUnixTimestamp64Nano(ts)` server-side there.

Timezone-qualified columns (`DateTime('TZ')`, `DateTime64(P, 'TZ')`) read as
`DateTimeOffset` with the column zone's offset, so the absolute instant is always
preserved — including during DST transitions where two instants share a wall-clock
time. Use `.UtcDateTime` for instant comparisons.

**Example:**

```sql
DateTime64(3, 'America/New_York')  -- Milliseconds with timezone
```

### Time Types

`Time` and `Time64(P)` represent a time-of-day with no calendar date — equivalent to `TimeSpan` constrained to `[00:00:00, 24:00:00)`.

| Type | .NET | Precision | Notes |
|------|------|-----------|-------|
| Time | TimeOnly | Seconds | Int32 seconds since 00:00:00 |
| Time64(0) | TimeOnly | Seconds | |
| Time64(3) | TimeOnly | Milliseconds | |
| Time64(6) | TimeOnly | Microseconds | |
| Time64(9) | TimeOnly | Nanoseconds | Truncated to 100-ns ticks (matches `DateTime64`) |

**Requires ClickHouse 25.10+** with `enable_time_time64_type=1`. Set it once per session:

```csharp
await connection.ExecuteNonQueryAsync("SET enable_time_time64_type=1");
var t = await connection.ExecuteScalarAsync<TimeOnly>("SELECT CAST('13:37:42' AS Time)");
```

Reads outside `[0, 86400)` (or `[0, 86400 × 10^P)` for `Time64`) throw `OverflowException` — `TimeOnly` cannot represent negative or overflow values. Power users wanting raw nanoseconds at `Time64(8/9)` can use the low-level `IColumnReader<long>` path.

`Time` and `Time64(P)` are also supported for **bulk insert**: a `TimeOnly` property maps directly to either column (precision is read from the column definition). The same ClickHouse 25.10+ requirement applies on the server side.

### Interval Types

All eleven `Interval*` types (`IntervalNanosecond` … `IntervalYear`) map to the
`ClickHouseInterval` struct — a signed count plus an `IntervalUnit`:

```csharp
using CH.Native.Data;

var interval = await connection.ExecuteScalarAsync<ClickHouseInterval>("SELECT INTERVAL 3 DAY");
// interval.Value == 3, interval.Unit == IntervalUnit.Day, ToString() == "3 Day"
```

Intervals are deliberately **not** mapped to `TimeSpan`: `Month`, `Quarter`, and `Year`
are calendar units with no fixed duration (how long is "1 month"?). Convert explicitly:

```csharp
if (!interval.IsCalendarUnit)
{
    TimeSpan span = interval.ToTimeSpan();   // exact for Second…Week;
                                             // Nanosecond truncates to 100-ns ticks
}
// ToTimeSpan() on Month/Quarter/Year throws NotSupportedException —
// apply calendar units with date arithmetic (e.g. dt.AddMonths) instead.
```

Intervals appear in result sets when an expression yields one (e.g. `INTERVAL 1 DAY`,
`toIntervalMonth(2)`, date subtraction helpers). They are not insertable table columns.

## String Types

### String

Variable-length strings, decoded as UTF-8:

```csharp
// ClickHouse String -> .NET string
var name = await connection.ExecuteScalarAsync<string>("SELECT 'Hello'");
```

ClickHouse `String` is actually an **arbitrary byte sequence** — it is not required to
be valid UTF-8. Embedded NUL bytes survive the string conversion fine, but bytes that
are not valid UTF-8 are replaced with U+FFFD (`�`) in the `string` view. If a column
holds binary data, read the exact bytes instead: open the connection with
`StringMaterialization=Lazy` and ask for `byte[]`:

```csharp
// Connection string: "...;StringMaterialization=Lazy"
await foreach (var row in connection.QueryStreamAsync("SELECT payload FROM blobs"))
{
    byte[] exact = row.GetFieldValue<byte[]>(0);   // raw bytes, no UTF-8 decoding
}
```

This works on the streaming row, the ADO `DbDataReader`, and `Nullable(String)` columns
(null rows surface as SQL null).

### FixedString(N)

Fixed-length binary data, padded with null bytes:

```csharp
// ClickHouse FixedString(16) -> .NET byte[]
var data = await connection.ExecuteScalarAsync<byte[]>("SELECT toFixedString('test', 16)");
// Returns 16 bytes: ['t', 'e', 's', 't', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

// For the trimmed text form, convert server-side — toString() drops the padding:
var text = await connection.ExecuteScalarAsync<string>(
    "SELECT toString(toFixedString('test', 16))");   // "test"
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

// For the member NAME, convert server-side:
var name = await connection.ExecuteScalarAsync<string>("SELECT toString(status) FROM events LIMIT 1");
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

#### Rectangular multidimensional arrays

C# rectangular arrays (`T[,]`, `T[,,]`, …) map to the same wire type as their
jagged counterparts: `Array(Array(T))` for `T[,]`, `Array(Array(Array(T)))` for
`T[,,]`, and so on. The client converts at the boundary — both for bulk insert
and for typed POCO reads — so the wire is always nested `Array(...)`.

```csharp
public class Sample
{
    public int Id { get; set; }
    public int[,] Grid { get; set; } = new int[0, 0];  // column type: Array(Array(Int32))
}

// Bulk insert.
await using var inserter = connection.CreateBulkInserter<Sample>("samples");
await inserter.InitAsync();
await inserter.AddAsync(new Sample { Id = 1, Grid = new int[2, 3] { { 1, 2, 3 }, { 4, 5, 6 } } });
await inserter.CompleteAsync();

// Read back as rectangular.
await foreach (var row in connection.QueryStreamAsync<Sample>("SELECT id, grid FROM samples"))
{
    Console.WriteLine(row.Grid[0, 2]);  // 3
}

// Or via ADO.NET / GetFieldValue.
using var reader = await connection.ExecuteReaderAsync("SELECT grid FROM samples LIMIT 1");
await reader.ReadAsync();
var grid = reader.GetFieldValue<int[,]>(0);
```

When reading back as rectangular, every inner array must have the same length
— ClickHouse data is naturally jagged on the wire. Ragged data causes a
`ClickHouseTypeConversionException` with the offending outer index and the
expected/actual inner lengths in the message; use jagged `int[][]` instead if
the data may be ragged.

Mixed shapes like `int[,][]` or `int[][,]` are rejected at type inference with
a `NotSupportedException`. Use either pure jagged (`int[][][]`) or pure
rectangular (`int[,,]`).

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

### Nested

`Nested(col1 T1, col2 T2, …)` is shorthand for parallel `Array(T)` columns of equal length. CH.Native materialises each row as a jagged `object[][]` whose outer array indexes the named subcolumns and inner arrays are the per-row values:

```sql
CREATE TABLE events (
    id UInt64,
    items Nested(name String, quantity UInt32)
) ENGINE = Memory
```

```csharp
var rows = await connection.QueryStreamAsync(
    "SELECT items.name, items.quantity FROM events"
).ToListAsync();
```

You can also project each subcolumn directly (`items.name`, `items.quantity`) and read it as `string[]` / `uint[]` — that is usually the more ergonomic shape and is what most queries against `Nested` use in practice.

#### Inserting and reading the whole column

By default (`flatten_nested = 1`) ClickHouse stores and exposes a `Nested` column as its flattened `name.subcolumn Array(T)` columns — the shape shown above, and the one most code uses. With `flatten_nested = 0` the column is presented as a single `Nested(...)` column on the wire, and CH.Native round-trips it whole: it bulk-inserts and reads back as an `object[]` per row holding the per-field arrays, in declared field order, all the same length (the fields share one offsets block).

```csharp
class Event
{
    [ClickHouseColumn(Name = "id")] public ulong Id { get; set; }

    // One object[] per row: the per-field arrays in declared order — here
    // [ string[] names, uint[] quantities ] for Nested(name String, quantity UInt32).
    [ClickHouseColumn(Name = "items")] public object[] Items { get; set; } = Array.Empty<object>();
}

await connection.ExecuteNonQueryAsync("SET flatten_nested = 0");

await using var inserter = connection.CreateBulkInserter<Event>("events");
await inserter.InitAsync();
await inserter.AddAsync(new Event
{
    Id = 1,
    Items = new object[] { new[] { "a", "b" }, new uint[] { 10, 20 } },
});
await inserter.CompleteAsync();

// Whole-column read returns object[] per row: [ namesArray, quantitiesArray ].
await foreach (var row in connection.QueryStreamAsync("SELECT items FROM events"))
{
    var items = (object[])row.GetFieldValue<object>("items");
    var names = (string[])items[0];
    var quantities = (uint[])items[1];
}
```

Field types may themselves be composite (e.g. `Nested(tag LowCardinality(String), vals Array(Int32))`); each field's array is materialised with its element CLR type.

### JSON

**Requires ClickHouse 25.6+**

The JSON type stores semi-structured data. CH.Native exposes two reader paths:

| ClickHouse Type | .NET Type | Notes |
|-----------------|-----------|-------|
| JSON | JsonDocument | Requires disposal |
| JSON | string | Set `output_format_native_write_json_as_string=1` to receive the raw JSON text |
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
await foreach (var row in connection.QueryStreamAsync(
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

### Dynamic

The `Dynamic` type stores a value plus a per-row type discriminator — different rows in the same column can hold different concrete types. Maps to `CH.Native.Data.Dynamic.ClickHouseDynamic`:

```csharp
await foreach (var row in connection.QueryStreamAsync(
    "SELECT value FROM dynamic_events"))
{
    var dyn = row.GetFieldValue<ClickHouseDynamic>("value");
    if (dyn.TryGetAs<long>(out var i)) Console.WriteLine($"int: {i}");
    else if (dyn.TryGetAs<string>(out var s)) Console.WriteLine($"str: {s}");
    else Console.WriteLine($"declared as {dyn.DeclaredTypeName}, raw: {dyn.Value}");
}
```

`IsNull`, `Discriminator`, `DeclaredTypeName`, and `TryGetAs<T>(out T)` cover the common access patterns. For bulk insert, construct `ClickHouseDynamic(discriminator, value, declaredTypeName)` directly.

When checking null-ness, read the raw column and use `IsNull` rather than stringifying:
a server-side quirk (identical in every client) makes `toString(NULL::Dynamic)` render
as an empty string and `toString(NULL::Variant(...))` as the literal text `ᴺᵁᴸᴸ` —
neither is SQL NULL.

### Variant

`Variant(T0, T1, …)` is a closed set of alternatives chosen at write time per row. CH.Native exposes two shapes:

- **`VariantValue<T0, T1>`** — boxing-free 2-arm variant. Use when you have exactly two arms and want zero allocations on the read path.
- **`ClickHouseVariant`** — N-arm variant (boxes the value). Use for ≥3 arms or when arms are heterogeneous enough that the typed shape doesn't fit.

```csharp
// 2-arm: int | string
var v = await connection.ExecuteScalarAsync<VariantValue<long, string>>(
    "SELECT CAST(42 AS Variant(Int64, String))");
// v.Discriminator picks the active arm; v.Arm0 / v.Arm1 access the value.

// N-arm
var any = await connection.ExecuteScalarAsync<ClickHouseVariant>(
    "SELECT CAST(3.14 AS Variant(Int64, String, Float64))");
if (any.TryGetAs<double>(out var d)) Console.WriteLine(d);
```

### SimpleAggregateFunction(fn, T)

`SimpleAggregateFunction(fn, T)` is a server-side hint used by `AggregatingMergeTree`: the wire format is identical to `T`, and the function name is a merge-time directive that the client ignores. The column reads as the inner CLR type directly — no wrapper.

```csharp
// Schema:  CREATE TABLE x (total SimpleAggregateFunction(sum, Int64), ...);
var total = row.GetFieldValue<long>("total");
```

`Nullable(SimpleAggregateFunction(...))` is forbidden by ClickHouse and rejected by the driver with a `FormatException`. Wrap the inner type instead: `SimpleAggregateFunction(sum, Nullable(Int64))`.

### AggregateFunction(fn, T...)

`AggregateFunction(fn, T...)` columns store the **serialized intermediate state** of an aggregate function — they appear in every materialized view backed by `AggregatingMergeTree` and in any `*State()` aggregate output.

These states are **opaque, server-internal binary blobs** whose layout is function- and inner-type-specific and is not part of a stable wire contract. CH.Native is a push-and-query client and does **not** decode raw `AggregateFunction(...)` state columns. Reading one throws `NotSupportedException` — and because the failure happens mid-block (native-protocol column data is not length-prefixed), the rest of the response is unreadable and **the connection is closed**. The exception message names the workarounds, the close is immediate (`State == Closed`), and a pooled `ClickHouseDataSource` discards the connection rather than reusing it — the blast radius is exactly one query.

The supported pattern is to query the **value** server-side rather than the state:

```sql
-- Finalize the state on the server; it reads back through the ordinary typed readers.
SELECT id, finalizeAggregation(sum_state) AS total
FROM mv_user_totals;
```

```csharp
await foreach (var row in connection.QueryStreamAsync(
    "SELECT id, toInt64(finalizeAggregation(sum_state)) AS total FROM mv_user_totals ORDER BY id"))
{
    var total = row.GetFieldValue<long>("total"); // a normal scalar, no special support
}
```

If you genuinely need the raw bytes (e.g. to ferry a mergeable state between servers that can't reach each other directly), transport them as a hex `String` — version-proof and works through any client:

```sql
SELECT id, hex(uniq_state) FROM mv_daily_users;                 -- producer
INSERT INTO mv (uniq_state) VALUES (unhex('aabbcc...'));        -- consumer
```

> Most state movement (rollups, `INSERT … SELECT` between `AggregatingMergeTree` tables, `remote()`/`Distributed`, backups) happens entirely server-side and never needs the client. Raw client-side state reading was removed because `finalizeAggregation()` covers the query case and `hex()` covers the rare transport case, both without coupling to ClickHouse internals.

For the transparent variant that **does** read as a value, see [SimpleAggregateFunction](#simpleaggregatefunctionfn-t) above.

#### Limitations

- `Nullable(AggregateFunction(...))` is rejected by both ClickHouse and the driver — there is no nullable wrapper for state columns.

### Geospatial

ClickHouse's WKT-style geometry types map to nested arrays of `Point` (a `(double X, double Y)` record struct in `CH.Native.Data.Geo`):

| ClickHouse Type | .NET Type |
|-----------------|-----------|
| Point | Point |
| Ring | Point[] |
| LineString | Point[] |
| Polygon | Point[][] (outer ring + holes) |
| MultiLineString | Point[][] |
| MultiPolygon | Point[][][] |

```csharp
var poly = await connection.ExecuteScalarAsync<Point[][]>(
    "SELECT [[(0,0),(0,1),(1,1),(1,0),(0,0)]]::Polygon");
```

All geospatial types round-trip through bulk insert as well.

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

    [ClickHouseColumn(Ignore = true)]
    public string TempData { get; set; } = ""; // Not mapped
}
```

### Attribute Reference

| Attribute | Target | Properties |
|-----------|--------|------------|
| `ClickHouseColumn` | Property | `Name` (column name), `Order` (column order), `Ignore` (skip property), `ClickHouseType` (type override) |

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

## Gotchas

Places where the .NET and ClickHouse type systems don't line up one-to-one. Each of
these is covered in detail on its type's entry above; this is the short list of what to
watch for. Every entry is pinned by an active test in the cross-client smoke suite
(`tests/CH.Native.SmokeTests`), which roundtrips values between CH.Native, the official
`clickhouse-client` CLI, and the official `ClickHouse.Driver`.

### DateTime resolution: 100 ns ticks vs nanoseconds

`System.DateTime` cannot represent nanoseconds — its ticks are 100 ns. The `DateTime`
view of `DateTime64(8)` loses the last fractional digit and `DateTime64(9)` the last
two; scales 0–7 are exact. Read the column as `long` to get the full-precision wire
value. See [DateTime Types](#datetime-types).

### String is bytes, not text

ClickHouse `String` is an arbitrary byte sequence with no encoding guarantee. The
`string` view decodes as UTF-8 and replaces invalid sequences with U+FFFD (`�`);
embedded NUL bytes survive fine. For binary-safe access, read the column as `byte[]`
under `StringMaterialization=Lazy`. See [String](#string).

### Enum values are numbers

`Enum8`/`Enum16` surface as the underlying `sbyte`/`short`, not the member name —
ClickHouse stores the number; the name only exists in the column definition. Select
`toString(val)` when you want names. See [Enum8 / Enum16](#enum8--enum16).

### FixedString keeps its padding

`FixedString(N)` is always exactly N bytes, so short values carry trailing NUL padding
into the `byte[]` you read. Select `toString(val)` for the trimmed text form. See
[FixedString](#fixedstringn).

### UInt64 doesn't fit in long

The upper half of `UInt64`'s range exceeds `long.MaxValue`. Values arrive as `ulong`;
mapping the column to a `long` property overflows for large values. See
[Unsigned Integers](#unsigned-integers).

### Wall clocks are ambiguous during DST

In a timezone with DST, two distinct instants can share the same wall-clock time during
the fall-back hour. Zoned columns therefore read as `DateTimeOffset` (instant + offset),
not wall-clock `DateTime` — compare instants via `.UtcDateTime`. See
[Date and Time Types](#date-and-time-types).

### Calendar intervals have no fixed duration

"1 month" is not a number of seconds, so `TimeSpan` cannot represent it. Interval
expressions read as `ClickHouseInterval` (count + unit); `ToTimeSpan()` converts the
time-based units and throws for `Month`/`Quarter`/`Year`. See
[Interval Types](#interval-types).

### Decimal128/256 exceed .NET decimal

`decimal` holds 28–29 significant digits; `Decimal128` holds up to 38 and `Decimal256`
up to 76. Both map to `ClickHouseDecimal` instead. See [Decimal](#decimal).

### BFloat16 truncates on write

`BFloat16` has a 7-bit mantissa. Writing a `float` keeps the high 16 bits (truncation,
matching the server-side cast), so only values exactly representable in bfloat16
round-trip bit-for-bit. See [Floating Point](#floating-point).

### An unsupported column type closes the connection

Native-protocol column data is not length-prefixed, so a response containing a column
type the reader cannot decode (in practice: exotic `AggregateFunction` state formats)
makes the rest of the response unparseable. The query fails with a `NotSupportedException`
explaining that the connection was closed; pooled connections are discarded rather than
reused. Workaround: convert in the projection (`finalizeAggregation(...)`,
`toString(...)`) so the wire carries a supported type. See
[AggregateFunction](#aggregatefunctionfn-t).

### Query parameters travel as text

`{name:Type}` parameter binding serializes values to text (in CH.Native *and* the
official driver), so the server's default fast float parser applies: the minimum
denormal `double.Epsilon` flushes to `0`. Everything else round-trips exactly —
including NaN, `0.1+0.2`, `UInt64.MaxValue`, and strings with quotes, newlines, NULs,
and emoji. If exact float bits matter, use the bulk inserter (binary, bit-exact) or
`SETTINGS precise_float_parsing=1`.

### Decimal writes truncate excess scale silently

Bulk-inserting a `decimal` with more fractional digits than the column's scale
truncates toward zero — `1.23456` into `Decimal64(4)` stores `1.2345`. No rounding, no
exception, and the official driver behaves identically. Round in application code if
banker's rounding (or any rounding) is expected. See [Decimal](#decimal).

### Decimal precision is not enforced on insert

Separately from scale, the declared **precision** (`P`, total digits) is not checked on
the native insert path: `12345.67` inserts cleanly into `Decimal(4, 2)` and round-trips
verbatim, whereas the same value via SQL `INSERT … VALUES` is rejected (error 69). The
binary protocol stores the pre-scaled backing integer as-is, and ClickHouse only
validates precision when parsing text — so this affects all native-protocol clients, not
just CH.Native. Keep values within the declared precision in application code if you rely
on it. See [Decimal](#decimal).

### toString(NULL) inside Dynamic/Variant

A server-side quirk affecting all clients: `toString(NULL::Dynamic)` renders as an empty
string and `toString(NULL::Variant(...))` as the literal text `ᴺᵁᴸᴸ` — neither is SQL
NULL. Read the raw column (typed `ClickHouseDynamic`/`ClickHouseVariant` with real
nulls) when null-ness matters. See [Dynamic](#dynamic).

## See Also

- [Quick Start](quickstart.md) - Basic examples
- [Bulk Insert](bulk-insert.md) - Type mapping for inserts
- [Configuration](configuration.md) - Connection settings
