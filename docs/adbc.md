# ADBC Driver (Apache Arrow)

`CH.Native.Adbc` is an [Apache Arrow ADBC](https://arrow.apache.org/adbc/) driver for ClickHouse,
built on the CH.Native native-protocol client. It streams query results as Arrow `RecordBatch`es, so
ClickHouse data flows into the Arrow ecosystem (Arrow compute, ADBC-aware tools, Arrow Flight, etc.)
without a per-row marshalling step.

## Contents

- [When to use it](#when-to-use-it)
- [Quick Start](#quick-start)
- [Connection Options](#connection-options)
- [Type Mapping (ClickHouse → Arrow)](#type-mapping-clickhouse--arrow)
  - [Scalar types](#scalar-types)
  - [Wrappers](#wrappers)
  - [Composite and geo types](#composite-and-geo-types)
  - [Text-projected types](#text-projected-types)
  - [Unsupported types](#unsupported-types)
- [Caveats](#caveats)
- [Performance](#performance)
- [Compatibility](#compatibility)

## When to use it

Reach for the ADBC driver when the consumer speaks Arrow — analytics/dataframe libraries, columnar
pipelines, or any tool that already accepts an ADBC driver. For ordinary application access (typed
reads, Dapper, ADO.NET) prefer the core client and [ADO.NET & Dapper](ado-net-dapper.md); the ADBC
driver returns Arrow columns, not CLR rows.

## Quick Start

```csharp
using Apache.Arrow.Adbc;
using CH.Native.Adbc;

var driver = new ClickHouseAdbcDriver();
var database = driver.Open(new Dictionary<string, string>
{
    [AdbcOptionKeys.ConnectionString] = "Host=localhost;Port=9000;Username=default;Password=",
});

using var connection = database.Connect(null);
using var statement = connection.CreateStatement();
statement.SqlQuery = "SELECT number, number * 2 AS doubled FROM system.numbers LIMIT 1000";

var result = statement.ExecuteQuery();
using var stream = result.Stream!;

while (await stream.ReadNextRecordBatchAsync() is { } batch)
{
    using (batch)
    {
        // batch.Column(0) is an Arrow Int64Array, batch.Column(1) an Int64Array, ...
    }
}
```

Other entry points on the connection/statement:

- `connection.GetTableSchema(catalog, dbSchema, tableName)` — the Arrow `Schema` for a table.
- `statement.ExecuteSchema()` — the result `Schema` without transferring rows.

> Dispose the stream (and each batch) promptly. Disposing a partially-read stream cancels the
> server-side query rather than draining the remaining result.

## Connection Options

Pass options to `driver.Open(...)` as a string dictionary. The simplest path is a single
[connection string](configuration.md) under `AdbcOptionKeys.ConnectionString`; the remaining keys
layer on top of it (and override it).

| Key (`AdbcOptionKeys`) | Raw string | Default | Meaning |
|---|---|---|---|
| `ConnectionString` | `ch.native.connection_string` | — | A full CH.Native connection string |
| `Host` | `ch.native.host` | `localhost` | Server host |
| `Port` | `ch.native.port` | `9000` | Native-protocol port |
| `Database` | `ch.native.database` | `default` | Default database |
| `Username` | `username` | `default` | Username (canonical ADBC key) |
| `Password` | `password` | — | Password (canonical ADBC key) |
| `UseTls` | `ch.native.tls` | `false` | Enable TLS |

## Type Mapping (ClickHouse → Arrow)

The driver maps each ClickHouse column type to an Arrow type. For the CLR-side mapping used by the
core client (reads, Dapper, bulk insert), see [Data Types](data-types.md) instead.

### Scalar types

| ClickHouse | Arrow type | Notes |
|---|---|---|
| `Int8` / `Int16` / `Int32` / `Int64` | `Int8` / `Int16` / `Int32` / `Int64` | |
| `UInt8` / `UInt16` / `UInt32` / `UInt64` | `UInt8` / `UInt16` / `UInt32` / `UInt64` | |
| `Float32` | `Float` (32-bit) | |
| `BFloat16` | `Float` (32-bit) | Widened to float32 (lossless for bf16) |
| `Float64` | `Double` | |
| `Bool` | `Boolean` | |
| `String` | `Utf8` | |
| `FixedString(N)` | `Binary` | Fixed bytes, including any zero padding |
| `UUID` | `Utf8` | Canonical `8-4-4-4-12` text |
| `IPv4` / `IPv6` | `Utf8` | Text form |
| `Date` / `Date32` | `Date32` | |
| `DateTime('tz')` | `Timestamp(Second, tz)` | Timezone carried on the Arrow type |
| `DateTime64(p, 'tz')` | `Timestamp(unit, tz)` | unit = ms/µs/ns from precision `p` |
| `Time` | `Time32(Second)` | |
| `Time64(p)` | `Time64(µs \| ns)` | Arrow `Time64` is µs/ns only |
| `Decimal32/64/128(P, S)` | `Decimal128(P, S)` | |
| `Decimal256(P, S)` | `Decimal256(P, S)` | |
| `Enum8` / `Enum16` | `Int8` / `Int16` | Underlying integer value, not the label |
| `Nothing` | `Null` | e.g. `SELECT NULL` |

### Wrappers

| ClickHouse | Arrow | Notes |
|---|---|---|
| `Nullable(T)` | `T`'s Arrow type, field marked nullable | |
| `LowCardinality(T)` | `T`'s Arrow type | Dictionary encoding is not surfaced; values are materialized |

### Composite and geo types

| ClickHouse | Arrow | Notes |
|---|---|---|
| `Array(T)` | `List<T>` | Nests freely, e.g. `Array(Array(Int32))` → `List<List<Int32>>` |
| `Map(K, V)` | `Map<K, V>` | |
| `Tuple(...)` | `Struct` | Fields named `1`, `2`, … positionally |
| `Nested(...)` | `List<Struct>` | (ClickHouse sends Nested as `Array(Tuple(...))`) |
| `Point` | `Struct<x: Double, y: Double>` | |
| `Ring` / `LineString` | `List<Point>` | |
| `Polygon` / `MultiLineString` | `List<List<Point>>` | |
| `MultiPolygon` | `List<List<List<Point>>>` | |

### Text-projected types

Some ClickHouse types have no faithful fixed Arrow representation. Rather than lose data, the driver
surfaces them as `Utf8` text:

| ClickHouse | Arrow | Why text |
|---|---|---|
| `Int128` / `Int256` / `UInt128` / `UInt256` | `Utf8` (exact decimal) | Arrow has no integer wider than 64 bits, and `Decimal256` tops out at 76 digits — one short of `Int256`/`UInt256`'s range |
| `JSON` | `Utf8` | Raw JSON text |
| `Variant` / `Dynamic` | `Utf8` | No single static Arrow type spans the alternatives |
| `Interval*` | `Utf8` | No Arrow interval unit covers every ClickHouse unit (Week, Quarter, …) |

### Unsupported types

`AggregateFunction`, `SimpleAggregateFunction`, and the `Geometry` discriminated-union type are not
yet mapped; a column of one of these throws `NotSupportedException` during conversion.

## Caveats

- **Naive `DateTime`** (no timezone) decodes against the server timezone, so the exact instant is
  server-dependent; pin a timezone (`DateTime('UTC')`) for deterministic instants.
- **Enums** surface as their underlying signed integer, not the string label.
- **Wide integers / JSON / Variant / Dynamic / Interval** are text projections (see above) — exact,
  but not native Arrow numeric/union types.
- Disposing a partially-read stream **cancels** the server query rather than draining it.

## Performance

The benchmark below reads the same result set through ADBC (Arrow) and through the row-oriented
paths, then aggregates two numeric columns — i.e. "read a result set and reduce it". Reproduce it
with:

```bash
dotnet run --project benchmarks/CH.Native.Benchmarks -c Release --framework net9.0 -- adbc
```

**1M rows** (Apple Silicon, .NET 9, ClickHouse in Testcontainers; representative, not absolute):

| Path | Mean | Allocated |
|---|---:|---:|
| Native `DataReader` (typed accessors) | ~82 ms | ~54 MB |
| Native `QueryStream<T>` (typed POCO) | ~108 ms | ~105 MB |
| **ADBC** sum columns (Arrow spans) | ~118 ms | ~259 MB |
| **ADBC** drain RecordBatches (no per-value work) | ~127 ms | ~259 MB |
| Native Dapper `QueryAsync<T>` | ~153 ms | ~117 MB |

At **small result sets** (100 rows) ADBC is competitive — on par with the leanest `DataReader` path
and faster than the row mappers, because there is no per-row POCO/mapper step.

What to take from this:

- **Consuming Arrow is cheap.** "Sum via Arrow spans" is no slower than "drain only" — reducing over
  the column buffers costs essentially nothing once the batch exists. ADBC's value is the columnar
  shape and zero-copy access on the *consumer* side.
- **The current cost is decode-side, not read-side.** The driver builds each Arrow array through the
  boxing `ITypedColumn.GetValue` accessor (one box per cell — ~4M for 4 columns × 1M rows) plus the
  Arrow builders' growable buffers, which is why ADBC allocates more and runs slower than the typed
  row paths on large scans today. Span-based fast paths for the hot primitive columns are a planned
  optimization; this table is the "before" baseline for that work.

In short: prefer ADBC when the consumer speaks Arrow or the result set is small/medium; for large
pure-CLR scans where you just need typed values, the `DataReader`/`QueryStream<T>` paths are leaner
until the converter's span fast-paths land.

## Compatibility

The ADBC read path is cross-checked against the core client: an integration suite reads the same
tables through both ADBC (Arrow) and Dapper (the row-mapper path) and asserts the decoded values
agree across the full scalar, wrapper, composite, and geo matrix. So the Arrow output is the same
data a long-standing CH.Native + Dapper consumer already gets, just in columnar form.
