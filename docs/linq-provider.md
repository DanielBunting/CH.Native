# LINQ Provider

> **Preview.** The LINQ provider is usable for single-table reads, but its operator coverage
> is deliberately narrow and unsupported constructs throw `NotSupportedException`
> **at query-build time — there is no compile-time signal**. Treat it as a convenience for
> readable single-table queries, not a general-purpose ORM. For multi-table queries (`Join`),
> subqueries, CTEs, or set operations, fall back to raw SQL via
> `connection.QueryStreamAsync<T>(sql)`. The full list is in [Limitations](#limitations) below.

CH.Native ships a typed LINQ provider in `CH.Native.Linq` that translates LINQ expressions to ClickHouse SQL. It's designed for readable, single-table queries — for joins, CTEs, or anything not in the operator list below, fall back to `connection.QueryStreamAsync<T>(sql)`.

## Entry point

The starting point is the `Table<T>()` extension on either `ClickHouseConnection` or `ClickHouseDataSource`. For the connection variant the connection must be open; the DataSource variant rents a pooled connection for the lifetime of each enumeration / `InsertAsync` call:

```csharp
using CH.Native.Connection;
using CH.Native.Linq;

// Direct connection
await using var connection = new ClickHouseConnection("Host=localhost;Port=9000");
await connection.OpenAsync();
var users = connection.Table<User>();        // table name = "user" (snake_case of T)
var rooms = connection.Table<Room>("rooms"); // explicit override

// Pooled DataSource (typical service code)
var events = dataSource.Table<Event>();
var logs   = dataSource.Table<LogEntry>("logs_v2");
```

`Table<T>()` resolves the table name from the entity type via `TableNameResolver` — it lowercases and snake-cases the type name (e.g. `OrderItem` → `order_item`). Pass an explicit table name when the convention doesn't fit. Column-name mapping uses `[ClickHouseColumn(Name = "…")]`, the same as the bulk-insert and read paths.

The DataSource handle itself does not pin a connection — it composes naturally with concurrent service code, with each enumeration or `InsertAsync` call renting and returning its own connection.

## Operators that translate

| Operator | Notes |
|---|---|
| `Where(predicate)` | Translated to a `WHERE` clause; multiple `Where` calls are AND-ed. |
| `Select(projector)` | Projects to anonymous types or DTOs. |
| `OrderBy` / `OrderByDescending` | First call sets the ordering; `ThenBy` / `ThenByDescending` append. |
| `Take(n)` / `Skip(n)` | Translated to `LIMIT … OFFSET …`. |
| `First` / `FirstOrDefault` / `Single` / `SingleOrDefault` | Adds `LIMIT 1` (or `LIMIT 2` for `Single*`). |
| `Distinct` | Translated to `DISTINCT`. |
| `Count` / `LongCount` / `Any` / `All` | Aggregate forms. |
| `Sum` / `Average` / `Min` / `Max` | Aggregate forms. |
| `GroupBy` | Translated to `GROUP BY`. |
| `Where` after `GroupBy` (filter on an aggregate) | Translated to `HAVING` — e.g. `GroupBy(x => x.K).Where(g => g.Count() > 5)`. A `Where` *before* `GroupBy` stays a `WHERE`. |

## ClickHouse-specific modifiers

| Method | SQL | Use case |
|---|---|---|
| `Final()` | `FROM table FINAL` | `ReplacingMergeTree` / `CollapsingMergeTree` — return only the final version of each row. |
| `Sample(double ratio)` | `SAMPLE 0.5` | Approximate query against a `SAMPLE BY`-keyed table. Ratio between 0 and 1. |
| `WithQueryId(string id)` | (sets the wire query_id) | Use a stable query ID for `KILL QUERY` / observability. Max length 128. |

```csharp
var top = connection.Table<Product>("products")
    .Final()
    .Where(p => p.Category == "Electronics")
    .OrderByDescending(p => p.Price)
    .Take(10);
```

## Async execution

The query is lazy until enumerated. To run it:

```csharp
// Streaming
await foreach (var p in query.AsAsyncEnumerable())
{
    Console.WriteLine(p.Name);
}

// Materialise
var list   = await query.ToListAsync();
var array  = await query.ToArrayAsync();
var first  = await query.FirstAsync();
var single = await query.SingleOrDefaultAsync();
var count  = await query.CountAsync();
var any    = await query.AnyAsync();
var sum    = await query.SumAsync(p => p.Price);
```

All async-execution extensions are in `CH.Native.Linq.AsyncQueryableExtensions` and accept an optional `CancellationToken`.

## Inserting via the table handle

The `IQueryable<T>` returned by `connection.Table<T>()` / `dataSource.Table<T>()` also supports `InsertAsync` for write paths. Three overloads cover the common shapes:

```csharp
var users = dataSource.Table<User>();

// Single record
await users.InsertAsync(new User { Id = 1, Name = "Alice" });

// In-memory collection
await users.InsertAsync(new[]
{
    new User { Id = 2, Name = "Bob" },
    new User { Id = 3, Name = "Charlie" },
});

// Async stream — preferred for large or unbounded sources
await users.InsertAsync(GenerateUsersAsync());
```

Under the hood `InsertAsync` delegates to the same `BulkInsertAsync<T>` plumbing as the native API — schema cache, role activation, query id, batch size, and telemetry are all inherited. An optional `BulkInsertOptions` and `CancellationToken` are accepted on every overload.

The single-record overload still opens a fresh INSERT context per call (handshake + commit), so callers on hot paths should prefer the collection or async-stream overload, or drop down to `BulkInserter<T>` directly. See [Bulk Insert](bulk-insert.md) for the full picture.

`InsertAsync` only works on queries created via `connection.Table<T>()` or `dataSource.Table<T>()` — calling it on an arbitrary `IQueryable<T>` throws.

## String operations

The visitor translates the standard `System.String` instance methods:

| .NET | ClickHouse SQL |
|---|---|
| `s.Contains(sub)` | `s LIKE '%sub%'` (special chars in `sub` escaped) |
| `s.StartsWith(p)` | `s LIKE 'p%'` |
| `s.EndsWith(p)` | `s LIKE '%p'` |
| `s.ToLower()` / `ToLowerInvariant()` | `lower(s)` |
| `s.ToUpper()` / `ToUpperInvariant()` | `upper(s)` |
| `s.Trim()` / `TrimStart()` / `TrimEnd()` | `trim(...)` / `trimLeft(...)` / `trimRight(...)` |

Collection `Contains` (`new[] { 1, 2, 3 }.Contains(p.Id)`) translates to `IN (…)` — this is the right way to express `IN` lists, since the parameterised raw-SQL path doesn't expand arrays for you.

## Debugging the generated SQL

`ToSql()` returns the SQL without executing the query — useful in tests and for sanity checks:

```csharp
var query = connection.Table<Product>("products")
    .Where(p => p.Category == "Electronics")
    .OrderByDescending(p => p.Price)
    .Take(3);

Console.WriteLine(query.ToSql());
// SELECT * FROM products WHERE category = 'Electronics' ORDER BY price DESC LIMIT 3
```

`ToSql()` only works on queries created via `connection.Table<T>()`; calling it on an arbitrary `IQueryable` throws.

## Limitations

The provider is **preview** and handles only the operators listed above. Anything else falls
outside scope and throws `NotSupportedException` **when the query is built (enumerated or
`ToSql()`-ed), not at compile time** — so unsupported code compiles cleanly and fails at runtime.
When in doubt, fall back to raw SQL.

| Not supported in LINQ | Workaround |
|---|---|
| `Join` / `GroupJoin` (multi-table) | Raw SQL with `connection.QueryStreamAsync<T>` |
| Subqueries / CTEs | Raw SQL |
| `Union` / `Intersect` / `Except` | Raw SQL or `UNION ALL` in raw SQL |
| `DateTime` parts beyond `.Year` / `.Month` / `.Day`, and date arithmetic | Raw SQL |
| Casts / conversions, and uncommon operators (bitwise, shift, power) | Raw SQL |
| User-defined SQL functions | Raw SQL, or extend `ClickHouseExpressionVisitor` |
| Server-side aggregations beyond Sum/Avg/Min/Max/Count | Raw SQL |

For anything raw, fall back to `connection.QueryStreamAsync<T>("…", new { p })` — see [ADO.NET & Dapper — parameter handling](ado-net-dapper.md) for the parameter forms.

## See also

- `samples/CH.Native.Samples.Queries` — runnable demo of `Where`, projections, `Final`, `Sample`, async aggregates, plus the broader query-flavour matrix (scalar, raw, typed, parameterised, ADO.NET, Dapper, pooled DataSource, resilient multi-host)
- [Data Types](data-types.md) — type mapping for projected DTOs
- [ADO.NET & Dapper](ado-net-dapper.md) — when to fall back to raw SQL
