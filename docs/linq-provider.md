# LINQ Provider

CH.Native ships a typed LINQ provider in `CH.Native.Linq` that translates LINQ expressions to ClickHouse SQL. It's designed for readable, single-table queries — for joins, CTEs, or anything not in the operator list below, fall back to `connection.QueryAsync<T>(sql)`.

## Entry point

The starting point is the `connection.Table<T>()` extension method. The connection must be open.

```csharp
using CH.Native.Connection;
using CH.Native.Linq;

await using var connection = new ClickHouseConnection("Host=localhost;Port=9000");
await connection.OpenAsync();

var users = connection.Table<User>();        // table name = "user" (snake_case of T)
var rooms = connection.Table<Room>("rooms"); // explicit override
```

`Table<T>()` resolves the table name from the entity type via `TableNameResolver` — it lowercases and snake-cases the type name (e.g. `OrderItem` → `order_item`). Pass an explicit table name when the convention doesn't fit. Column-name mapping uses `[ClickHouseColumn(Name = "…")]`, the same as the bulk-insert and read paths.

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

## String operations

The visitor translates the standard `System.String` instance methods:

| .NET | ClickHouse SQL |
|---|---|
| `s.Contains(sub)` | `position(s, sub) > 0` |
| `s.StartsWith(p)` | `startsWith(s, p)` |
| `s.EndsWith(p)` | `endsWith(s, p)` |
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

The visitor handles the operators above. Anything else falls outside scope:

| Not supported in LINQ | Workaround |
|---|---|
| `Join` (multi-table) | Raw SQL with `connection.QueryAsync<T>` |
| Subqueries / CTEs | Raw SQL |
| `Union` / `Intersect` | Raw SQL or `UNION ALL` in raw SQL |
| User-defined SQL functions | Raw SQL, or extend `ClickHouseExpressionVisitor` |
| Server-side aggregations beyond Sum/Avg/Min/Max/Count | Raw SQL |

For anything raw, fall back to `connection.QueryAsync<T>("…", new { p })` — see [ADO.NET & Dapper — parameter handling](ado-net-dapper.md) for the parameter forms.

## See also

- `samples/CH.Native.Samples.LinqQueries` — runnable demo of `Where`, projections, `Final`, `Sample`, async aggregates
- [Data Types](data-types.md) — type mapping for projected DTOs
- [ADO.NET & Dapper](ado-net-dapper.md) — when to fall back to raw SQL
