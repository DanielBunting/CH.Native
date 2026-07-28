# CH.Native.Dapper

Dapper integration for [CH.Native](https://www.nuget.org/packages/CH.Native), the high-performance .NET client for ClickHouse using the native binary TCP protocol.

This package does two things: it makes the familiar Dapper query methods **faster** against ClickHouse, and it makes **array parameters** bind correctly.

## Install

```bash
dotnet add package CH.Native.Dapper
```

## Faster queries, same call shape

`QueryAsync<T>` and friends are extended on `ClickHouseConnection` and route
through CH.Native's typed-accessor read path rather than Dapper's compiled row
mapper. That skips the per-value boxing Dapper pays for value-type columns —
typically **30-40% lower allocations** on large reads — with no change at the
call site:

```csharp
using CH.Native.Connection;
using CH.Native.Dapper;

await using var connection = new ClickHouseConnection("Host=localhost;Database=default");
await connection.OpenAsync();

var users = await connection.QueryAsync<User>("SELECT id, name, age FROM users");
```

Available: `QueryAsync<T>`, `QueryFirstAsync<T>`, `QueryFirstOrDefaultAsync<T>`,
`QuerySingleAsync<T>`, `QuerySingleOrDefaultAsync<T>`.

**Which overload runs?** C# picks the more-derived receiver, so a variable typed
as `ClickHouseConnection` gets the fast path and anything typed as
`IDbConnection`/`DbConnection` falls through to Dapper's classic path. Row-shaped
methods are deliberately *not* extended on `IDbConnection`, so `using Dapper;`
and `using CH.Native.Dapper;` can coexist in the same file without ambiguity. If
your DI container only hands out `IDbConnection`, assign to a concrete local
first to become fast-path-eligible:

```csharp
ClickHouseConnection ch = await dataSource.OpenConnectionAsync();
var rows = await ch.QueryAsync<User>(sql);   // fast path
```

Execute-style methods (`ExecuteAsync`, `ExecuteScalarAsync`, and the sync
variants) are thin pass-throughs to Dapper, provided so a lone
`using CH.Native.Dapper;` still binds them. `QueryMultipleAsync` throws
`NotSupportedException` — ClickHouse has no multiple-result-set concept, so
failing at the call site beats an opaque server-side syntax error.

## Array parameters

By default, Dapper expands array parameters (`int[]`, `string[]`, etc.) into SQL tuples — fine for most databases, wrong for ClickHouse, where you usually want them bound as `Array(T)` on the wire. This package registers Dapper type handlers that send arrays as native ClickHouse arrays.

Call `Register()` once during startup:

```csharp
using CH.Native.Dapper;

ClickHouseDapperIntegration.Register();
```

Then use array parameters as you would expect:

```csharp
var ids = new[] { 1, 2, 3, 4, 5 };
var rows = await connection.QueryAsync<MyRow>(
    "SELECT * FROM events WHERE id IN (SELECT arrayJoin(@ids))",
    new { ids });
```

Without this package, Dapper would rewrite `@ids` into `(@ids1, @ids2, ...)`. With it, `@ids` is sent as a single `Array(Int32)` parameter.

### Registered types

`bool[]`, `sbyte[]`, `short[]`, `int[]`, `long[]`, `byte[]`, `ushort[]`, `uint[]`, `ulong[]`, `float[]`, `double[]`, `decimal[]`, `string[]`, `Guid[]`, `DateTime[]`, `DateTimeOffset[]`, `DateOnly[]`.

`Register()` is idempotent — repeated calls are no-ops.

## License

Apache-2.0
