# CH.Native.Dapper

Dapper integration for [CH.Native](https://www.nuget.org/packages/CH.Native), the high-performance .NET client for ClickHouse using the native binary TCP protocol.

By default, Dapper expands array parameters (`int[]`, `string[]`, etc.) into SQL tuples — fine for most databases, wrong for ClickHouse, where you usually want them bound as `Array(T)` on the wire. This package registers Dapper type handlers that send arrays as native ClickHouse arrays.

## Install

```bash
dotnet add package CH.Native.Dapper
```

## Usage

Call `Register()` once during startup:

```csharp
using CH.Native.Dapper;

ClickHouseDapperIntegration.Register();
```

Then use array parameters as you would expect:

```csharp
using var connection = new ClickHouseDbConnection("Host=localhost;Database=default");
await connection.OpenAsync();

var ids = new[] { 1, 2, 3, 4, 5 };
var rows = await connection.QueryAsync<MyRow>(
    "SELECT * FROM events WHERE id IN (SELECT arrayJoin(@ids))",
    new { ids });
```

Without this package, Dapper would rewrite `@ids` into `(@ids1, @ids2, ...)`. With it, `@ids` is sent as a single `Array(Int32)` parameter.

## Registered types

`bool[]`, `sbyte[]`, `short[]`, `int[]`, `long[]`, `byte[]`, `ushort[]`, `uint[]`, `ulong[]`, `float[]`, `double[]`, `decimal[]`, `string[]`, `Guid[]`, `DateTime[]`, `DateTimeOffset[]`, `DateOnly[]`.

`Register()` is idempotent — repeated calls are no-ops.

## License

MIT
