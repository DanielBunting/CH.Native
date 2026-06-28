# ADO.NET & Dapper

CH.Native provides a standard ADO.NET provider for compatibility with existing .NET data access patterns and ORMs like Dapper.

## ADO.NET Provider

### Classes

| Class | Base Class | Description |
|-------|------------|-------------|
| `ClickHouseConnection` | DbConnection | Database connection |
| `ClickHouseCommand` | DbCommand | SQL command |
| `ClickHouseDataReader` | DbDataReader | Forward-only result reader |
| `ClickHouseDbParameter` | DbParameter | Query parameter |
| `ClickHouseDbParameterCollection` | DbParameterCollection | Parameter collection |

### Basic Usage

```csharp
using CH.Native.Ado;

await using var connection = new ClickHouseConnection("Host=localhost;Port=9000");
await connection.OpenAsync();

// ExecuteScalar
using var cmd = connection.CreateCommand();
cmd.CommandText = "SELECT count() FROM users";
var count = await cmd.ExecuteScalarAsync();

// ExecuteNonQuery
cmd.CommandText = "INSERT INTO users (id, name) VALUES (1, 'Alice')";
await cmd.ExecuteNonQueryAsync();

// ExecuteReader
cmd.CommandText = "SELECT id, name FROM users";
using var reader = await cmd.ExecuteReaderAsync();
while (await reader.ReadAsync())
{
    var id = reader.GetInt32(0);
    var name = reader.GetString(1);
    Console.WriteLine($"{id}: {name}");
}
```

### Connection Properties

| Property | Description |
|----------|-------------|
| `ConnectionString` | Get/set connection string |
| `Database` | Current database name |
| `DataSource` | Server host:port |
| `ServerVersion` | Server version (major.minor) |
| `State` | ConnectionState (Open, Closed, etc.) |

### Parameters

```csharp
using var cmd = connection.CreateCommand();
cmd.CommandText = "SELECT * FROM users WHERE age > @minAge AND status = @status";

// Name/value overload on the native parameter collection.
cmd.Parameters.Add("minAge", 18);
cmd.Parameters.Add("status", "active");

using var reader = await cmd.ExecuteReaderAsync();
```

When the command is typed as `DbCommand` (e.g. obtained via the ADO.NET `DbConnection`
surface), use the standard `CreateParameter()` / `DbParameterCollection.Add` shape instead:

```csharp
var p = cmd.CreateParameter();
p.ParameterName = "minAge";
p.Value = 18;
cmd.Parameters.Add(p);
```

### One connection, both surfaces

`ClickHouseConnection` exposes both the ADO.NET contract (it inherits `DbConnection`, and `CreateCommand()` returns a `ClickHouseCommand : DbCommand` with `Parameters` on the DbCommand side) **and** the native API (`BulkInsertAsync`, `CreateBulkInserter<T>`, `QueryStreamAsync<T>` / `QueryTypedAsync<T>`, role activation, LINQ via `connection.Table<T>()`). Use the same instance for both:

```csharp
const string ConnectionString = "Host=localhost;Port=9000";

await using var connection = new ClickHouseConnection(ConnectionString);
await connection.OpenAsync();

// ADO.NET / Dapper
var user = await connection.QueryFirstAsync<User>(
    "SELECT * FROM users WHERE id = @id", new { id = 1 });

// Native bulk insert on the same physical socket
await connection.BulkInsertAsync("users", newUsers);
```

In a long-running service, share a single `ClickHouseDataSource` so each unit of work rents a pooled connection — see [Connection Pooling](connection-pooling.md) and [Dependency Injection](dependency-injection.md).

## Dapper Integration

CH.Native ships a dedicated package — **`CH.Native.Dapper`** — that exposes the
familiar Dapper API (`QueryAsync<T>`, `QueryFirstAsync<T>`, `ExecuteAsync`, …)
on top of CH.Native's typed-accessor read path. Compared to vanilla Dapper over
our ADO.NET surface it skips the per-value boxing tax that Dapper's compiled
mapper pays for value-type columns — typically **30-40% lower allocations** on
1M-row reads while keeping the exact same call shape.

### Setup

```csharp
using CH.Native.Ado;
using CH.Native.Dapper;
// `using Dapper;` is fine alongside CH.Native.Dapper — they no longer collide
// on row-shaped methods.
using Dapper;

await using var connection = new ClickHouseConnection("Host=localhost;Port=9000");
await connection.OpenAsync();
```

### Resolution rules — which `QueryAsync<T>` runs?

The fast path is exposed only on the concrete CH connection types. C# extension
resolution picks the more-derived receiver first, so the rules are simple:

| Variable typed as | Result |
|---|---|
| `ClickHouseConnection` (whether via `new ClickHouseConnection(...)` or `await ds.OpenConnectionAsync()`) | **Fast path** — `ClickHouseConnectionDapperExtensions.QueryAsync<T>` wins via more-derived receiver |
| `IDbConnection` or `DbConnection` | **Dapper's classic path** — `Dapper.SqlMapper.QueryAsync<T>` (per-value boxing) |

`using Dapper;` and `using CH.Native.Dapper;` can coexist freely. CH.Native.Dapper
no longer extends `IDbConnection` with row-shaped methods (`QueryAsync<T>` etc.),
so there is no ambiguity to resolve. Execute-style methods (`ExecuteAsync`,
`ExecuteScalarAsync`) remain as thin pass-throughs to Dapper for namespace-import
convenience. `QueryMultipleAsync` is the exception: ClickHouse has no
multiple-result-set concept, so CH.Native.Dapper's overload throws
`NotSupportedException` immediately rather than letting the multi-statement SQL
reach the server and fail with an opaque syntax error. Issue separate queries instead.

If a DI registration only hands out `IDbConnection`, fast-path resolution
requires assigning to a concrete-type local first:

```csharp
ClickHouseConnection ch = await ds.OpenConnectionAsync(); // fast-path-eligible
var rows = await ch.QueryAsync<User>(sql);                // CH.Native.Dapper fast path
```

### Query

```csharp
using CH.Native.Mapping;

public class User
{
    // The [ClickHouseColumn] attributes are only needed for the native bulk-insert
    // path (it sends column identifiers to the server case-sensitively, so PascalCase
    // properties don't auto-match snake_case columns). Dapper itself maps by name
    // case-insensitively and would work without them.
    [ClickHouseColumn(Name = "id")]   public uint Id { get; set; }
    [ClickHouseColumn(Name = "name")] public string Name { get; set; } = "";
    [ClickHouseColumn(Name = "age")]  public int Age { get; set; }
}

// Query multiple rows
var users = await connection.QueryAsync<User>("SELECT id, name, age FROM users");

// Query single row
var user = await connection.QueryFirstOrDefaultAsync<User>(
    "SELECT id, name, age FROM users WHERE id = @id",
    new { id = 1 }
);

// Query scalar
var count = await connection.ExecuteScalarAsync<long>("SELECT count() FROM users");
```

### Execute

```csharp
// Insert
await connection.ExecuteAsync(
    "INSERT INTO users (id, name, age) VALUES (@id, @name, @age)",
    new { id = 1, name = "Alice", age = 30 }
);

// DDL
await connection.ExecuteAsync(@"
    CREATE TABLE IF NOT EXISTS users (
        id UInt32,
        name String,
        age Int32
    ) ENGINE = Memory
");
```

### Mapping Conventions

Dapper maps columns to properties by name (case-insensitive):

```sql
SELECT user_id, full_name, created_at FROM users
```

```csharp
public class User
{
    public uint UserId { get; set; }     // Maps to user_id
    public string FullName { get; set; } // Maps to full_name
    public DateTime CreatedAt { get; set; } // Maps to created_at
}
```

Or use column aliases:

```sql
SELECT id AS UserId, name AS FullName FROM users
```

## Parameter Handling

### Reserved parameter names (`limit` / `offset`)

ClickHouse 26.x's parser misinterprets `{limit:Type}` and `{offset:Type}` as the
start of a LIMIT/OFFSET clause and rejects the query with
`CANNOT_PARSE_QUOTED_STRING: expected opening quote ''', got '1'`. Other SQL
keywords (`select`, `from`, `where`, `group`, `order`, …) work fine as
placeholder names — only the two tail-clause keywords that take a numeric
argument trip the parser.

CH.Native fails fast with a clear `ArgumentException` if you try either name,
so you'll see a descriptive error instead of the cryptic server message.
Rename the parameter:

```csharp
// Don't:  new { limit = 10, offset = 5 }
// Do:     new { max_rows = 10, start_at = 5 }
await connection.QueryAsync<Row>(
    "SELECT * FROM events ORDER BY ts LIMIT @max_rows OFFSET @start_at",
    new { max_rows = 10, start_at = 5 });
```

### Supported Parameter Types

| .NET Type | ClickHouse Type |
|-----------|-----------------|
| int, long, short, byte | Int32, Int64, Int16, UInt8, etc. |
| float, double | Float32, Float64 |
| decimal | Decimal |
| string | String |
| bool | Bool |
| DateTime | DateTime |
| DateTimeOffset | DateTime |
| Guid | UUID |
| byte[] | String (hex) |

### Dapper Array Parameters

**Important:** Dapper's default array expansion (IN clauses) does not work correctly with ClickHouse:

```csharp
// This will NOT work as expected with Dapper
var ids = new[] { 1, 2, 3 };
var users = await connection.QueryAsync<User>(
    "SELECT * FROM users WHERE id IN @ids",  // Dapper expands this incorrectly
    new { ids }
);
```

**Workaround:** open a native `ClickHouseConnection` for the array-parameter call — the native API expands arrays correctly:

```csharp
await using var native = new ClickHouseConnection(ConnectionString);
await native.OpenAsync();

var users = await native.QueryStreamAsync<User>(
    "SELECT * FROM users WHERE id IN @ids",
    new { ids = new[] { 1, 2, 3 } }
).ToListAsync();
```

Or build the `IN` list manually on the ADO.NET path:

```csharp
var ids = new[] { 1, 2, 3 };
var inClause = string.Join(", ", ids);
var users = await connection.QueryAsync<User>(
    $"SELECT * FROM users WHERE id IN ({inClause})"
);
```

## LINQ Provider

CH.Native includes a typed LINQ provider for simple queries:

```csharp
using CH.Native.Connection;
using CH.Native.Linq;

await using var connection = new ClickHouseConnection("Host=localhost;Port=9000");
await connection.OpenAsync();

await foreach (var user in connection.Table<User>()
    .Where(u => u.Age > 18)
    .OrderBy(u => u.Name)
    .AsAsyncEnumerable())
{
    Console.WriteLine(user.Name);
}
```

The entry point is the `connection.Table<T>()` extension (or `connection.Table<T>("table_name")` to override the table). See **[LINQ Provider](linq-provider.md)** for the full operator list, modifiers (`Final()`, `Sample()`, `WithQueryId()`), async-execution extensions, `ToSql()` debugging, and limitations.

## Comparison: ADO.NET vs Native API

| Feature | ADO.NET | Native API |
|---------|---------|------------|
| ORM compatibility | Full | Limited |
| Dapper support | Yes | N/A |
| Array parameters | Limited | Full |
| Bulk insert | Via Execute | Optimized BulkInserter |
| Streaming results | DbDataReader | IAsyncEnumerable |
| Performance | Good | Best |

**Recommendation:**

- Use ADO.NET/Dapper for simple CRUD and ORM integration
- Use native API for bulk operations and complex queries

## Example: Mixed Usage

There's a single connection type — `ClickHouseConnection` exposes both the ADO.NET/Dapper surface and the native API — but two ways to obtain one. Constructing it directly (`new ClickHouseConnection(connectionString)`) opens its own socket and is **not** pooled; `ClickHouseDataSource` pools `ClickHouseConnection` instances and is the recommended choice for services. A directly-constructed connection does not draw from the data source's pool. Pick whichever fits the code path:

```csharp
const string ConnectionString = "Host=localhost;Port=9000";

// Dapper / ADO.NET path: a fresh DbConnection per unit of work.
await using (var dbConnection = new ClickHouseConnection(ConnectionString))
{
    await dbConnection.OpenAsync();
    var user = await dbConnection.QueryFirstAsync<User>(
        "SELECT * FROM users WHERE id = @id", new { id = 1 });
}

// Native path: pool with ClickHouseDataSource (recommended for services).
await using var dataSource = new ClickHouseDataSource(new ClickHouseDataSourceOptions
{
    Settings = ClickHouseConnectionSettings.Parse(ConnectionString),
});

await using (var native = await dataSource.OpenConnectionAsync())
{
    await native.BulkInsertAsync("users", newUsers);
}

await using (var native = await dataSource.OpenConnectionAsync())
{
    await foreach (var row in native.QueryStreamAsync<LogEntry>(
        "SELECT * FROM logs WHERE date = today()"))
    {
        ProcessLog(row);
    }
}
```

If your hot path is Dapper, keep using `ClickHouseConnection`; reach for the native side only where it pays off (bulk insert, streaming, LINQ).

## See Also

- [Quick Start](quickstart.md) - Getting started
- [Bulk Insert](bulk-insert.md) - High-performance data loading
- [Data Types](data-types.md) - Type mapping reference
