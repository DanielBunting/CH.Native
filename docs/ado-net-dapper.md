# ADO.NET & Dapper

CH.Native provides a standard ADO.NET provider for compatibility with existing .NET data access patterns and ORMs like Dapper.

## ADO.NET Provider

### Classes

| Class | Base Class | Description |
|-------|------------|-------------|
| `ClickHouseDbConnection` | DbConnection | Database connection |
| `ClickHouseDbCommand` | DbCommand | SQL command |
| `ClickHouseDbDataReader` | DbDataReader | Forward-only result reader |
| `ClickHouseDbParameter` | DbParameter | Query parameter |
| `ClickHouseDbParameterCollection` | DbParameterCollection | Parameter collection |

### Basic Usage

```csharp
using CH.Native.Ado;

await using var connection = new ClickHouseDbConnection("Host=localhost;Port=9000");
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

cmd.Parameters.Add(new ClickHouseDbParameter
{
    ParameterName = "minAge",
    Value = 18
});

cmd.Parameters.Add(new ClickHouseDbParameter
{
    ParameterName = "status",
    Value = "active"
});

using var reader = await cmd.ExecuteReaderAsync();
```

### Reaching Native Features

The ADO.NET surface (`ClickHouseDbConnection` / `DbCommand` / `DbDataReader`) is intentionally narrow — it covers what Dapper, EF Core scaffolding, and other ADO.NET consumers expect, and nothing more. For native-only capabilities (`BulkInserter<T>`, `DynamicBulkInserter`, `IAsyncEnumerable<T>` streaming, LINQ via `connection.Table<T>()`, role activation, etc.), open a `ClickHouseConnection` directly using the same connection string:

```csharp
const string ConnectionString = "Host=localhost;Port=9000";

// ADO.NET path (Dapper, scaffolding)
await using var dbConnection = new ClickHouseDbConnection(ConnectionString);
await dbConnection.OpenAsync();
var user = await dbConnection.QueryFirstAsync<User>(
    "SELECT * FROM users WHERE id = @id", new { id = 1 });

// Native path for bulk insert
await using var nativeConnection = new ClickHouseConnection(ConnectionString);
await nativeConnection.OpenAsync();
await nativeConnection.BulkInsertAsync("users", newUsers);
```

In a long-running service, share a single `ClickHouseDataSource` across both paths instead of opening a fresh connection each time — see [Connection Pooling](connection-pooling.md) and [Dependency Injection](dependency-injection.md).

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
// Prefer CH.Native.Dapper over `using Dapper;` — it provides drop-in
// replacements for the methods you'd otherwise import from Dapper, plus
// the fast-path Query<T> family for ClickHouse connections.
using CH.Native.Dapper;

await using var connection = new ClickHouseDbConnection("Host=localhost;Port=9000");
await connection.OpenAsync();
```

### Resolution rules — which `QueryAsync<T>` runs?

The fast path lives in `CH.Native.Dapper`. C# extension-method resolution picks
the more-derived receiver first, so the rules are:

| Variable typed as | Imports | Result |
|---|---|---|
| `ClickHouseDbConnection` | either / both | **Always fast path** — `CH.Native.Dapper.ClickHouseDbConnectionDapperExtensions` wins via "more derived type" rule |
| `IDbConnection` or `DbConnection` | `using CH.Native.Dapper;` only | **Fast path via runtime dispatch** — `IDbConnectionDapperExtensions` checks the receiver type and routes ClickHouse connections to the fast path; everything else delegates to `Dapper.SqlMapper` |
| `IDbConnection` or `DbConnection` | `using Dapper;` only | **Dapper's classic path** (with per-value boxing) |
| `IDbConnection` or `DbConnection` | both `using Dapper;` AND `using CH.Native.Dapper;` | **Compile-time ambiguity error** — import one or the other, not both |

For DI scenarios where the connection is registered as `IDbConnection`, the
recommendation is: **replace `using Dapper;` with `using CH.Native.Dapper;`**
in the consuming files. `CH.Native.Dapper` re-exports the rest of Dapper's
surface (`ExecuteAsync`, `ExecuteScalarAsync`, `QueryMultipleAsync`, dynamic
`QueryAsync`, all sync variants) as thin delegates, so no other call sites
need to change.

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

The two connection types don't share a pool — `ClickHouseDbConnection` opens its own socket from a connection string, and `ClickHouseDataSource` pools `ClickHouseConnection` instances for the native API. Pick one per code path:

```csharp
const string ConnectionString = "Host=localhost;Port=9000";

// Dapper / ADO.NET path: a fresh DbConnection per unit of work.
await using (var dbConnection = new ClickHouseDbConnection(ConnectionString))
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

If your hot path is Dapper, keep using `ClickHouseDbConnection`; reach for the native side only where it pays off (bulk insert, streaming, LINQ).

## See Also

- [Quick Start](quickstart.md) - Getting started
- [Bulk Insert](bulk-insert.md) - High-performance data loading
- [Data Types](data-types.md) - Type mapping reference
