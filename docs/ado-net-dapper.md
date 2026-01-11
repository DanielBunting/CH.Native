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
| `Inner` | Access underlying `ClickHouseConnection` |

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

### Accessing Native Connection

For advanced features, access the underlying native connection:

```csharp
var dbConnection = new ClickHouseDbConnection("Host=localhost;Port=9000");
await dbConnection.OpenAsync();

// Access native connection
ClickHouseConnection native = dbConnection.Inner;

// Use native bulk insert
await using var inserter = native.CreateBulkInserter<User>("users");
await inserter.InitAsync();
await inserter.AddRangeAsync(users);
await inserter.CompleteAsync();
```

## Dapper Integration

CH.Native works with [Dapper](https://github.com/DapperLib/Dapper) for simple object mapping.

### Setup

```csharp
using CH.Native.Ado;
using Dapper;

await using var connection = new ClickHouseDbConnection("Host=localhost;Port=9000");
await connection.OpenAsync();
```

### Query

```csharp
public class User
{
    public uint Id { get; set; }
    public string Name { get; set; } = "";
    public int Age { get; set; }
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

**Workaround:** Use the native API for array parameters:

```csharp
// Use native connection for array parameters
var native = ((ClickHouseDbConnection)connection).Inner;

var users = await native.QueryAsync<User>(
    "SELECT * FROM users WHERE id IN @ids",
    new { ids = new[] { 1, 2, 3 } }
).ToListAsync();
```

Or construct the query manually:

```csharp
var ids = new[] { 1, 2, 3 };
var inClause = string.Join(", ", ids);
var users = await connection.QueryAsync<User>(
    $"SELECT * FROM users WHERE id IN ({inClause})"
);
```

## LINQ Provider

CH.Native includes a basic LINQ provider for simple queries.

### Setup

```csharp
using CH.Native.Connection;
using CH.Native.Linq;

await using var connection = new ClickHouseConnection("Host=localhost;Port=9000");
await connection.OpenAsync();

var query = ClickHouseQueryable.Create<User>(connection);
```

### Supported Operations

```csharp
// Where
await foreach (var user in query.Where(u => u.Age > 18))
{
    Console.WriteLine(user.Name);
}

// Select
var names = query.Select(u => u.Name);

// OrderBy
var ordered = query.OrderBy(u => u.Name);

// Multiple conditions
var adults = query
    .Where(u => u.Age >= 18)
    .Where(u => u.Status == "active")
    .OrderBy(u => u.Name);

await foreach (var user in adults)
{
    Console.WriteLine($"{user.Name}: {user.Age}");
}
```

### Debugging Queries

View the generated SQL:

```csharp
var query = ClickHouseQueryable.Create<User>(connection)
    .Where(u => u.Age > 18)
    .OrderBy(u => u.Name);

string sql = query.ToSql();
Console.WriteLine(sql);
// Output: SELECT * FROM users WHERE age > 18 ORDER BY name
```

### Limitations

The LINQ provider supports basic operations only:

| Supported | Not Supported |
|-----------|---------------|
| Where | Join |
| Select | GroupBy |
| OrderBy/OrderByDescending | Aggregate functions |
| Take/Skip | Union/Intersect |
| First/FirstOrDefault | Complex subqueries |

For complex queries, use raw SQL with `QueryAsync<T>()`.

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

```csharp
await using var dbConnection = new ClickHouseDbConnection("Host=localhost;Port=9000");
await dbConnection.OpenAsync();

// Use Dapper for simple queries
var user = await dbConnection.QueryFirstAsync<User>(
    "SELECT * FROM users WHERE id = @id",
    new { id = 1 }
);

// Use native API for bulk insert
var native = dbConnection.Inner;
await using var inserter = native.CreateBulkInserter<User>("users");
await inserter.InitAsync();
await inserter.AddRangeAsync(newUsers);
await inserter.CompleteAsync();

// Use native API for streaming large results
await foreach (var row in native.QueryAsync<LogEntry>("SELECT * FROM logs WHERE date = today()"))
{
    ProcessLog(row);
}
```

## See Also

- [Quick Start](quickstart.md) - Getting started
- [Bulk Insert](bulk-insert.md) - High-performance data loading
- [Data Types](data-types.md) - Type mapping reference
