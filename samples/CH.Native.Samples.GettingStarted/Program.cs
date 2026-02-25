using CH.Native.Connection;
using CH.Native.Mapping;

var connectionString = args.Length > 0 ? args[0] : "Host=localhost;Port=9000";
var tableName = $"sample_users_{Guid.NewGuid():N}";

await using var connection = new ClickHouseConnection(connectionString);
await connection.OpenAsync();
Console.WriteLine($"Connected to ClickHouse {connection.ServerInfo?.VersionMajor}.{connection.ServerInfo?.VersionMinor}");

try
{
    // Create a table
    await connection.ExecuteNonQueryAsync($"""
        CREATE TABLE {tableName} (
            id    UInt32,
            name  String,
            email String,
            age   UInt8
        ) ENGINE = MergeTree()
        ORDER BY id
        """);
    Console.WriteLine($"Created table {tableName}");

    // Insert rows
    await connection.ExecuteNonQueryAsync($"""
        INSERT INTO {tableName} VALUES
            (1, 'Alice',   'alice@example.com',   30),
            (2, 'Bob',     'bob@example.com',     25),
            (3, 'Charlie', 'charlie@example.com', 35)
        """);
    Console.WriteLine("Inserted 3 rows");

    // Scalar query
    var count = await connection.ExecuteScalarAsync<ulong>($"SELECT count() FROM {tableName}");
    Console.WriteLine($"Row count: {count}");

    // Untyped row iteration
    Console.WriteLine("\n--- Untyped rows ---");
    await foreach (var row in connection.QueryAsync($"SELECT id, name, email, age FROM {tableName} ORDER BY id"))
    {
        Console.WriteLine($"  id={row["id"]}, name={row["name"]}, email={row["email"]}, age={row["age"]}");
    }

    // Typed query with POCO mapping
    Console.WriteLine("\n--- Typed rows ---");
    await foreach (var user in connection.QueryAsync<User>($"SELECT id, name, email, age FROM {tableName} ORDER BY id"))
    {
        Console.WriteLine($"  User {{ Id={user.Id}, Name={user.Name}, Email={user.Email}, Age={user.Age} }}");
    }
}
finally
{
    await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
    Console.WriteLine($"\nDropped table {tableName}");
}

public class User
{
    [ClickHouseColumn(Name = "id")]
    public uint Id { get; set; }

    [ClickHouseColumn(Name = "name")]
    public string Name { get; set; } = "";

    [ClickHouseColumn(Name = "email")]
    public string Email { get; set; } = "";

    [ClickHouseColumn(Name = "age")]
    public byte Age { get; set; }
}
