// CH.Native QuickStart sample — runnable mirror of docs/quickstart.md.
// If you change one, change the other. The doc is the canonical narrative;
// this file is the executable counterpart.
//
//   docker run -d -p 9000:9000 --name ch-qs clickhouse/clickhouse-server
//   dotnet run --project samples/CH.Native.Samples.QuickStart
//   docker rm -f ch-qs
//
// Override the host with CH_HOST / CH_PORT environment variables.

using CH.Native.Connection;
using CH.Native.Mapping;

var host = Environment.GetEnvironmentVariable("CH_HOST") ?? "localhost";
var port = int.TryParse(Environment.GetEnvironmentVariable("CH_PORT"), out var p) ? p : 9000;

await using var connection = new ClickHouseConnection($"Host={host};Port={port}");
await connection.OpenAsync();

// 1. Scalar query
var sum = await connection.ExecuteScalarAsync<int>("SELECT 1 + 1");
Console.WriteLine($"SELECT 1 + 1 = {sum}");

// 2. Create a table
await connection.ExecuteNonQueryAsync("""
    CREATE TABLE IF NOT EXISTS quickstart_users (
        id UInt32,
        name String,
        created DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY id
    """);

try
{
    // 3. Bulk-insert three rows
    var seed = new List<User>
    {
        new() { Id = 1, Name = "Alice" },
        new() { Id = 2, Name = "Bob" },
        new() { Id = 3, Name = "Charlie" },
    };

    await using (var inserter = connection.CreateBulkInserter<User>("quickstart_users"))
    {
        await inserter.InitAsync();
        await inserter.AddRangeAsync(seed);
        await inserter.CompleteAsync();
    }
    Console.WriteLine($"Inserted {seed.Count} users.");

    // 4. Typed query
    Console.WriteLine("Users:");
    await foreach (var user in connection.QueryAsync<User>(
        "SELECT id, name, created FROM quickstart_users ORDER BY id"))
    {
        Console.WriteLine($"  {user.Id}: {user.Name} (created {user.Created:O})");
    }
}
finally
{
    await connection.ExecuteNonQueryAsync("DROP TABLE IF EXISTS quickstart_users");
}

internal sealed class User
{
    [ClickHouseColumn(Name = "id")] public uint Id { get; set; }
    [ClickHouseColumn(Name = "name")] public string Name { get; set; } = "";
    [ClickHouseColumn(Name = "created")] public DateTime Created { get; set; }
}
