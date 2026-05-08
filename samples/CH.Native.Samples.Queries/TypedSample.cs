using CH.Native.Connection;
using CH.Native.Mapping;

namespace CH.Native.Samples.Queries;

/// <summary>
/// <c>connection.QueryAsync&lt;T&gt;(sql)</c> — reflection-based POCO mapping over
/// streamed rows. Models a user-listing service that hands a typed
/// <c>IAsyncEnumerable&lt;User&gt;</c> back to a presentation layer.
/// </summary>
/// <remarks>
/// The everyday read shape — pair a SELECT with a POCO carrying
/// <c>[ClickHouseColumn(Name = "...")]</c> attributes and consume via
/// <c>await foreach</c>. The mapper builds a per-shape delegate the first time
/// it sees a result set; subsequent runs of the same query are amortised.
/// </remarks>
internal static class TypedSample
{
    public static async Task RunAsync(string connectionString)
    {
        var tableName = $"sample_users_{Guid.NewGuid():N}";

        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        try
        {
            await connection.ExecuteNonQueryAsync($"""
                CREATE TABLE {tableName} (
                    id         UInt32,
                    name       String,
                    email      String,
                    active     UInt8,
                    created_at DateTime,
                    score      Float64
                ) ENGINE = MergeTree()
                ORDER BY id
                """);

            await connection.ExecuteNonQueryAsync($"""
                INSERT INTO {tableName} VALUES
                    (1, 'Alice',   'alice@example.com',   1, '2026-01-04 10:00:00', 87.5),
                    (2, 'Bob',     'bob@example.com',     0, '2026-01-12 14:30:00', 62.1),
                    (3, 'Carol',   'carol@example.com',   1, '2026-02-01 09:15:00', 95.0),
                    (4, 'Dave',    'dave@example.com',    1, '2026-02-22 18:45:00', 71.9),
                    (5, 'Eve',     'eve@example.com',     0, '2026-03-09 12:00:00', 43.2),
                    (6, 'Frank',   'frank@example.com',   1, '2026-03-15 11:11:00', 80.0),
                    (7, 'Grace',   'grace@example.com',   1, '2026-04-04 16:20:00', 88.8),
                    (8, 'Heidi',   'heidi@example.com',   0, '2026-04-21 08:05:00', 50.5)
                """);
            Console.WriteLine($"Seeded {tableName} with 8 users");

            // Pull the active users, most recent first, mapped straight onto the
            // POCO. Note the WHERE / ORDER BY happen server-side — the consumer
            // never sees the inactive rows. Plumbing demonstrated:
            //   - parameterised filter (@minScore)
            //   - CancellationToken threaded into QueryAsync<T>
            //   - explicit queryId on a follow-up non-parameterised call
            using var cts = new CancellationTokenSource();
            var minScore = 70.0;
            var sql = $"""
                SELECT id, name, email, active, created_at, score
                FROM {tableName}
                WHERE active = 1 AND score >= @minScore
                ORDER BY created_at DESC
                LIMIT 100
                """;

            Console.WriteLine();
            Console.WriteLine($"--- Active users with score >= {minScore} (parameterised) ---");
            await foreach (var user in connection.QueryAsync<User>(sql, new { minScore }, cts.Token))
            {
                Console.WriteLine(
                    $"  [{user.Id}] {user.Name,-7} {user.Email,-22} " +
                    $"joined {user.CreatedAt:yyyy-MM-dd}  score={user.Score:F1}");
            }

            // Same shape via the non-parameterised overload — this one accepts a
            // queryId we can echo back via connection.LastQueryId.
            var queryId = $"typed-recent-active-{Guid.NewGuid():N}";
            var recent = new List<User>();
            await foreach (var user in connection.QueryAsync<User>(
                $"SELECT id, name, email, active, created_at, score FROM {tableName} WHERE active = 1 ORDER BY created_at DESC LIMIT 3",
                cts.Token,
                queryId: queryId))
            {
                recent.Add(user);
            }

            Console.WriteLine();
            Console.WriteLine("--- Plumbing check ---");
            Console.WriteLine($"  Parameters bound   : @minScore = {minScore}");
            Console.WriteLine($"  Most-recent active : {string.Join(", ", recent.ConvertAll(u => u.Name))}");
            Console.WriteLine($"  queryId sent       : {queryId}");
            Console.WriteLine($"  queryId echoed     : {connection.LastQueryId}");
            Console.WriteLine($"  Cancellation token : threaded via cts.Token");
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
            Console.WriteLine($"\nDropped table {tableName}");
        }
    }
}

file class User
{
    [ClickHouseColumn(Name = "id")] public uint Id { get; set; }
    [ClickHouseColumn(Name = "name")] public string Name { get; set; } = string.Empty;
    [ClickHouseColumn(Name = "email")] public string Email { get; set; } = string.Empty;
    [ClickHouseColumn(Name = "active")] public byte Active { get; set; }
    [ClickHouseColumn(Name = "created_at")] public DateTime CreatedAt { get; set; }
    [ClickHouseColumn(Name = "score")] public double Score { get; set; }
}
