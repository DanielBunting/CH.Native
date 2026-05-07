using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.Mapping;

namespace CH.Native.Samples.Insert;

/// <summary>
/// <c>connection.Table&lt;T&gt;(name).InsertAsync(row)</c> — single-record insert via
/// the LINQ table handle. Models an audit-log scenario where events arrive one at
/// a time as user actions happen — too rare to batch, but each one matters.
/// </summary>
/// <remarks>
/// Each call opens a fresh INSERT context on the wire (handshake + per-block commit
/// + end-of-stream), so per-row overhead is non-trivial. Fine for occasional writes
/// like audit entries; reach for the IEnumerable&lt;T&gt; overload or
/// <c>BulkInserter&lt;T&gt;</c> on hot paths.
/// </remarks>
internal static class SingleRecordSample
{
    public static async Task RunAsync(string connectionString)
    {
        var tableName = $"sample_audit_{Guid.NewGuid():N}";

        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        try
        {
            // Realistic audit-log table: ordered by (actor, ts) so per-actor history
            // queries are cheap. LowCardinality on the action enum keeps it compact.
            await connection.ExecuteNonQueryAsync($"""
                CREATE TABLE {tableName} (
                    occurred_at DateTime64(3),
                    actor_id    UInt64,
                    action      LowCardinality(String),
                    target      String,
                    metadata    Map(String, String)
                ) ENGINE = MergeTree()
                ORDER BY (actor_id, occurred_at)
                """);
            Console.WriteLine($"Created audit table {tableName}");

            var table = connection.Table<AuditEvent>(tableName);

            // Three independent admin actions arriving as separate events. Each
            // InsertAsync call is its own INSERT round-trip — the API is the same
            // shape used in real audit code where each event is independent.
            await table.InsertAsync(new AuditEvent
            {
                OccurredAt = DateTime.UtcNow,
                ActorId = 1001,
                Action = "user.login",
                Target = "session#abc",
                Metadata = new Dictionary<string, string> { ["ip"] = "10.0.0.1", ["ua"] = "curl/8" }
            });

            await table.InsertAsync(new AuditEvent
            {
                OccurredAt = DateTime.UtcNow,
                ActorId = 1001,
                Action = "role.assign",
                Target = "user#42",
                Metadata = new Dictionary<string, string> { ["role"] = "admin" }
            });

            await table.InsertAsync(new AuditEvent
            {
                OccurredAt = DateTime.UtcNow,
                ActorId = 1002,
                Action = "user.logout",
                Target = "session#xyz",
                Metadata = new Dictionary<string, string>()
            });

            var total = await connection.ExecuteScalarAsync<ulong>($"SELECT count() FROM {tableName}");
            Console.WriteLine($"Inserted {total:N0} audit events.");

            Console.WriteLine("\n--- Events per actor ---");
            await foreach (var row in connection.QueryAsync(
                $"SELECT actor_id, count() AS n FROM {tableName} GROUP BY actor_id ORDER BY actor_id"))
            {
                Console.WriteLine($"  actor {row["actor_id"]}: {row["n"]} event(s)");
            }
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
            Console.WriteLine($"\nDropped table {tableName}");
        }
    }
}

file class AuditEvent
{
    [ClickHouseColumn(Name = "occurred_at", Order = 0)] public DateTime OccurredAt { get; set; }
    [ClickHouseColumn(Name = "actor_id", Order = 1)] public ulong ActorId { get; set; }
    [ClickHouseColumn(Name = "action", Order = 2)] public string Action { get; set; } = string.Empty;
    [ClickHouseColumn(Name = "target", Order = 3)] public string Target { get; set; } = string.Empty;
    [ClickHouseColumn(Name = "metadata", Order = 4)] public Dictionary<string, string> Metadata { get; set; } = new();
}
