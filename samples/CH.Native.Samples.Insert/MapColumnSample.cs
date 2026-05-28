using CH.Native.Connection;
using CH.Native.Data;
using CH.Native.Mapping;

namespace CH.Native.Samples.Insert;

/// <summary>
/// Inserting <c>Map(K, V)</c> columns. Demonstrates both CLR-side shapes the
/// library accepts on the writer side, writing into the same <c>Map(String, String)</c>
/// column:
/// <list type="bullet">
/// <item><c>Dictionary&lt;K, V&gt;</c> — the historical default. Entries are
/// inherently unique (the Dictionary itself rejects duplicate keys). Use for
/// routine writes where the Map is a key→value set.</item>
/// <item><c>ClickHouseMap&lt;K, V&gt;</c> — lossless. Preserves wire order and
/// duplicate keys. Use when entries are ordered observations or duplicates
/// carry information (e.g. multi-layer overrides, append-only audit data).</item>
/// </list>
/// Both POCOs target the same physical column — the choice is purely CLR-side
/// and per-call. The wire format is identical when no duplicates are present.
/// Models a feature-flag audit table where routine evaluations use a Dictionary
/// (unique flag values) but emergency-override events use a ClickHouseMap
/// (multiple values for the same flag preserved in application order).
/// </summary>
internal static class MapColumnSample
{
    public static async Task RunAsync(string connectionString)
    {
        var tableName = $"sample_flag_audit_{Guid.NewGuid():N}";

        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        try
        {
            await connection.ExecuteNonQueryAsync($"""
                CREATE TABLE {tableName} (
                    occurred_at DateTime64(3),
                    tenant_id   UInt64,
                    -- Flag overrides observed at evaluation time. Duplicates are
                    -- legal in ClickHouse Map(K, V) and meaningful in audit data.
                    overrides   Map(String, String)
                ) ENGINE = MergeTree()
                ORDER BY (tenant_id, occurred_at)
                """);
            Console.WriteLine($"Created audit table {tableName}");

            // --- Path 1: Dictionary-typed POCO --------------------------------
            // Routine evaluations: a flag has exactly one value per evaluation,
            // so Dictionary is the most natural CLR shape. The writer hits the
            // fast path (no per-row buffer allocation, byte-identical to the
            // pre-feature wire output).
            Console.WriteLine("\nWriting routine evaluations as Dictionary<string, string>...");
            await using (var inserter = connection.CreateBulkInserter<RoutineEvaluation>(tableName))
            {
                await inserter.InitAsync();
                await inserter.AddAsync(new RoutineEvaluation
                {
                    OccurredAt = DateTime.UtcNow,
                    TenantId = 4001,
                    Overrides = new Dictionary<string, string>
                    {
                        ["checkout.v2"] = "on",
                        ["dark-mode"] = "auto",
                    },
                });
                await inserter.AddAsync(new RoutineEvaluation
                {
                    OccurredAt = DateTime.UtcNow.AddSeconds(2),
                    TenantId = 4002,
                    Overrides = new Dictionary<string, string>(),
                });
                await inserter.CompleteAsync();
            }

            // --- Path 2: ClickHouseMap-typed POCO -----------------------------
            // Same table, same column, different POCO. Use this when the
            // sequence of overrides matters and duplicates need to survive on
            // the wire — e.g. a fallback layer set 'payments.timeout' to
            // '200ms', then an emergency override flipped it to '500ms' 30s
            // later. The audit needs both observations, in order.
            Console.WriteLine("Writing override-sequence events as ClickHouseMap<string, string>...");
            await using (var inserter = connection.CreateBulkInserter<OverrideSequenceEvent>(tableName))
            {
                await inserter.InitAsync();
                await inserter.AddAsync(new OverrideSequenceEvent
                {
                    OccurredAt = DateTime.UtcNow.AddSeconds(1),
                    TenantId = 4001,
                    Overrides = new ClickHouseMap<string, string>(new[]
                    {
                        new KeyValuePair<string, string>("payments.timeout", "200ms"),
                        new KeyValuePair<string, string>("payments.timeout", "500ms"),
                        new KeyValuePair<string, string>("checkout.v2", "on"),
                    }),
                });
                await inserter.CompleteAsync();
            }

            var total = await connection.ExecuteScalarAsync<ulong>($"SELECT count() FROM {tableName}");
            Console.WriteLine($"\nInserted {total:N0} audit events across both POCO shapes.");

            Console.WriteLine("\n--- Server-side proof of what each row stored ---");
            // length(overrides) reports the on-wire entry count — the Dictionary
            // rows show their natural unique-key counts; the ClickHouseMap row
            // shows 3 entries despite only 2 distinct keys (the duplicate survived).
            await foreach (var row in connection.QueryStreamAsync(
                $"SELECT tenant_id, occurred_at, length(overrides) AS entries " +
                $"FROM {tableName} ORDER BY occurred_at"))
            {
                Console.WriteLine($"  tenant {row["tenant_id"]} @ {row["occurred_at"]:HH:mm:ss.fff}: {row["entries"]} entries");
            }
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
            Console.WriteLine($"\nDropped table {tableName}");
        }
    }
}

// Routine writes: Dictionary is the natural shape. Hits the writer fast path.
file class RoutineEvaluation
{
    [ClickHouseColumn(Name = "occurred_at", Order = 0)] public DateTime OccurredAt { get; set; }
    [ClickHouseColumn(Name = "tenant_id", Order = 1)] public ulong TenantId { get; set; }
    [ClickHouseColumn(Name = "overrides", Order = 2)] public Dictionary<string, string> Overrides { get; set; } = new();
}

// Override-sequence writes: ClickHouseMap preserves duplicates and order.
file class OverrideSequenceEvent
{
    [ClickHouseColumn(Name = "occurred_at", Order = 0)] public DateTime OccurredAt { get; set; }
    [ClickHouseColumn(Name = "tenant_id", Order = 1)] public ulong TenantId { get; set; }
    [ClickHouseColumn(Name = "overrides", Order = 2)] public ClickHouseMap<string, string> Overrides { get; set; } = null!;
}
