using CH.Native.Connection;
using CH.Native.Data;
using CH.Native.Mapping;

namespace CH.Native.Samples.Queries;

/// <summary>
/// Reading <c>Map(K, V)</c> columns. Demonstrates the two CLR-side shapes
/// available to consumers:
/// <list type="bullet">
/// <item><c>Dictionary&lt;K, V&gt;</c> — the historical default. Duplicate keys
/// on the wire collapse last-wins. Use when entries are semantically a set and
/// you don't care about wire order.</item>
/// <item><c>ClickHouseMap&lt;K, V&gt;</c> — lossless. Preserves wire order and
/// every duplicate-key entry. Use when entries are ordered observations,
/// append-only audit data, or when first-wins lookup matters (matches
/// ClickHouse's own <c>m[k]</c> semantics).</item>
/// </list>
/// Selection is per-typed-call-site: declare the property/scalar T as the shape
/// you want, and the library wires the matching reader transparently — no
/// connection-string or registration change required.
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
                    tenant_id UInt64,
                    overrides Map(String, String)
                ) ENGINE = MergeTree()
                ORDER BY tenant_id
                """);

            // Seed: two tenants. Tenant 4001's overrides include a duplicate key
            // ('payments.timeout') that a Dictionary read would silently collapse.
            await connection.ExecuteNonQueryAsync($"""
                INSERT INTO {tableName} VALUES
                    (4001, map('payments.timeout', '200ms', 'payments.timeout', '500ms', 'checkout.v2', 'on')),
                    (4002, map('dark-mode', 'auto'))
                """);
            Console.WriteLine($"Seeded {tableName} with 2 tenants (one has a duplicate-key Map).");

            // --- Default path: Dictionary ----------------------------------------
            // Pre-existing behaviour. The duplicate-key entry from tenant 4001
            // becomes a single ['payments.timeout' => '500ms'] (last-wins).
            Console.WriteLine("\n--- Read as Dictionary<,> (default; lossy for duplicates) ---");
            await foreach (var row in connection.QueryStreamAsync<TenantOverridesAsDictionary>(
                $"SELECT tenant_id, overrides FROM {tableName} ORDER BY tenant_id"))
            {
                Console.WriteLine($"  tenant {row.TenantId}: {row.Overrides.Count} entries");
                foreach (var (k, v) in row.Overrides)
                    Console.WriteLine($"    {k} = {v}");
            }

            // --- Lossless path: ClickHouseMap ------------------------------------
            // Same query, same wire bytes, different POCO. The property type
            // alone selects the entries reader for this call.
            Console.WriteLine("\n--- Read as ClickHouseMap<,> (lossless; preserves duplicates and order) ---");
            await foreach (var row in connection.QueryStreamAsync<TenantOverridesAsClickHouseMap>(
                $"SELECT tenant_id, overrides FROM {tableName} ORDER BY tenant_id"))
            {
                Console.WriteLine(
                    $"  tenant {row.TenantId}: {row.Overrides.Count} entries " +
                    $"(HasDuplicateKeys={row.Overrides.HasDuplicateKeys})");
                for (int i = 0; i < row.Overrides.Count; i++)
                {
                    var entry = row.Overrides[i];
                    Console.WriteLine($"    [{i}] {entry.Key} = {entry.Value}");
                }

                // First-wins lookup mirrors ClickHouse's documented m[k] semantics:
                if (row.Overrides.TryGetValue("payments.timeout", out var firstWins))
                    Console.WriteLine($"    first-wins lookup payments.timeout -> {firstWins}");
            }

            // --- Scalar T: ClickHouseMap directly --------------------------------
            // No POCO needed when you want a single Map value out of an expression.
            // Setting T to ClickHouseMap<,> forces the entries reader for the call.
            Console.WriteLine("\n--- ExecuteScalarAsync<ClickHouseMap<,>> ---");
            var map = await connection.ExecuteScalarAsync<ClickHouseMap<string, string>>(
                $"SELECT overrides FROM {tableName} WHERE tenant_id = 4001");
            Console.WriteLine($"  loaded {map!.Count} entries; duplicates preserved.");

            // Need a Dictionary back from a ClickHouseMap? Explicit collapse:
            var collapsed = map.ToDictionary();
            Console.WriteLine($"  ToDictionary() collapses to {collapsed.Count} entries (last-wins).");

            // Want to group duplicates instead of collapsing? Use ToLookup:
            var grouped = map.ToLookup();
            Console.WriteLine($"  ToLookup()['payments.timeout'] -> [{string.Join(", ", grouped["payments.timeout"])}]");
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
            Console.WriteLine($"\nDropped table {tableName}");
        }
    }
}

file class TenantOverridesAsDictionary
{
    [ClickHouseColumn(Name = "tenant_id")] public ulong TenantId { get; set; }
    [ClickHouseColumn(Name = "overrides")] public Dictionary<string, string> Overrides { get; set; } = new();
}

file class TenantOverridesAsClickHouseMap
{
    [ClickHouseColumn(Name = "tenant_id")] public ulong TenantId { get; set; }
    [ClickHouseColumn(Name = "overrides")] public ClickHouseMap<string, string> Overrides { get; set; } = null!;
}
