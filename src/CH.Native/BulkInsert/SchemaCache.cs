using System.Collections.Concurrent;

namespace CH.Native.BulkInsert;

/// <summary>
/// Thread-safe per-connection cache of bulk insert schemas, keyed by
/// (table name, ordered column list fingerprint). The fingerprint differentiates
/// POCOs that map disjoint column subsets of the same table.
/// </summary>
/// <remarks>
/// Lifetime is bounded by the owning <c>ClickHouseConnection</c>: the cache is
/// cleared on <c>CloseInternalAsync</c> and per-table on <c>InvalidateTable</c>.
/// Entries are not evicted by size, so callers that bulk-insert into very many
/// distinct tables on a long-lived pooled connection can accumulate metadata
/// (a few hundred bytes per <see cref="BulkInsertSchema"/>). If that profile
/// matches your workload, call
/// <c>ClickHouseConnection.InvalidateSchemaCache()</c> periodically — or
/// configure <c>BulkInsertOptions.UseSchemaCache = false</c> — to keep the
/// per-connection footprint flat.
/// </remarks>
internal sealed class SchemaCache
{
    private readonly ConcurrentDictionary<SchemaKey, BulkInsertSchema> _entries = new();

    public bool TryGet(SchemaKey key, out BulkInsertSchema schema)
        => _entries.TryGetValue(key, out schema!);

    public void Set(SchemaKey key, BulkInsertSchema schema)
        => _entries[key] = schema;

    /// <summary>
    /// Evicts all entries for the given table (across all column fingerprints).
    /// </summary>
    public void InvalidateTable(string tableName)
    {
        foreach (var key in _entries.Keys)
        {
            if (string.Equals(key.TableName, tableName, StringComparison.Ordinal))
            {
                _entries.TryRemove(key, out _);
            }
        }
    }

    public void Clear() => _entries.Clear();

    public int Count => _entries.Count;
}

internal readonly record struct SchemaKey(string TableName, string ColumnListFingerprint);

internal sealed record BulkInsertSchema(string[] ColumnNames, string[] ColumnTypes);
