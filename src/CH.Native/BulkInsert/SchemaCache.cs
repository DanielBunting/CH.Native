using System.Collections.Concurrent;

namespace CH.Native.BulkInsert;

/// <summary>
/// Thread-safe per-connection cache of bulk insert schemas, keyed by
/// (database, table, ordered column list fingerprint). The fingerprint differentiates
/// POCOs that map disjoint column subsets of the same table; the database segment
/// keeps cross-database inserts on the same connection from colliding.
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
    /// Evicts all entries for the given (database, table) pair (across all column fingerprints).
    /// </summary>
    /// <remarks>
    /// **Case-sensitive.** ClickHouse table identifiers are byte-equal compared,
    /// so callers must invalidate using the exact casing the entry was inserted
    /// with. <c>InvalidateTable("db", "MyTable")</c> does <b>not</b> evict an entry
    /// stored under <c>("db", "mytable")</c>; on a case-insensitive filesystem or
    /// catalog wrapper this can leave stale schema metadata in the cache. If
    /// your code paths reference the same table with mixed casing, normalise
    /// table names at a single boundary before calling either <see cref="Set"/>
    /// or <see cref="InvalidateTable"/>.
    /// </remarks>
    public void InvalidateTable(string database, string table)
    {
        foreach (var key in _entries.Keys)
        {
            if (string.Equals(key.Database, database, StringComparison.Ordinal) &&
                string.Equals(key.Table, table, StringComparison.Ordinal))
            {
                _entries.TryRemove(key, out _);
            }
        }
    }

    public void Clear() => _entries.Clear();

    public int Count => _entries.Count;
}

internal readonly record struct SchemaKey(string Database, string Table, string ColumnListFingerprint);

internal sealed record BulkInsertSchema(string[] ColumnNames, string[] ColumnTypes);
