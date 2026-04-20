using System.Collections.Concurrent;

namespace CH.Native.BulkInsert;

/// <summary>
/// Thread-safe per-connection cache of bulk insert schemas, keyed by
/// (table name, ordered column list fingerprint). The fingerprint differentiates
/// POCOs that map disjoint column subsets of the same table.
/// </summary>
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
