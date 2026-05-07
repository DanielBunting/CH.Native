namespace CH.Native.BulkInsert;

/// <summary>
/// Options for configuring bulk insert operations.
/// </summary>
public sealed class BulkInsertOptions
{
    /// <summary>
    /// Gets or sets the number of rows to buffer before sending to the server.
    /// Default is 10,000 rows.
    /// </summary>
    public int BatchSize { get; set; } = 10_000;

    /// <summary>
    /// Gets or sets whether to include columns that have null values.
    /// When false, columns with all null values in a batch will still be sent.
    /// Default is true.
    /// </summary>
    public bool IncludeNullColumns { get; set; } = true;

    /// <summary>
    /// Gets or sets whether to use pooled arrays for column data extraction in the fallback path.
    /// When true (default), arrays are reused across batch flushes to reduce GC pressure.
    /// </summary>
    public bool UsePooledArrays { get; set; } = true;

    /// <summary>
    /// Gets or sets whether to prefer direct-to-wire streaming when inserting from an IEnumerable.
    /// When true (default), rows are accumulated in a pooled array and sent directly to the server
    /// without copying to an intermediate List buffer. This reduces Gen1 GC collections for
    /// large streaming inserts.
    /// </summary>
    public bool PreferDirectStreaming { get; set; } = true;

    /// <summary>
    /// Gets or sets whether to reuse a per-connection cache of table schemas across inserter
    /// initializations. When enabled, repeat inserters against the same table+column list skip
    /// the server round-trip that fetches column names and types.
    /// </summary>
    /// <remarks>
    /// When <c>null</c> (the default), the setting is inherited from
    /// <see cref="Connection.ClickHouseConnectionSettings.UseSchemaCache"/>. Set to <c>true</c>
    /// or <c>false</c> to override the connection default per call.
    /// A stale cache after a server-side ALTER TABLE can produce an error at CompleteAsync time;
    /// call <see cref="Connection.ClickHouseConnection.InvalidateSchemaCache"/> after ALTERs, or
    /// catch the <see cref="Exceptions.ClickHouseServerException"/> and retry with a fresh
    /// inserter (the cache entry is evicted automatically on schema-drift errors).
    /// </remarks>
    public bool? UseSchemaCache { get; set; } = null;

    /// <summary>
    /// Gets or sets the ClickHouse roles to activate for this bulk insert. Overrides
    /// the connection-level <see cref="Connection.ClickHouseConnectionSettings.Roles"/>.
    /// Empty list strips all roles (<c>SET ROLE NONE</c>); <c>null</c> inherits the
    /// connection default.
    /// </summary>
    public IList<string>? Roles { get; set; }

    /// <summary>
    /// Gets or sets the query ID to send with the INSERT query. When non-null and non-empty,
    /// the supplied value is used; otherwise the driver generates a GUID. Max length 128
    /// characters. The resolved ID is observable via
    /// <see cref="Connection.ClickHouseConnection.CurrentQueryId"/> after <c>InitAsync</c>.
    /// </summary>
    public string? QueryId { get; set; }

    /// <summary>
    /// Pre-supplied column types, keyed by column name (OrdinalIgnoreCase).
    /// When set and covering every column being inserted, the dynamic
    /// (POCO-less) bulk-insert path skips the server schema-probe round-trip
    /// and builds the schema directly from this dictionary.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Currently honored by the <see cref="DynamicBulkInserter"/> (non-generic)
    /// path only; the POCO <see cref="BulkInserter{T}"/> path always probes
    /// the server. Adding POCO support is a planned follow-up that touches
    /// the property-mapping flow.
    /// </para>
    /// <para>
    /// Partial coverage (some but not all column names present) throws an
    /// <see cref="InvalidOperationException"/> at <c>InitAsync</c> time:
    /// partial types is treated as a programming error, not a fallback
    /// condition. Mismatched types vs. the server's actual schema surface as
    /// a <see cref="Exceptions.ClickHouseServerException"/> at
    /// <c>CompleteAsync</c> time.
    /// </para>
    /// </remarks>
    public IReadOnlyDictionary<string, string>? ColumnTypes { get; set; }

    /// <summary>
    /// Gets the default options instance.
    /// </summary>
    public static BulkInsertOptions Default { get; } = new();
}
