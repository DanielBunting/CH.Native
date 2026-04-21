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
    /// Gets the default options instance.
    /// </summary>
    public static BulkInsertOptions Default { get; } = new();
}
