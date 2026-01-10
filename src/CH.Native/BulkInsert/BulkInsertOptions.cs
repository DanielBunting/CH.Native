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
    /// Gets the default options instance.
    /// </summary>
    public static BulkInsertOptions Default { get; } = new();
}
