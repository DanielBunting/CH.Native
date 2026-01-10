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
    /// Gets the default options instance.
    /// </summary>
    public static BulkInsertOptions Default { get; } = new();
}
