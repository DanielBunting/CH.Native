namespace CH.Native.Data;

/// <summary>
/// Represents progress information for an executing query.
/// </summary>
/// <param name="RowsRead">Number of rows read so far.</param>
/// <param name="BytesRead">Number of bytes read so far.</param>
/// <param name="TotalRowsToRead">Total number of rows to read (estimate).</param>
/// <param name="RowsWritten">Number of rows written (for INSERT operations).</param>
/// <param name="BytesWritten">Number of bytes written (for INSERT operations).</param>
public readonly record struct QueryProgress(
    ulong RowsRead,
    ulong BytesRead,
    ulong TotalRowsToRead,
    ulong RowsWritten,
    ulong BytesWritten)
{
    /// <summary>
    /// Gets the progress as a percentage (0-100) if TotalRowsToRead is known.
    /// Returns null if TotalRowsToRead is 0.
    /// </summary>
    public double? PercentComplete => TotalRowsToRead > 0
        ? (double)RowsRead / TotalRowsToRead * 100.0
        : null;
}
