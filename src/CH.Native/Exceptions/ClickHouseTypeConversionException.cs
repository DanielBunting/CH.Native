namespace CH.Native.Exceptions;

/// <summary>
/// Thrown when a value cannot be converted to the requested CLR type.
/// Carries the offending row index and dimensional metadata so callers
/// can localize the failure within a result set.
/// </summary>
public sealed class ClickHouseTypeConversionException : ClickHouseException
{
    /// <summary>
    /// Index of the row that triggered the failure, or <c>-1</c> when the
    /// failure is not row-specific.
    /// </summary>
    public int RowIndex { get; }

    /// <summary>
    /// The length the converter required (e.g., the uniform inner length
    /// inferred from row 0), or <c>-1</c> when not applicable.
    /// </summary>
    public int ExpectedLength { get; }

    /// <summary>
    /// The actual length observed at <see cref="RowIndex"/>, or <c>-1</c>
    /// when not applicable.
    /// </summary>
    public int ActualLength { get; }

    /// <summary>
    /// Initializes a new instance with a descriptive message and shape metadata.
    /// </summary>
    public ClickHouseTypeConversionException(string message, int rowIndex = -1, int expectedLength = -1, int actualLength = -1)
        : base(message)
    {
        RowIndex = rowIndex;
        ExpectedLength = expectedLength;
        ActualLength = actualLength;
    }
}
