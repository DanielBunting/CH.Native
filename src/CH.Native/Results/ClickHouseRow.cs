namespace CH.Native.Results;

/// <summary>
/// Represents a lightweight view over the current row in a ClickHouseDataReader.
/// </summary>
/// <remarks>
/// This struct is only valid while the reader is positioned on the current row.
/// Do not store instances of this struct for later use.
/// </remarks>
public readonly struct ClickHouseRow
{
    private readonly ClickHouseDataReader _reader;

    internal ClickHouseRow(ClickHouseDataReader reader)
    {
        _reader = reader;
    }

    /// <summary>
    /// Gets the value at the specified column ordinal.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The value at the specified column.</returns>
    public object? this[int ordinal] => _reader.GetValue(ordinal);

    /// <summary>
    /// Gets the value at the specified column name.
    /// </summary>
    /// <param name="name">The column name (case-insensitive).</param>
    /// <returns>The value at the specified column.</returns>
    public object? this[string name] => _reader.GetValue(_reader.GetOrdinal(name));

    /// <summary>
    /// Gets the number of columns in the row.
    /// </summary>
    public int FieldCount => _reader.FieldCount;

    /// <summary>
    /// Gets a typed value at the specified column ordinal.
    /// </summary>
    /// <typeparam name="T">The expected type.</typeparam>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The typed value at the specified column.</returns>
    public T GetFieldValue<T>(int ordinal) => _reader.GetFieldValue<T>(ordinal);

    /// <summary>
    /// Gets a typed value at the specified column name.
    /// </summary>
    /// <typeparam name="T">The expected type.</typeparam>
    /// <param name="name">The column name (case-insensitive).</param>
    /// <returns>The typed value at the specified column.</returns>
    public T GetFieldValue<T>(string name) => _reader.GetFieldValue<T>(name);

    /// <summary>
    /// Checks if the value at the specified ordinal is null.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>True if the value is null; otherwise false.</returns>
    public bool IsDBNull(int ordinal) => _reader.IsDBNull(ordinal);

    /// <summary>
    /// Checks if the value at the specified column name is null.
    /// </summary>
    /// <param name="name">The column name (case-insensitive).</param>
    /// <returns>True if the value is null; otherwise false.</returns>
    public bool IsDBNull(string name) => _reader.IsDBNull(_reader.GetOrdinal(name));
}
