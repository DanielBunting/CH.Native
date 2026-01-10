namespace CH.Native.Data;

/// <summary>
/// A block with typed column storage - no boxing for value types.
/// This is the fast-path alternative to <see cref="Block"/> for typed queries.
/// </summary>
public sealed class TypedBlock : IDisposable
{
    /// <summary>
    /// The table name this block's data comes from.
    /// </summary>
    public required string TableName { get; init; }

    /// <summary>
    /// The column names in order.
    /// </summary>
    public required string[] ColumnNames { get; init; }

    /// <summary>
    /// The ClickHouse type names for each column.
    /// </summary>
    public required string[] ColumnTypes { get; init; }

    /// <summary>
    /// The typed columns containing the data.
    /// </summary>
    public required ITypedColumn[] Columns { get; init; }

    /// <summary>
    /// The number of columns in this block.
    /// </summary>
    public int ColumnCount => Columns.Length;

    /// <summary>
    /// The number of rows in this block.
    /// </summary>
    public int RowCount => Columns.Length > 0 ? Columns[0].Count : 0;

    /// <summary>
    /// Gets a value at the specified row and column (boxes value types).
    /// Use typed access via GetColumn&lt;T&gt; when possible.
    /// </summary>
    /// <param name="row">The row index.</param>
    /// <param name="column">The column index.</param>
    /// <returns>The value at the specified position.</returns>
    public object? GetValue(int row, int column) => Columns[column].GetValue(row);

    /// <summary>
    /// Gets a column by index.
    /// </summary>
    public ITypedColumn this[int index] => Columns[index];

    /// <summary>
    /// Gets a typed column by index.
    /// </summary>
    /// <typeparam name="T">The expected element type.</typeparam>
    /// <param name="index">The column index.</param>
    /// <returns>The typed column.</returns>
    /// <exception cref="InvalidCastException">If the column type doesn't match.</exception>
    public TypedColumn<T> GetColumn<T>(int index)
    {
        return (TypedColumn<T>)Columns[index];
    }

    /// <summary>
    /// Gets a column index by name.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <returns>The column index, or -1 if not found.</returns>
    public int GetColumnIndex(string name)
    {
        for (int i = 0; i < ColumnNames.Length; i++)
        {
            if (ColumnNames[i].Equals(name, StringComparison.OrdinalIgnoreCase))
                return i;
        }
        return -1;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        foreach (var column in Columns)
        {
            column.Dispose();
        }
    }
}
