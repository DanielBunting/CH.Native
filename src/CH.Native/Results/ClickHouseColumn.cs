namespace CH.Native.Results;

/// <summary>
/// Represents metadata about a column in a ClickHouse result set.
/// </summary>
public readonly struct ClickHouseColumn
{
    /// <summary>
    /// Gets the zero-based ordinal position of the column.
    /// </summary>
    public int Ordinal { get; }

    /// <summary>
    /// Gets the name of the column.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the ClickHouse type name (e.g., "Int32", "Nullable(String)").
    /// </summary>
    public string ClickHouseTypeName { get; }

    /// <summary>
    /// Gets the CLR type that values in this column are mapped to.
    /// </summary>
    public Type ClrType { get; }

    /// <summary>
    /// Creates a new column metadata instance.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <param name="name">The column name.</param>
    /// <param name="clickHouseTypeName">The ClickHouse type name.</param>
    /// <param name="clrType">The CLR type for column values.</param>
    public ClickHouseColumn(int ordinal, string name, string clickHouseTypeName, Type clrType)
    {
        Ordinal = ordinal;
        Name = name;
        ClickHouseTypeName = clickHouseTypeName;
        ClrType = clrType;
    }
}
