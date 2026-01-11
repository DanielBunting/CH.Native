namespace CH.Native.Mapping;

/// <summary>
/// Metadata for a mapped ClickHouse column.
/// </summary>
/// <param name="PropertyName">The CLR property name.</param>
/// <param name="ColumnName">The ClickHouse column name.</param>
/// <param name="ClickHouseType">The ClickHouse type name.</param>
/// <param name="ClrType">The CLR type of the property.</param>
/// <param name="IsNullable">Whether the column is nullable.</param>
public readonly record struct ClickHouseColumnInfo(
    string PropertyName,
    string ColumnName,
    string ClickHouseType,
    Type ClrType,
    bool IsNullable);
