using CH.Native.Connection;

namespace CH.Native.Linq;

/// <summary>
/// Holds context information for executing ClickHouse LINQ queries.
/// </summary>
internal sealed class ClickHouseQueryContext
{
    /// <summary>
    /// The connection to execute queries against.
    /// </summary>
    public ClickHouseConnection Connection { get; }

    /// <summary>
    /// The resolved table name for the root entity.
    /// </summary>
    public string TableName { get; }

    /// <summary>
    /// Column names from the generated mapper (if available).
    /// Used for property-to-column name resolution.
    /// </summary>
    public string[]? ColumnNames { get; }

    /// <summary>
    /// The element type of the queryable.
    /// </summary>
    public Type ElementType { get; }

    public ClickHouseQueryContext(
        ClickHouseConnection? connection,
        string tableName,
        Type elementType,
        string[]? columnNames = null)
    {
        // Connection can be null for SQL generation tests - execution will fail but ToSql() works
        Connection = connection!;
        TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        ElementType = elementType ?? throw new ArgumentNullException(nameof(elementType));
        ColumnNames = columnNames;
    }
}
