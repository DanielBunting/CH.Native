using System.Reflection;
using CH.Native.Results;

namespace CH.Native.Mapping;

/// <summary>
/// Helper for discovering and using source-generated ClickHouse mappers.
/// </summary>
internal static class GeneratedMapperHelper
{
    private static readonly Dictionary<Type, Delegate?> _mapperCache = new();
    private static readonly object _cacheLock = new();

    /// <summary>
    /// Tries to get a generated ReadRow function for type T.
    /// </summary>
    /// <typeparam name="T">The type to get the mapper for.</typeparam>
    /// <param name="readRow">The ReadRow delegate if found.</param>
    /// <returns>True if a generated mapper was found.</returns>
    public static bool TryGetReadRow<T>(out Func<ClickHouseDataReader, T>? readRow)
    {
        var type = typeof(T);

        lock (_cacheLock)
        {
            if (_mapperCache.TryGetValue(type, out var cached))
            {
                readRow = cached as Func<ClickHouseDataReader, T>;
                return readRow is not null;
            }

            // Try to find nested ClickHouseMapper class
            var mapperType = type.GetNestedType("ClickHouseMapper", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
            if (mapperType is null)
            {
                _mapperCache[type] = null;
                readRow = null;
                return false;
            }

            // Try to find ReadRow method
            var readRowMethod = mapperType.GetMethod("ReadRow", BindingFlags.Public | BindingFlags.Static);
            if (readRowMethod is null ||
                readRowMethod.ReturnType != type ||
                readRowMethod.GetParameters().Length != 1 ||
                readRowMethod.GetParameters()[0].ParameterType != typeof(ClickHouseDataReader))
            {
                _mapperCache[type] = null;
                readRow = null;
                return false;
            }

            // Create delegate
            readRow = (Func<ClickHouseDataReader, T>)Delegate.CreateDelegate(
                typeof(Func<ClickHouseDataReader, T>),
                readRowMethod);

            _mapperCache[type] = readRow;
            return true;
        }
    }

    /// <summary>
    /// Tries to get the WriteRow method info for type T.
    /// Note: We return MethodInfo instead of a delegate because Span&lt;T&gt; cannot be used as a generic type argument.
    /// </summary>
    /// <typeparam name="T">The type to get the mapper for.</typeparam>
    /// <param name="writeRowMethod">The WriteRow method info if found.</param>
    /// <returns>True if a generated mapper was found.</returns>
    public static bool TryGetWriteRowMethod<T>(out MethodInfo? writeRowMethod)
    {
        var type = typeof(T);

        // Try to find nested ClickHouseMapper class
        var mapperType = type.GetNestedType("ClickHouseMapper", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
        if (mapperType is null)
        {
            writeRowMethod = null;
            return false;
        }

        // Try to find WriteRow method
        writeRowMethod = mapperType.GetMethod("WriteRow", BindingFlags.Public | BindingFlags.Static);
        return writeRowMethod is not null;
    }

    /// <summary>
    /// Tries to get the column names from a generated mapper.
    /// </summary>
    /// <typeparam name="T">The type to get column names for.</typeparam>
    /// <param name="columnNames">The column names if found.</param>
    /// <returns>True if column names were found.</returns>
    public static bool TryGetColumnNames<T>(out string[]? columnNames)
    {
        var type = typeof(T);

        var mapperType = type.GetNestedType("ClickHouseMapper", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
        if (mapperType is null)
        {
            columnNames = null;
            return false;
        }

        var columnNamesProperty = mapperType.GetProperty("ColumnNames", BindingFlags.Public | BindingFlags.Static);
        if (columnNamesProperty is null)
        {
            columnNames = null;
            return false;
        }

        columnNames = columnNamesProperty.GetValue(null) as string[];
        return columnNames is not null;
    }

    /// <summary>
    /// Tries to get the table name from a generated mapper.
    /// </summary>
    /// <typeparam name="T">The type to get the table name for.</typeparam>
    /// <param name="tableName">The table name if found.</param>
    /// <returns>True if the table name was found.</returns>
    public static bool TryGetTableName<T>(out string? tableName)
    {
        var type = typeof(T);

        var mapperType = type.GetNestedType("ClickHouseMapper", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
        if (mapperType is null)
        {
            tableName = null;
            return false;
        }

        var tableNameProperty = mapperType.GetProperty("TableName", BindingFlags.Public | BindingFlags.Static);
        if (tableNameProperty is null)
        {
            tableName = null;
            return false;
        }

        tableName = tableNameProperty.GetValue(null) as string;
        return tableName is not null;
    }
}
