using System.Linq.Expressions;
using System.Reflection;
using CH.Native.Connection;

namespace CH.Native.Linq;

/// <summary>
/// Extension methods for creating and querying ClickHouse LINQ queries.
/// </summary>
public static class ClickHouseQueryableExtensions
{
    #region Entry Point - Table<T>()

    /// <summary>
    /// Creates a queryable for the specified entity type.
    /// Table name is resolved using snake_case conversion of the type name.
    /// </summary>
    /// <typeparam name="T">The entity type. Must have a parameterless constructor for result mapping.</typeparam>
    /// <param name="connection">The connection to query against.</param>
    /// <returns>A queryable representing the table.</returns>
    /// <exception cref="InvalidOperationException">Thrown if the connection is not open.</exception>
    /// <example>
    /// var users = await connection.Table&lt;User&gt;()
    ///     .Where(u => u.Age > 18)
    ///     .OrderBy(u => u.Name)
    ///     .ToListAsync();
    /// </example>
    public static ClickHouseQueryable<T> Table<T>(this ClickHouseConnection connection)
    {
        if (!connection.IsOpen)
            throw new InvalidOperationException("Connection must be open before creating a queryable.");

        var tableName = TableNameResolver.Resolve<T>();
        var context = new ClickHouseQueryContext(connection, tableName, typeof(T), columnNames: null);

        return new ClickHouseQueryable<T>(context);
    }

    /// <summary>
    /// Creates a queryable for the specified table name with explicit entity mapping.
    /// </summary>
    /// <typeparam name="T">The entity type. Must have a parameterless constructor for result mapping.</typeparam>
    /// <param name="connection">The connection to query against.</param>
    /// <param name="tableName">The explicit table name to query.</param>
    /// <returns>A queryable representing the table.</returns>
    public static ClickHouseQueryable<T> Table<T>(this ClickHouseConnection connection, string tableName)
    {
        if (!connection.IsOpen)
            throw new InvalidOperationException("Connection must be open before creating a queryable.");

        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        var context = new ClickHouseQueryContext(connection, tableName, typeof(T), columnNames: null);

        return new ClickHouseQueryable<T>(context);
    }

    #endregion

    #region ClickHouse-Specific Extensions

    /// <summary>
    /// Adds the FINAL modifier for ReplacingMergeTree and CollapsingMergeTree tables.
    /// This ensures only the final version of each row is returned.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source queryable.</param>
    /// <returns>A queryable with the FINAL modifier.</returns>
    public static IQueryable<T> Final<T>(this IQueryable<T> source)
    {
        return source.Provider.CreateQuery<T>(
            Expression.Call(
                null,
                GetMethodInfo(Final<T>, source),
                source.Expression));
    }

    /// <summary>
    /// Adds the SAMPLE clause for approximate query processing.
    /// Returns a random sample of approximately the specified ratio of rows.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source queryable.</param>
    /// <param name="ratio">Sample ratio between 0 and 1 (e.g., 0.1 for 10% of rows).</param>
    /// <returns>A queryable with the SAMPLE modifier.</returns>
    public static IQueryable<T> Sample<T>(this IQueryable<T> source, double ratio)
    {
        return source.Provider.CreateQuery<T>(
            Expression.Call(
                null,
                GetMethodInfo(Sample<T>, source, ratio),
                source.Expression,
                Expression.Constant(ratio)));
    }

    /// <summary>
    /// Returns the generated SQL without executing the query.
    /// Useful for debugging and testing.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source queryable.</param>
    /// <returns>The SQL query string.</returns>
    public static string ToSql<T>(this IQueryable<T> source)
    {
        if (source is ClickHouseQueryable<T> chQueryable)
        {
            return chQueryable.ToSql();
        }

        if (source.Provider is ClickHouseQueryProvider provider)
        {
            return provider.TranslateToSql(source.Expression);
        }

        throw new InvalidOperationException(
            "ToSql() is only available on ClickHouse queries created via connection.Table<T>().");
    }

    #endregion

    #region Helpers

    private static MethodInfo GetMethodInfo<T1, T2>(Func<T1, T2> f, T1 unused1)
    {
        return f.Method;
    }

    private static MethodInfo GetMethodInfo<T1, T2, T3>(Func<T1, T2, T3> f, T1 unused1, T2 unused2)
    {
        return f.Method;
    }

    #endregion
}
