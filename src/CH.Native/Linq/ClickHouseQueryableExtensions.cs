using System.Linq.Expressions;
using System.Reflection;
using CH.Native.BulkInsert;
using CH.Native.Connection;

namespace CH.Native.Linq;

/// <summary>
/// Extension methods for creating and querying ClickHouse LINQ queries.
/// </summary>
public static class ClickHouseQueryableExtensions
{
    #region Entry Point - Table<T>()

    /// <summary>
    /// Creates a queryable for the specified entity type that also supports
    /// <c>InsertAsync</c> via the extension methods on this class. Table name
    /// is resolved using snake_case conversion of the type name.
    /// </summary>
    /// <typeparam name="T">The entity type. Must have a parameterless constructor for result mapping.</typeparam>
    /// <param name="connection">The connection to query against.</param>
    /// <returns>A queryable representing the table.</returns>
    /// <exception cref="InvalidOperationException">Thrown if the connection is not open.</exception>
    /// <example>
    /// var users = connection.Table&lt;User&gt;();
    /// await users.InsertAsync(new User { ... });
    /// var adults = await users.Where(u =&gt; u.Age &gt; 18).ToListAsync();
    /// </example>
    public static IQueryable<T> Table<T>(this ClickHouseConnection connection)
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
    public static IQueryable<T> Table<T>(this ClickHouseConnection connection, string tableName)
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
    /// Sets the query ID sent on the wire for this query. When non-null and non-empty, the
    /// supplied value is used instead of the auto-generated GUID. Max length is 128 characters.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source queryable. Must be a ClickHouse query.</param>
    /// <param name="queryId">The query ID to send on the wire.</param>
    /// <returns>The same queryable, with the query ID configured on its context.</returns>
    /// <exception cref="InvalidOperationException">Thrown if the source is not a ClickHouse query.</exception>
    public static IQueryable<T> WithQueryId<T>(this IQueryable<T> source, string queryId)
    {
        if (source.Provider is not ClickHouseQueryProvider provider)
            throw new InvalidOperationException(
                "WithQueryId() is only available on ClickHouse queries created via connection.Table<T>().");

        provider.Context.QueryId = queryId;
        return source;
    }

    /// <summary>
    /// Returns the query as an <see cref="IAsyncEnumerable{T}"/> for use with <c>await foreach</c>.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source queryable. Must be a ClickHouse query.</param>
    /// <returns>An async enumerable that executes the query and streams results.</returns>
    /// <exception cref="InvalidOperationException">Thrown if the source is not a ClickHouse query.</exception>
    public static IAsyncEnumerable<T> AsAsyncEnumerable<T>(this IQueryable<T> source)
    {
        if (source is IAsyncEnumerable<T> asyncEnumerable)
        {
            return asyncEnumerable;
        }

        throw new InvalidOperationException(
            "AsAsyncEnumerable() is only available on ClickHouse queries created via connection.Table<T>().");
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

    #region Insert

#pragma warning disable RS0026, RS0027 // Sibling InsertAsync overloads — distinct row-shape (T / IEnumerable<T> / IAsyncEnumerable<T>) keeps overload resolution unambiguous.
    /// <summary>
    /// Inserts a single record into the table represented by this queryable.
    /// </summary>
    /// <remarks>
    /// Each call opens a fresh INSERT context on the wire (query handshake,
    /// per-block commit, end-of-stream). That overhead is fine for occasional
    /// inserts but adds up in a hot loop — for high-volume writes prefer the
    /// <see cref="IEnumerable{T}"/> or <see cref="IAsyncEnumerable{T}"/>
    /// overload (one INSERT, many rows), or a long-lived
    /// <see cref="BulkInserter{T}"/> for explicit batching.
    /// </remarks>
    public static Task InsertAsync<T>(
        this IQueryable<T> source,
        T row,
        BulkInsertOptions? options = null,
        CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(row);
        return InsertAsync(source, new[] { row }, options, cancellationToken);
    }

    /// <summary>
    /// Inserts a sequence of records into the table represented by this queryable
    /// in a single bulk-insert pass.
    /// </summary>
    public static async Task InsertAsync<T>(
        this IQueryable<T> source,
        IEnumerable<T> rows,
        BulkInsertOptions? options = null,
        CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(rows);
        var (connection, tableName, ownsConnection) = await ResolveInsertTargetAsync(source, cancellationToken).ConfigureAwait(false);
        try
        {
            await connection.BulkInsertAsync(tableName, rows, options, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (ownsConnection)
                await connection.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Inserts an async stream of records into the table represented by this queryable
    /// in a single bulk-insert pass.
    /// </summary>
    public static async Task InsertAsync<T>(
        this IQueryable<T> source,
        IAsyncEnumerable<T> rows,
        BulkInsertOptions? options = null,
        CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(rows);
        var (connection, tableName, ownsConnection) = await ResolveInsertTargetAsync(source, cancellationToken).ConfigureAwait(false);
        try
        {
            await connection.BulkInsertAsync(tableName, rows, options, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (ownsConnection)
                await connection.DisposeAsync().ConfigureAwait(false);
        }
    }
#pragma warning restore RS0026, RS0027

    /// <summary>
    /// Resolves the connection and table name behind a queryable for an insert
    /// operation. For data-source-bound queryables, rents a connection from the
    /// pool — the caller must dispose it (returns to pool) when <c>ownsConnection</c>
    /// is <see langword="true"/>.
    /// </summary>
    private static async Task<(ClickHouseConnection Connection, string TableName, bool OwnsConnection)>
        ResolveInsertTargetAsync<T>(IQueryable<T> source, CancellationToken cancellationToken)
    {
        if (source.Provider is not ClickHouseQueryProvider provider)
            throw new InvalidOperationException(
                "InsertAsync() is only available on ClickHouse queries created via connection.Table<T>() or dataSource.Table<T>().");

        var ctx = provider.Context;

        if (ctx.Connection is not null)
            return (ctx.Connection, ctx.TableName, OwnsConnection: false);

        if (ctx.DataSource is not null)
        {
            var rented = await ctx.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            return (rented, ctx.TableName, OwnsConnection: true);
        }

        throw new InvalidOperationException(
            "InsertAsync() requires the underlying query to be bound to a Connection or DataSource. " +
            "This queryable was created without either, which only supports SQL generation, not execution.");
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
