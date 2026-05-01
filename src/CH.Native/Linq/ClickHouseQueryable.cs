using System.Collections;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using CH.Native.Connection;
using CH.Native.Numerics;
using CH.Native.Results;

namespace CH.Native.Linq;

/// <summary>
/// Represents a ClickHouse LINQ query that can be executed asynchronously.
/// Implements IQueryable&lt;T&gt;, IOrderedQueryable&lt;T&gt;, and IAsyncEnumerable&lt;T&gt;.
/// </summary>
/// <typeparam name="T">The element type of the query results.</typeparam>
public sealed class ClickHouseQueryable<T> : IQueryable<T>, IOrderedQueryable<T>, IAsyncEnumerable<T>
{
    // Closed generics fix typeof(T) at JIT time, so this static decision is correct
    // per concrete element type. Scalar projections (string, primitives, decimal, DateTime,
    // Guid, ClickHouseDecimal, and Nullable<T> wrappers) bypass the typed row mapper —
    // the mapper's `where T : new()` constraint excludes string and silently returns
    // default(T) for primitives because they have no settable properties.
    private static readonly bool _isScalarProjection = IsScalarProjectionType(typeof(T));

    private readonly ClickHouseQueryProvider _provider;
    private readonly Expression _expression;

    /// <summary>
    /// Creates a root queryable for a table.
    /// </summary>
    internal ClickHouseQueryable(ClickHouseQueryContext context)
    {
        _provider = new ClickHouseQueryProvider(context);
        _expression = Expression.Constant(this);
    }

    /// <summary>
    /// Creates a queryable from an existing provider and expression.
    /// </summary>
    internal ClickHouseQueryable(ClickHouseQueryProvider provider, Expression expression)
    {
        _provider = provider ?? throw new ArgumentNullException(nameof(provider));
        _expression = expression ?? throw new ArgumentNullException(nameof(expression));
    }

    /// <summary>
    /// Gets the type of the elements returned by the query.
    /// </summary>
    public Type ElementType => typeof(T);

    /// <summary>
    /// Gets the expression tree representing the query.
    /// </summary>
    public Expression Expression => _expression;

    /// <summary>
    /// Gets the query provider.
    /// </summary>
    public IQueryProvider Provider => _provider;

    /// <summary>
    /// Synchronous enumeration is not supported.
    /// Use async enumeration (await foreach) or async methods (ToListAsync, etc.) instead.
    /// </summary>
    /// <exception cref="NotSupportedException">Always thrown.</exception>
    public IEnumerator<T> GetEnumerator()
    {
        throw new NotSupportedException(
            "Synchronous enumeration is not supported. " +
            "Use 'await foreach' or async methods (ToListAsync, FirstAsync, etc.) instead.");
    }

    /// <summary>
    /// Synchronous enumeration is not supported.
    /// </summary>
    /// <exception cref="NotSupportedException">Always thrown.</exception>
    IEnumerator IEnumerable.GetEnumerator()
    {
        throw new NotSupportedException(
            "Synchronous enumeration is not supported. " +
            "Use 'await foreach' or async methods (ToListAsync, FirstAsync, etc.) instead.");
    }

    /// <summary>
    /// Gets an async enumerator for the query results.
    /// Translates the expression tree to SQL and executes against ClickHouse.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An async enumerator for the query results.</returns>
    public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        var (sql, parameters) = _provider.TranslateToSqlWithParameters(_expression);
        var connection = _provider.Context.Connection;
        var queryId = _provider.Context.QueryId;

        await using var reader = await connection
            .ExecuteReaderWithParametersAsync(sql, parameters, cancellationToken, queryId: queryId)
            .ConfigureAwait(false);

        if (_isScalarProjection)
        {
            // Scalar projection: read column 0 directly via the data reader's typed accessor,
            // which handles null → default(T), exact-type fast path, and Convert.ChangeType
            // for numeric widening. No reflection, no `where T : new()` constraint.
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                yield return reader.GetFieldValue<T>(0);
            }
            yield break;
        }

        // Entity projection: reflect into the helper to satisfy its `where T : new()`
        // constraint (the mapper needs a parameterless ctor to materialise rows).
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            yield break;

        var mapMethod = typeof(ClickHouseQueryableHelper)
            .GetMethod(nameof(ClickHouseQueryableHelper.MapAll))!
            .MakeGenericMethod(typeof(T));

        var enumerable = (IAsyncEnumerable<T>)mapMethod.Invoke(null, new object?[] { reader, cancellationToken })!;
        await foreach (var item in enumerable.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            yield return item;
        }
    }

    private static bool IsScalarProjectionType(Type type)
    {
        var underlying = Nullable.GetUnderlyingType(type) ?? type;

        return underlying == typeof(bool)
            || underlying == typeof(byte)
            || underlying == typeof(sbyte)
            || underlying == typeof(short)
            || underlying == typeof(ushort)
            || underlying == typeof(int)
            || underlying == typeof(uint)
            || underlying == typeof(long)
            || underlying == typeof(ulong)
            || underlying == typeof(float)
            || underlying == typeof(double)
            || underlying == typeof(decimal)
            || underlying == typeof(DateTime)
            || underlying == typeof(DateTimeOffset)
            || underlying == typeof(DateOnly)
            || underlying == typeof(TimeOnly)
            || underlying == typeof(Guid)
            || underlying == typeof(string)
            || underlying == typeof(byte[])
            || underlying == typeof(ClickHouseDecimal);
    }

    /// <summary>
    /// Returns the SQL that would be generated for this query, with captured
    /// constants and closure values inlined as SQL literals so the result is
    /// directly runnable in <c>clickhouse-client</c> or any raw-query path.
    /// Useful for debugging and testing.
    /// </summary>
    /// <remarks>
    /// The execution path uses parameter-bound SQL (<c>{p1:Int32}</c> placeholders)
    /// for SQL-injection safety and server-side query-cache reuse; this method
    /// is a diagnostic helper, not the executed form. The inline literals are
    /// still escaped, but callers should not feed the output of <c>ToSql()</c>
    /// into a privileged execution path.
    /// </remarks>
    /// <returns>The SQL query string with literals inlined.</returns>
    public string ToSql()
    {
        var (sql, parameters) = _provider.TranslateToSqlWithParameters(_expression);
        return SqlLiteralFormatter.RenderInline(sql, parameters);
    }
}

/// <summary>
/// Reflection helper to satisfy the typed mapper's `where T : new()` constraint.
/// The data reader has already been advanced to the first row by the caller.
/// </summary>
internal static class ClickHouseQueryableHelper
{
    public static async IAsyncEnumerable<T> MapAll<T>(
        ClickHouseDataReader reader,
        [EnumeratorCancellation] CancellationToken cancellationToken)
        where T : new()
    {
        var mapper = new CH.Native.Results.TypeMapper<T>(reader);

        // Caller has already advanced to the first row; map it before pulling the rest.
        yield return mapper.Map(reader);

        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            yield return mapper.Map(reader);
        }
    }
}
