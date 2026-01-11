using System.Linq.Expressions;
using System.Reflection;

namespace CH.Native.Linq;

/// <summary>
/// Provides LINQ query translation and execution for ClickHouse.
/// </summary>
public sealed class ClickHouseQueryProvider : IQueryProvider
{
    private readonly ClickHouseQueryContext _context;

    internal ClickHouseQueryProvider(ClickHouseQueryContext context)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
    }

    /// <summary>
    /// Gets the query context.
    /// </summary>
    internal ClickHouseQueryContext Context => _context;

    /// <summary>
    /// Creates a new queryable when LINQ operators are applied.
    /// </summary>
    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    {
        // Note: We need to create the queryable without the new() constraint for intermediate queries.
        // The constraint is only needed at execution time.
        return (IQueryable<TElement>)CreateQuery(expression);
    }

    /// <summary>
    /// Creates a new queryable (non-generic).
    /// </summary>
    public IQueryable CreateQuery(Expression expression)
    {
        var elementType = GetElementType(expression.Type);
        var queryableType = typeof(ClickHouseQueryable<>).MakeGenericType(elementType);

        // Use reflection to call internal constructor
        var constructor = queryableType.GetConstructor(
            BindingFlags.NonPublic | BindingFlags.Instance,
            null,
            new[] { typeof(ClickHouseQueryProvider), typeof(Expression) },
            null);

        return (IQueryable)constructor!.Invoke(new object[] { this, expression });
    }

    /// <summary>
    /// Synchronous execution is not supported. Use async methods instead.
    /// </summary>
    public TResult Execute<TResult>(Expression expression)
    {
        throw new NotSupportedException(
            "Synchronous query execution is not supported. " +
            "Use async methods (ToListAsync, FirstAsync, CountAsync, etc.) instead.");
    }

    /// <summary>
    /// Synchronous execution is not supported. Use async methods instead.
    /// </summary>
    public object? Execute(Expression expression)
    {
        throw new NotSupportedException(
            "Synchronous query execution is not supported. " +
            "Use async methods (ToListAsync, FirstAsync, CountAsync, etc.) instead.");
    }

    /// <summary>
    /// Translates the expression tree to SQL.
    /// </summary>
    internal string TranslateToSql(Expression expression)
    {
        var visitor = new ClickHouseExpressionVisitor(_context);
        return visitor.Translate(expression);
    }

    /// <summary>
    /// Creates a new context for a different element type (used during projection).
    /// </summary>
    internal ClickHouseQueryContext CreateContextForType(Type elementType)
    {
        return new ClickHouseQueryContext(
            _context.Connection,
            _context.TableName,
            elementType,
            _context.ColumnNames);
    }

    private static Type GetElementType(Type type)
    {
        // Check for IQueryable<T>
        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IQueryable<>))
            return type.GetGenericArguments()[0];

        // Check interfaces
        foreach (var iface in type.GetInterfaces())
        {
            if (iface.IsGenericType && iface.GetGenericTypeDefinition() == typeof(IQueryable<>))
                return iface.GetGenericArguments()[0];
        }

        // Check for IEnumerable<T>
        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
            return type.GetGenericArguments()[0];

        throw new ArgumentException($"Cannot determine element type from '{type}'");
    }
}
