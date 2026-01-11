using System.Linq.Expressions;

namespace CH.Native.Linq;

/// <summary>
/// Async extension methods for ClickHouseQueryable.
/// Provides ToListAsync, FirstAsync, CountAsync, and other async terminal operators.
/// </summary>
public static class AsyncQueryableExtensions
{
    #region ToList / ToArray

    /// <summary>
    /// Asynchronously creates a List from the queryable.
    /// </summary>
    public static async Task<List<T>> ToListAsync<T>(
        this IQueryable<T> source,
        CancellationToken cancellationToken = default)
    {
        if (source is not IAsyncEnumerable<T> asyncEnumerable)
            throw new InvalidOperationException("Source does not support async enumeration.");

        var list = new List<T>();
        await foreach (var item in asyncEnumerable.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            list.Add(item);
        }
        return list;
    }

    /// <summary>
    /// Asynchronously creates an array from the queryable.
    /// </summary>
    public static async Task<T[]> ToArrayAsync<T>(
        this IQueryable<T> source,
        CancellationToken cancellationToken = default)
    {
        var list = await source.ToListAsync(cancellationToken).ConfigureAwait(false);
        return list.ToArray();
    }

    #endregion

    #region First / FirstOrDefault

    /// <summary>
    /// Asynchronously returns the first element of the sequence.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when the sequence is empty.</exception>
    public static async Task<T> FirstAsync<T>(
        this IQueryable<T> source,
        CancellationToken cancellationToken = default)
    {
        // Apply Take(1) for efficiency
        var query = source.Take(1);

        if (query is not IAsyncEnumerable<T> asyncEnumerable)
            throw new InvalidOperationException("Source does not support async enumeration.");

        await foreach (var item in asyncEnumerable.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            return item;
        }

        throw new InvalidOperationException("Sequence contains no elements.");
    }

    /// <summary>
    /// Asynchronously returns the first element matching the predicate.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when no matching element is found.</exception>
    public static async Task<T> FirstAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, bool>> predicate,
        CancellationToken cancellationToken = default)
    {
        return await source.Where(predicate).FirstAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously returns the first element, or default if empty.
    /// </summary>
    public static async Task<T?> FirstOrDefaultAsync<T>(
        this IQueryable<T> source,
        CancellationToken cancellationToken = default)
    {
        var query = source.Take(1);

        if (query is not IAsyncEnumerable<T> asyncEnumerable)
            throw new InvalidOperationException("Source does not support async enumeration.");

        await foreach (var item in asyncEnumerable.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            return item;
        }

        return default;
    }

    /// <summary>
    /// Asynchronously returns the first element matching the predicate, or default if not found.
    /// </summary>
    public static async Task<T?> FirstOrDefaultAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, bool>> predicate,
        CancellationToken cancellationToken = default)
    {
        return await source.Where(predicate).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
    }

    #endregion

    #region Single / SingleOrDefault

    /// <summary>
    /// Asynchronously returns the only element of the sequence.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when the sequence has zero or more than one element.</exception>
    public static async Task<T> SingleAsync<T>(
        this IQueryable<T> source,
        CancellationToken cancellationToken = default)
    {
        // Take 2 to detect multiple elements
        var query = source.Take(2);

        if (query is not IAsyncEnumerable<T> asyncEnumerable)
            throw new InvalidOperationException("Source does not support async enumeration.");

        T? result = default;
        bool hasValue = false;

        await foreach (var item in asyncEnumerable.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (hasValue)
                throw new InvalidOperationException("Sequence contains more than one element.");

            result = item;
            hasValue = true;
        }

        if (!hasValue)
            throw new InvalidOperationException("Sequence contains no elements.");

        return result!;
    }

    /// <summary>
    /// Asynchronously returns the only element matching the predicate.
    /// </summary>
    public static async Task<T> SingleAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, bool>> predicate,
        CancellationToken cancellationToken = default)
    {
        return await source.Where(predicate).SingleAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously returns the only element, or default if empty.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when the sequence has more than one element.</exception>
    public static async Task<T?> SingleOrDefaultAsync<T>(
        this IQueryable<T> source,
        CancellationToken cancellationToken = default)
    {
        var query = source.Take(2);

        if (query is not IAsyncEnumerable<T> asyncEnumerable)
            throw new InvalidOperationException("Source does not support async enumeration.");

        T? result = default;
        bool hasValue = false;

        await foreach (var item in asyncEnumerable.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (hasValue)
                throw new InvalidOperationException("Sequence contains more than one element.");

            result = item;
            hasValue = true;
        }

        return result;
    }

    /// <summary>
    /// Asynchronously returns the only element matching the predicate, or default if not found.
    /// </summary>
    public static async Task<T?> SingleOrDefaultAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, bool>> predicate,
        CancellationToken cancellationToken = default)
    {
        return await source.Where(predicate).SingleOrDefaultAsync(cancellationToken).ConfigureAwait(false);
    }

    #endregion

    #region Count / LongCount

    /// <summary>
    /// Asynchronously returns the count of elements.
    /// </summary>
    public static async Task<int> CountAsync<T>(
        this IQueryable<T> source,
        CancellationToken cancellationToken = default)
    {
        if (source.Provider is ClickHouseQueryProvider provider)
        {
            // Optimized: generate SELECT count() query
            var countExpression = Expression.Call(
                typeof(Queryable),
                nameof(Queryable.Count),
                new[] { typeof(T) },
                source.Expression);

            var sql = provider.TranslateToSql(countExpression);

            var result = await provider.Context.Connection
                .ExecuteScalarAsync<long>(sql, cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            return (int)result;
        }

        // Fallback: enumerate and count
        if (source is not IAsyncEnumerable<T> asyncEnumerable)
            throw new InvalidOperationException("Source does not support async enumeration.");

        int count = 0;
        await foreach (var _ in asyncEnumerable.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            count++;
        }
        return count;
    }

    /// <summary>
    /// Asynchronously returns the count of elements matching the predicate.
    /// </summary>
    public static async Task<int> CountAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, bool>> predicate,
        CancellationToken cancellationToken = default)
    {
        return await source.Where(predicate).CountAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously returns the count of elements as a long.
    /// </summary>
    public static async Task<long> LongCountAsync<T>(
        this IQueryable<T> source,
        CancellationToken cancellationToken = default)
    {
        if (source.Provider is ClickHouseQueryProvider provider)
        {
            var countExpression = Expression.Call(
                typeof(Queryable),
                nameof(Queryable.LongCount),
                new[] { typeof(T) },
                source.Expression);

            var sql = provider.TranslateToSql(countExpression);

            return await provider.Context.Connection
                .ExecuteScalarAsync<long>(sql, cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        }

        // Fallback: enumerate and count
        if (source is not IAsyncEnumerable<T> asyncEnumerable)
            throw new InvalidOperationException("Source does not support async enumeration.");

        long count = 0;
        await foreach (var _ in asyncEnumerable.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            count++;
        }
        return count;
    }

    #endregion

    #region Any / All

    /// <summary>
    /// Asynchronously determines whether the sequence contains any elements.
    /// </summary>
    public static async Task<bool> AnyAsync<T>(
        this IQueryable<T> source,
        CancellationToken cancellationToken = default)
    {
        var query = source.Take(1);

        if (query is not IAsyncEnumerable<T> asyncEnumerable)
            throw new InvalidOperationException("Source does not support async enumeration.");

        await foreach (var _ in asyncEnumerable.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            return true;
        }

        return false;
    }

    /// <summary>
    /// Asynchronously determines whether any element matches the predicate.
    /// </summary>
    public static async Task<bool> AnyAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, bool>> predicate,
        CancellationToken cancellationToken = default)
    {
        return await source.Where(predicate).AnyAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously determines whether all elements match the predicate.
    /// </summary>
    public static async Task<bool> AllAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, bool>> predicate,
        CancellationToken cancellationToken = default)
    {
        // All(predicate) == !Any(!predicate)
        var negatedPredicate = Expression.Lambda<Func<T, bool>>(
            Expression.Not(predicate.Body),
            predicate.Parameters);

        return !await source.Where(negatedPredicate).AnyAsync(cancellationToken).ConfigureAwait(false);
    }

    #endregion

    #region Sum / Average / Min / Max

    /// <summary>
    /// Asynchronously computes the sum of a sequence.
    /// </summary>
    public static async Task<TResult> SumAsync<T, TResult>(
        this IQueryable<T> source,
        Expression<Func<T, TResult>> selector,
        CancellationToken cancellationToken = default)
        where TResult : struct
    {
        if (source.Provider is ClickHouseQueryProvider provider)
        {
            var sumExpression = Expression.Call(
                typeof(Queryable),
                nameof(Queryable.Sum),
                new[] { typeof(T), typeof(TResult) },
                source.Expression,
                selector);

            var sql = provider.TranslateToSql(sumExpression);

            var result = await provider.Context.Connection
                .ExecuteScalarAsync<TResult>(sql, cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            // For struct types, the result is directly usable (default if null)
            return result;
        }

        throw new InvalidOperationException("SumAsync requires a ClickHouse query provider.");
    }

    /// <summary>
    /// Asynchronously computes the average of a sequence.
    /// </summary>
    public static async Task<double> AverageAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, int>> selector,
        CancellationToken cancellationToken = default)
    {
        if (source.Provider is ClickHouseQueryProvider provider)
        {
            var avgExpression = Expression.Call(
                typeof(Queryable),
                nameof(Queryable.Average),
                new[] { typeof(T) },
                source.Expression,
                selector);

            var sql = provider.TranslateToSql(avgExpression);

            return await provider.Context.Connection
                .ExecuteScalarAsync<double>(sql, cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        }

        throw new InvalidOperationException("AverageAsync requires a ClickHouse query provider.");
    }

    /// <summary>
    /// Asynchronously computes the average of a sequence.
    /// </summary>
    public static async Task<double> AverageAsync<T>(
        this IQueryable<T> source,
        Expression<Func<T, double>> selector,
        CancellationToken cancellationToken = default)
    {
        if (source.Provider is ClickHouseQueryProvider provider)
        {
            var avgExpression = Expression.Call(
                typeof(Queryable),
                nameof(Queryable.Average),
                new[] { typeof(T) },
                source.Expression,
                selector);

            var sql = provider.TranslateToSql(avgExpression);

            return await provider.Context.Connection
                .ExecuteScalarAsync<double>(sql, cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        }

        throw new InvalidOperationException("AverageAsync requires a ClickHouse query provider.");
    }

    /// <summary>
    /// Asynchronously returns the minimum value.
    /// </summary>
    public static async Task<TResult> MinAsync<T, TResult>(
        this IQueryable<T> source,
        Expression<Func<T, TResult>> selector,
        CancellationToken cancellationToken = default)
    {
        if (source.Provider is ClickHouseQueryProvider provider)
        {
            var minExpression = Expression.Call(
                typeof(Queryable),
                nameof(Queryable.Min),
                new[] { typeof(T), typeof(TResult) },
                source.Expression,
                selector);

            var sql = provider.TranslateToSql(minExpression);

            return await provider.Context.Connection
                .ExecuteScalarAsync<TResult>(sql, cancellationToken: cancellationToken)
                .ConfigureAwait(false) ?? default!;
        }

        throw new InvalidOperationException("MinAsync requires a ClickHouse query provider.");
    }

    /// <summary>
    /// Asynchronously returns the maximum value.
    /// </summary>
    public static async Task<TResult> MaxAsync<T, TResult>(
        this IQueryable<T> source,
        Expression<Func<T, TResult>> selector,
        CancellationToken cancellationToken = default)
    {
        if (source.Provider is ClickHouseQueryProvider provider)
        {
            var maxExpression = Expression.Call(
                typeof(Queryable),
                nameof(Queryable.Max),
                new[] { typeof(T), typeof(TResult) },
                source.Expression,
                selector);

            var sql = provider.TranslateToSql(maxExpression);

            return await provider.Context.Connection
                .ExecuteScalarAsync<TResult>(sql, cancellationToken: cancellationToken)
                .ConfigureAwait(false) ?? default!;
        }

        throw new InvalidOperationException("MaxAsync requires a ClickHouse query provider.");
    }

    #endregion
}
