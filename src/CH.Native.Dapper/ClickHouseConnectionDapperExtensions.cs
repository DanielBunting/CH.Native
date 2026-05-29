using System.Data;
using CH.Native.Connection;

namespace CH.Native.Dapper;

/// <summary>
/// Dapper-style query extensions on <see cref="ClickHouseConnection"/> — the
/// connection type returned by <c>ClickHouseDataSource.OpenConnectionAsync</c>
/// — that bypass Dapper's compiled mapper for the row-materialisation hot path
/// and route through CH.Native's typed-accessor <c>TypeMapper&lt;T&gt;</c>.
/// </summary>
/// <remarks>
/// <para>
/// Sibling of <see cref="ClickHouseDbConnectionDapperExtensions"/> for the
/// pool-rented connection shape. C# resolves extensions on the more-derived
/// receiver first, so calls on a <see cref="ClickHouseConnection"/>-typed
/// variable hit this fast path even when <c>using Dapper;</c> is also imported.
/// </para>
/// <para>
/// Variables typed as <see cref="IDbConnection"/> or <see cref="System.Data.Common.DbConnection"/>
/// bind to Dapper directly — CH.Native.Dapper no longer extends those receivers
/// for row-shaped methods, so there is no ambiguity to resolve.
/// </para>
/// </remarks>
public static class ClickHouseConnectionDapperExtensions
{
    /// <summary>Executes a query and returns all rows materialised into a <see cref="List{T}"/>. Mirrors Dapper's buffered <c>QueryAsync&lt;T&gt;</c> shape.</summary>
    public static async Task<IReadOnlyList<T>> QueryAsync<T>(
        this ClickHouseConnection connection,
        string sql,
        object? param = null,
        CancellationToken cancellationToken = default)
    {
        await EnsureOpenAsync(connection, cancellationToken).ConfigureAwait(false);
        var list = new List<T>();
        await foreach (var row in StreamCore<T>(connection, sql, param, cancellationToken).ConfigureAwait(false))
        {
            list.Add(row);
        }
        return list;
    }

    /// <summary>Executes a query and returns the first row; throws <see cref="InvalidOperationException"/> if empty.</summary>
    public static async Task<T> QueryFirstAsync<T>(
        this ClickHouseConnection connection,
        string sql,
        object? param = null,
        CancellationToken cancellationToken = default)
    {
        await EnsureOpenAsync(connection, cancellationToken).ConfigureAwait(false);
        await foreach (var row in StreamCore<T>(connection, sql, param, cancellationToken).ConfigureAwait(false))
        {
            return row;
        }
        throw new InvalidOperationException("Sequence contains no elements.");
    }

    /// <summary>Executes a query and returns the first row, or <see langword="default"/> if the result set is empty.</summary>
    public static async Task<T?> QueryFirstOrDefaultAsync<T>(
        this ClickHouseConnection connection,
        string sql,
        object? param = null,
        CancellationToken cancellationToken = default)
    {
        await EnsureOpenAsync(connection, cancellationToken).ConfigureAwait(false);
        await foreach (var row in StreamCore<T>(connection, sql, param, cancellationToken).ConfigureAwait(false))
        {
            return row;
        }
        return default;
    }

    /// <summary>Executes a query and returns exactly one row; throws on zero or more than one.</summary>
    public static async Task<T> QuerySingleAsync<T>(
        this ClickHouseConnection connection,
        string sql,
        object? param = null,
        CancellationToken cancellationToken = default)
    {
        await EnsureOpenAsync(connection, cancellationToken).ConfigureAwait(false);
        T? captured = default;
        bool found = false;
        await foreach (var row in StreamCore<T>(connection, sql, param, cancellationToken).ConfigureAwait(false))
        {
            if (found)
                throw new InvalidOperationException("Sequence contains more than one element.");
            captured = row;
            found = true;
        }
        if (!found)
            throw new InvalidOperationException("Sequence contains no elements.");
        return captured!;
    }

    /// <summary>Executes a query and returns at most one row, throwing on more than one and returning <see langword="default"/> on empty.</summary>
    public static async Task<T?> QuerySingleOrDefaultAsync<T>(
        this ClickHouseConnection connection,
        string sql,
        object? param = null,
        CancellationToken cancellationToken = default)
    {
        await EnsureOpenAsync(connection, cancellationToken).ConfigureAwait(false);
        T? captured = default;
        bool found = false;
        await foreach (var row in StreamCore<T>(connection, sql, param, cancellationToken).ConfigureAwait(false))
        {
            if (found)
                throw new InvalidOperationException("Sequence contains more than one element.");
            captured = row;
            found = true;
        }
        return captured;
    }

    // ------------------------------------------------------------------
    // Implementation helpers
    // ------------------------------------------------------------------

    private static async ValueTask EnsureOpenAsync(
        ClickHouseConnection connection,
        CancellationToken cancellationToken)
    {
        if (connection.State != ConnectionState.Open)
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private static IAsyncEnumerable<T> StreamCore<T>(
        ClickHouseConnection native,
        string sql,
        object? param,
        CancellationToken cancellationToken)
    {
        // Important: this class's own QueryStreamAsync extension on
        // ClickHouseConnection is in scope here, so the natural call
        // `native.QueryStreamAsync<T>(sql, ct)` re-binds to ourselves and
        // recurses. Two ways to break that:
        //   - null-param: pass the explicit `queryId: null` named arg — our
        //     extension has no queryId parameter, forcing the instance method.
        //   - parameter cases: invoke the existing parameter-binding helpers
        //     in CH.Native.Connection.ClickHouseConnectionExtensions via their
        //     static-class qualifier so extension lookup never gets a chance
        //     to pick this class.
        return param switch
        {
            null => native.QueryStreamAsync<T>(sql, cancellationToken: cancellationToken, queryId: null),
            IDictionary<string, object?> dict => ClickHouseConnectionExtensions.QueryStreamAsync<T>(native, sql, dict, cancellationToken),
            _ => ClickHouseConnectionExtensions.QueryStreamAsync<T>(native, sql, param, cancellationToken),
        };
    }
}
