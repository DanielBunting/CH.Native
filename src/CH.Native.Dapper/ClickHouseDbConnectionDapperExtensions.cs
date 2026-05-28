using System.Data;
using System.Runtime.CompilerServices;
using CH.Native.Ado;
using CH.Native.Connection;

namespace CH.Native.Dapper;

/// <summary>
/// Dapper-style query extensions on <see cref="ClickHouseDbConnection"/> that
/// bypass Dapper's compiled mapper for the row-materialisation hot path and
/// route through CH.Native's typed-accessor <c>TypeMapper&lt;T&gt;</c> instead.
/// </summary>
/// <remarks>
/// <para>
/// These methods are wire-compatible with Dapper's <c>SqlMapper.QueryAsync</c>
/// surface: same name, same shape of <c>(string sql, object? param)</c>. C#
/// resolves extensions on the more-derived type first, so a variable typed as
/// <see cref="ClickHouseDbConnection"/> will hit this fast path even when
/// <c>using Dapper;</c> is also imported.
/// </para>
/// <para>
/// For variables typed as <see cref="IDbConnection"/> or <see cref="System.Data.Common.DbConnection"/>
/// (the typical DI pattern), see <c>IDbConnectionDapperExtensions</c> in this
/// same namespace — which uses runtime dispatch to choose the fast path when
/// the connection happens to be ours.
/// </para>
/// </remarks>
public static class ClickHouseDbConnectionDapperExtensions
{
    /// <summary>
    /// Executes a query and returns all rows materialised into a <see cref="List{T}"/>.
    /// Mirrors Dapper's <c>QueryAsync&lt;T&gt;</c> default (buffered) shape.
    /// </summary>
    /// <typeparam name="T">Target row type. Must have a parameterless public constructor or a single public ctor with parameter names matching column names.</typeparam>
    /// <param name="connection">An open or closed <see cref="ClickHouseDbConnection"/>. Opened on demand if closed.</param>
    /// <param name="sql">SQL with <c>@param</c> placeholders.</param>
    /// <param name="param">Anonymous object or <see cref="IDictionary{TKey,TValue}"/> of parameter values; <see langword="null"/> for none.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public static async Task<IReadOnlyList<T>> QueryAsync<T>(
        this ClickHouseDbConnection connection,
        string sql,
        object? param = null,
        CancellationToken cancellationToken = default)
    {
        var native = await EnsureOpenAsync(connection, cancellationToken).ConfigureAwait(false);
        var list = new List<T>();
        await foreach (var row in StreamCore<T>(native, sql, param, cancellationToken).ConfigureAwait(false))
        {
            list.Add(row);
        }
        return list;
    }

    /// <summary>
    /// Executes a query and streams rows one at a time without buffering.
    /// Use this for very large result sets that would otherwise pressure the GC.
    /// </summary>
    public static async IAsyncEnumerable<T> QueryStreamAsync<T>(
        this ClickHouseDbConnection connection,
        string sql,
        object? param = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var native = await EnsureOpenAsync(connection, cancellationToken).ConfigureAwait(false);
        await foreach (var row in StreamCore<T>(native, sql, param, cancellationToken).ConfigureAwait(false))
        {
            yield return row;
        }
    }

    /// <summary>
    /// Executes a query and returns the first row. Throws <see cref="InvalidOperationException"/>
    /// if the result set is empty, matching Dapper's <c>QueryFirstAsync</c> contract.
    /// </summary>
    public static async Task<T> QueryFirstAsync<T>(
        this ClickHouseDbConnection connection,
        string sql,
        object? param = null,
        CancellationToken cancellationToken = default)
    {
        var native = await EnsureOpenAsync(connection, cancellationToken).ConfigureAwait(false);
        await foreach (var row in StreamCore<T>(native, sql, param, cancellationToken).ConfigureAwait(false))
        {
            return row;
        }
        throw new InvalidOperationException("Sequence contains no elements.");
    }

    /// <summary>
    /// Executes a query and returns the first row, or <see langword="default"/> if the result
    /// set is empty. Matches Dapper's <c>QueryFirstOrDefaultAsync</c> contract.
    /// </summary>
    public static async Task<T?> QueryFirstOrDefaultAsync<T>(
        this ClickHouseDbConnection connection,
        string sql,
        object? param = null,
        CancellationToken cancellationToken = default)
    {
        var native = await EnsureOpenAsync(connection, cancellationToken).ConfigureAwait(false);
        await foreach (var row in StreamCore<T>(native, sql, param, cancellationToken).ConfigureAwait(false))
        {
            return row;
        }
        return default;
    }

    /// <summary>
    /// Executes a query and returns exactly one row. Throws <see cref="InvalidOperationException"/>
    /// if zero or more than one row is returned. Matches Dapper's <c>QuerySingleAsync</c>.
    /// </summary>
    public static async Task<T> QuerySingleAsync<T>(
        this ClickHouseDbConnection connection,
        string sql,
        object? param = null,
        CancellationToken cancellationToken = default)
    {
        var native = await EnsureOpenAsync(connection, cancellationToken).ConfigureAwait(false);
        T? captured = default;
        bool found = false;
        await foreach (var row in StreamCore<T>(native, sql, param, cancellationToken).ConfigureAwait(false))
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

    /// <summary>
    /// Executes a query and returns at most one row, throwing if more than one is returned
    /// and returning <see langword="default"/> if none. Matches Dapper's <c>QuerySingleOrDefaultAsync</c>.
    /// </summary>
    public static async Task<T?> QuerySingleOrDefaultAsync<T>(
        this ClickHouseDbConnection connection,
        string sql,
        object? param = null,
        CancellationToken cancellationToken = default)
    {
        var native = await EnsureOpenAsync(connection, cancellationToken).ConfigureAwait(false);
        T? captured = default;
        bool found = false;
        await foreach (var row in StreamCore<T>(native, sql, param, cancellationToken).ConfigureAwait(false))
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

    private static async ValueTask<ClickHouseConnection> EnsureOpenAsync(
        ClickHouseDbConnection connection,
        CancellationToken cancellationToken)
    {
        if (connection.State != ConnectionState.Open)
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        }
        return connection.Inner;
    }

    private static IAsyncEnumerable<T> StreamCore<T>(
        ClickHouseConnection native,
        string sql,
        object? param,
        CancellationToken cancellationToken)
    {
        // Route to the existing parameter-binding overloads on
        // ClickHouseConnectionExtensions / ClickHouseConnection. These already
        // use TypeMapper<T> under the hood, which (after Part A) reads via
        // typed accessors — no per-row boxing for value-type columns.
        return param switch
        {
            null => native.QueryStreamAsync<T>(sql, cancellationToken),
            IDictionary<string, object?> dict => native.QueryStreamAsync<T>(sql, dict, cancellationToken),
            _ => native.QueryStreamAsync<T>(sql, param, cancellationToken),
        };
    }
}
