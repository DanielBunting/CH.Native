using System.Data;
using Dapper;

namespace CH.Native.Dapper;

/// <summary>
/// Thin pass-through extensions on <see cref="IDbConnection"/> that delegate to
/// Dapper's <see cref="SqlMapper"/>. These exist so users can <c>using CH.Native.Dapper;</c>
/// alone and still have execute-style methods bind on <see cref="IDbConnection"/>-typed
/// variables; they carry no fast path of their own (none is possible at the
/// <see cref="IDbConnection"/> receiver type without colliding with Dapper).
/// </summary>
/// <remarks>
/// <para>
/// Row-shaped methods (<c>QueryAsync&lt;T&gt;</c>, <c>QueryFirstAsync&lt;T&gt;</c>, etc.)
/// are not extended on <see cref="IDbConnection"/> — they live only on the
/// concrete CH connection type via <see cref="ClickHouseConnectionDapperExtensions"/>.
/// C# extension resolution
/// then picks the fast path automatically when the variable is typed as a CH
/// connection, and falls through to Dapper's <see cref="IDbConnection"/> extension
/// when it isn't. That removes the historic ambiguity between
/// <c>using Dapper;</c> and <c>using CH.Native.Dapper;</c> for row-shaped calls.
/// </para>
/// <para>
/// The methods here (<c>Execute</c>, <c>ExecuteScalar</c>, <c>QueryMultiple</c>)
/// have no fast path even on a CH connection — they are pure delegates, kept
/// for namespace-import convenience.
/// </para>
/// </remarks>
public static class IDbConnectionDapperExtensions
{
    /// <summary>Executes a non-query command; returns affected row count. Delegates to Dapper.</summary>
    public static Task<int> ExecuteAsync(
        this IDbConnection cnn,
        string sql,
        object? param = null,
        IDbTransaction? transaction = null,
        int? commandTimeout = null,
        CommandType? commandType = null)
        => SqlMapper.ExecuteAsync(cnn, sql, param, transaction, commandTimeout, commandType);

    /// <summary>Executes a scalar query and returns the first column of the first row as <see cref="object"/>. Delegates to Dapper.</summary>
    public static Task<object?> ExecuteScalarAsync(
        this IDbConnection cnn,
        string sql,
        object? param = null,
        IDbTransaction? transaction = null,
        int? commandTimeout = null,
        CommandType? commandType = null)
        => SqlMapper.ExecuteScalarAsync(cnn, sql, param, transaction, commandTimeout, commandType);

    /// <summary>Executes a scalar query and returns the first column of the first row as <typeparamref name="T"/>. Delegates to Dapper.</summary>
    public static Task<T?> ExecuteScalarAsync<T>(
        this IDbConnection cnn,
        string sql,
        object? param = null,
        IDbTransaction? transaction = null,
        int? commandTimeout = null,
        CommandType? commandType = null)
        => SqlMapper.ExecuteScalarAsync<T>(cnn, sql, param, transaction, commandTimeout, commandType);

    /// <summary>
    /// Not supported. ClickHouse has no multiple-result-set / multi-statement
    /// concept, so Dapper's grid-reader pattern cannot be honoured. Always throws
    /// <see cref="NotSupportedException"/> with guidance to issue separate queries.
    /// </summary>
    /// <exception cref="NotSupportedException">Always thrown.</exception>
    public static Task<SqlMapper.GridReader> QueryMultipleAsync(
        this IDbConnection cnn,
        string sql,
        object? param = null,
        IDbTransaction? transaction = null,
        int? commandTimeout = null,
        CommandType? commandType = null)
        => throw new NotSupportedException(
            "ClickHouse does not support multiple result sets, so QueryMultipleAsync " +
            "(Dapper's grid-reader pattern) cannot be used. ClickHouse rejects multi-statement " +
            "queries server-side. Issue each statement as a separate query instead.");

    /// <summary>Sync execute; returns affected row count. Delegates to Dapper.</summary>
    public static int Execute(this IDbConnection cnn, string sql, object? param = null,
        IDbTransaction? transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        => SqlMapper.Execute(cnn, sql, param, transaction, commandTimeout, commandType);

    /// <summary>Sync execute returning a scalar object. Delegates to Dapper.</summary>
    public static object? ExecuteScalar(this IDbConnection cnn, string sql, object? param = null,
        IDbTransaction? transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        => SqlMapper.ExecuteScalar(cnn, sql, param, transaction, commandTimeout, commandType);

    /// <summary>Sync execute returning a scalar typed as <typeparamref name="T"/>. Delegates to Dapper.</summary>
    public static T? ExecuteScalar<T>(this IDbConnection cnn, string sql, object? param = null,
        IDbTransaction? transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        => SqlMapper.ExecuteScalar<T>(cnn, sql, param, transaction, commandTimeout, commandType);
}
