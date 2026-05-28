using System.Data;
using System.Data.Common;
using CH.Native.Ado;
using Dapper;

namespace CH.Native.Dapper;

/// <summary>
/// Dapper-compatible extensions on <see cref="IDbConnection"/> that dispatch to
/// CH.Native's fast typed-accessor path when the connection is a
/// <see cref="ClickHouseDbConnection"/>, and delegate to Dapper's
/// <see cref="SqlMapper"/> for every other case.
/// </summary>
/// <remarks>
/// <para>
/// Designed for the namespace-swap pattern: users replace
/// <c>using Dapper;</c> with <c>using CH.Native.Dapper;</c> and continue
/// calling <c>connection.QueryAsync&lt;T&gt;(sql, params)</c> against an
/// <see cref="IDbConnection"/> (typical DI shape). The fast path triggers
/// automatically when the underlying connection is ours; Dapper handles
/// every other provider.
/// </para>
/// <para>
/// <b>Important:</b> if both <c>using CH.Native.Dapper;</c> and
/// <c>using Dapper;</c> are present, calls on <see cref="IDbConnection"/>-typed
/// variables fail with an ambiguity error. Import one or the other.
/// </para>
/// <para>
/// Methods that don't materialise rows (<c>ExecuteAsync</c>, <c>ExecuteScalarAsync</c>,
/// <c>QueryMultipleAsync</c>) are thin delegates to Dapper — there's no boxing
/// tax to fix for those paths.
/// </para>
/// </remarks>
public static class IDbConnectionDapperExtensions
{
    // ----------------------------------------------------------------------
    // Async row-mapping — fast path when connection is ClickHouseDbConnection.
    // Transaction / commandTimeout / commandType arguments force Dapper
    // fallback because the CH.Native fast path doesn't translate those today.
    // ----------------------------------------------------------------------

    /// <summary>Async query returning an enumerable of <typeparamref name="T"/>. Fast path when the connection is a <see cref="ClickHouseDbConnection"/> with no transaction / command-timeout / command-type override.</summary>
    public static Task<IEnumerable<T>> QueryAsync<T>(
        this IDbConnection cnn,
        string sql,
        object? param = null,
        IDbTransaction? transaction = null,
        int? commandTimeout = null,
        CommandType? commandType = null)
    {
        if (CanFastPath(cnn, transaction, commandTimeout, commandType, out var ch))
            return FastQueryAsync<T>(ch, sql, param);
        return SqlMapper.QueryAsync<T>(cnn, sql, param, transaction, commandTimeout, commandType);
    }

    /// <summary>Async query returning the first row as <typeparamref name="T"/>; throws if empty.</summary>
    public static Task<T> QueryFirstAsync<T>(
        this IDbConnection cnn,
        string sql,
        object? param = null,
        IDbTransaction? transaction = null,
        int? commandTimeout = null,
        CommandType? commandType = null)
    {
        if (CanFastPath(cnn, transaction, commandTimeout, commandType, out var ch))
            return ch.QueryFirstAsync<T>(sql, param);
        return SqlMapper.QueryFirstAsync<T>(cnn, sql, param, transaction, commandTimeout, commandType);
    }

    /// <summary>Async query returning the first row as <typeparamref name="T"/>, or <see langword="default"/> if empty.</summary>
    public static Task<T?> QueryFirstOrDefaultAsync<T>(
        this IDbConnection cnn,
        string sql,
        object? param = null,
        IDbTransaction? transaction = null,
        int? commandTimeout = null,
        CommandType? commandType = null)
    {
        if (CanFastPath(cnn, transaction, commandTimeout, commandType, out var ch))
            return ch.QueryFirstOrDefaultAsync<T>(sql, param);
        return SqlMapper.QueryFirstOrDefaultAsync<T>(cnn, sql, param, transaction, commandTimeout, commandType);
    }

    /// <summary>Async query returning exactly one row; throws on empty or multi-row results.</summary>
    public static Task<T> QuerySingleAsync<T>(
        this IDbConnection cnn,
        string sql,
        object? param = null,
        IDbTransaction? transaction = null,
        int? commandTimeout = null,
        CommandType? commandType = null)
    {
        if (CanFastPath(cnn, transaction, commandTimeout, commandType, out var ch))
            return ch.QuerySingleAsync<T>(sql, param);
        return SqlMapper.QuerySingleAsync<T>(cnn, sql, param, transaction, commandTimeout, commandType);
    }

    /// <summary>Async query returning at most one row; throws on multi-row results, returns <see langword="default"/> on empty.</summary>
    public static Task<T?> QuerySingleOrDefaultAsync<T>(
        this IDbConnection cnn,
        string sql,
        object? param = null,
        IDbTransaction? transaction = null,
        int? commandTimeout = null,
        CommandType? commandType = null)
    {
        if (CanFastPath(cnn, transaction, commandTimeout, commandType, out var ch))
            return ch.QuerySingleOrDefaultAsync<T>(sql, param);
        return SqlMapper.QuerySingleOrDefaultAsync<T>(cnn, sql, param, transaction, commandTimeout, commandType);
    }

    // ----------------------------------------------------------------------
    // Async dynamic / non-row-mapping — pure Dapper delegation.
    // ----------------------------------------------------------------------

    /// <summary>Async query returning <c>dynamic</c> rows. Delegates to Dapper (no boxing-tax fast path for dynamic shapes).</summary>
    public static Task<IEnumerable<dynamic>> QueryAsync(
        this IDbConnection cnn,
        string sql,
        object? param = null,
        IDbTransaction? transaction = null,
        int? commandTimeout = null,
        CommandType? commandType = null)
        => SqlMapper.QueryAsync(cnn, sql, param, transaction, commandTimeout, commandType);

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

    /// <summary>Executes a query returning multiple result sets. Delegates to Dapper. ClickHouse does not support multiple result sets — calling this on a CH connection will fail on the second <c>Read</c>.</summary>
    public static Task<SqlMapper.GridReader> QueryMultipleAsync(
        this IDbConnection cnn,
        string sql,
        object? param = null,
        IDbTransaction? transaction = null,
        int? commandTimeout = null,
        CommandType? commandType = null)
        => SqlMapper.QueryMultipleAsync(cnn, sql, param, transaction, commandTimeout, commandType);

    // ----------------------------------------------------------------------
    // Sync row-mapping — delegate to Dapper. Our fast path is async-only;
    // sync callers go through DbDataReader.Read() which already benefits
    // from the typed accessor + cached-Task fast paths landed earlier.
    // ----------------------------------------------------------------------

    /// <summary>Sync query returning an enumerable of <typeparamref name="T"/>. Delegates to Dapper.</summary>
    public static IEnumerable<T> Query<T>(
        this IDbConnection cnn,
        string sql,
        object? param = null,
        IDbTransaction? transaction = null,
        bool buffered = true,
        int? commandTimeout = null,
        CommandType? commandType = null)
        => SqlMapper.Query<T>(cnn, sql, param, transaction, buffered, commandTimeout, commandType);

    /// <summary>Sync query returning <c>dynamic</c> rows. Delegates to Dapper.</summary>
    public static IEnumerable<dynamic> Query(
        this IDbConnection cnn,
        string sql,
        object? param = null,
        IDbTransaction? transaction = null,
        bool buffered = true,
        int? commandTimeout = null,
        CommandType? commandType = null)
        => SqlMapper.Query(cnn, sql, param, transaction, buffered, commandTimeout, commandType);

    /// <summary>Sync query returning the first row. Delegates to Dapper.</summary>
    public static T QueryFirst<T>(this IDbConnection cnn, string sql, object? param = null,
        IDbTransaction? transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        => SqlMapper.QueryFirst<T>(cnn, sql, param, transaction, commandTimeout, commandType);

    /// <summary>Sync query returning the first row or <see langword="default"/>. Delegates to Dapper.</summary>
    public static T? QueryFirstOrDefault<T>(this IDbConnection cnn, string sql, object? param = null,
        IDbTransaction? transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        => SqlMapper.QueryFirstOrDefault<T>(cnn, sql, param, transaction, commandTimeout, commandType);

    /// <summary>Sync query returning exactly one row. Delegates to Dapper.</summary>
    public static T QuerySingle<T>(this IDbConnection cnn, string sql, object? param = null,
        IDbTransaction? transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        => SqlMapper.QuerySingle<T>(cnn, sql, param, transaction, commandTimeout, commandType);

    /// <summary>Sync query returning at most one row. Delegates to Dapper.</summary>
    public static T? QuerySingleOrDefault<T>(this IDbConnection cnn, string sql, object? param = null,
        IDbTransaction? transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        => SqlMapper.QuerySingleOrDefault<T>(cnn, sql, param, transaction, commandTimeout, commandType);

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

    // ----------------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------------

    private static bool CanFastPath(
        IDbConnection cnn,
        IDbTransaction? transaction,
        int? commandTimeout,
        CommandType? commandType,
        out ClickHouseDbConnection ch)
    {
        if (cnn is ClickHouseDbConnection chConn
            && transaction is null
            && commandTimeout is null
            && (commandType is null || commandType == CommandType.Text))
        {
            ch = chConn;
            return true;
        }
        ch = null!;
        return false;
    }

    private static async Task<IEnumerable<T>> FastQueryAsync<T>(
        ClickHouseDbConnection ch, string sql, object? param)
    {
        // Materialise into a List<T> so the return shape matches Dapper's
        // default buffered behaviour.
        return await ch.QueryAsync<T>(sql, param).ConfigureAwait(false);
    }
}
