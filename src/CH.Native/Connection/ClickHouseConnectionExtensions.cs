using System.Reflection;
using System.Runtime.CompilerServices;
using CH.Native.Commands;
using CH.Native.Results;

namespace CH.Native.Connection;

/// <summary>
/// Extension methods for ClickHouseConnection with parameter support.
/// </summary>
public static class ClickHouseConnectionExtensions
{
    #region ExecuteScalarAsync

    /// <summary>
    /// Executes a scalar query with anonymous object parameters.
    /// </summary>
    /// <typeparam name="T">The expected result type.</typeparam>
    /// <param name="connection">The connection.</param>
    /// <param name="sql">The SQL query with @param placeholders.</param>
    /// <param name="parameters">Anonymous object with properties matching parameter names.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The scalar result, or default if no rows returned.</returns>
    /// <example>
    /// var count = await connection.ExecuteScalarAsync&lt;int&gt;(
    ///     "SELECT count() FROM users WHERE age > @minAge",
    ///     new { minAge = 18 });
    /// </example>
    public static async Task<T?> ExecuteScalarAsync<T>(
        this ClickHouseConnection connection,
        string sql,
        object parameters,
        CancellationToken cancellationToken = default)
    {
        await using var command = connection.CreateCommand(sql);
        AddParametersFromObject(command.Parameters, parameters);
        return await command.ExecuteScalarAsync<T>(cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Executes a scalar query with dictionary parameters.
    /// </summary>
    /// <typeparam name="T">The expected result type.</typeparam>
    /// <param name="connection">The connection.</param>
    /// <param name="sql">The SQL query with @param placeholders.</param>
    /// <param name="parameters">Dictionary of parameter names and values.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The scalar result, or default if no rows returned.</returns>
    /// <example>
    /// var parameters = new Dictionary&lt;string, object?&gt; { ["minAge"] = 18 };
    /// var count = await connection.ExecuteScalarAsync&lt;int&gt;(
    ///     "SELECT count() FROM users WHERE age > @minAge",
    ///     parameters);
    /// </example>
    public static async Task<T?> ExecuteScalarAsync<T>(
        this ClickHouseConnection connection,
        string sql,
        IDictionary<string, object?> parameters,
        CancellationToken cancellationToken = default)
    {
        await using var command = connection.CreateCommand(sql);
        AddParametersFromDictionary(command.Parameters, parameters);
        return await command.ExecuteScalarAsync<T>(cancellationToken: cancellationToken);
    }

    #endregion

    #region ExecuteNonQueryAsync

    /// <summary>
    /// Executes a non-query with anonymous object parameters.
    /// </summary>
    /// <param name="connection">The connection.</param>
    /// <param name="sql">The SQL query with @param placeholders.</param>
    /// <param name="parameters">Anonymous object with properties matching parameter names.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of rows affected.</returns>
    public static async Task<long> ExecuteNonQueryAsync(
        this ClickHouseConnection connection,
        string sql,
        object parameters,
        CancellationToken cancellationToken = default)
    {
        await using var command = connection.CreateCommand(sql);
        AddParametersFromObject(command.Parameters, parameters);
        return await command.ExecuteNonQueryAsync(cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Executes a non-query with dictionary parameters.
    /// </summary>
    /// <param name="connection">The connection.</param>
    /// <param name="sql">The SQL query with @param placeholders.</param>
    /// <param name="parameters">Dictionary of parameter names and values.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of rows affected.</returns>
    public static async Task<long> ExecuteNonQueryAsync(
        this ClickHouseConnection connection,
        string sql,
        IDictionary<string, object?> parameters,
        CancellationToken cancellationToken = default)
    {
        await using var command = connection.CreateCommand(sql);
        AddParametersFromDictionary(command.Parameters, parameters);
        return await command.ExecuteNonQueryAsync(cancellationToken: cancellationToken);
    }

    #endregion

    #region QueryAsync (ClickHouseRow)

    /// <summary>
    /// Executes a query with anonymous object parameters and returns rows.
    /// </summary>
    /// <param name="connection">The connection.</param>
    /// <param name="sql">The SQL query with @param placeholders.</param>
    /// <param name="parameters">Anonymous object with properties matching parameter names.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An async enumerable of rows.</returns>
    public static async IAsyncEnumerable<ClickHouseRow> QueryAsync(
        this ClickHouseConnection connection,
        string sql,
        object parameters,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await using var command = connection.CreateCommand(sql);
        AddParametersFromObject(command.Parameters, parameters);

        await foreach (var row in command.QueryAsync(cancellationToken))
        {
            yield return row;
        }
    }

    /// <summary>
    /// Executes a query with dictionary parameters and returns rows.
    /// </summary>
    /// <param name="connection">The connection.</param>
    /// <param name="sql">The SQL query with @param placeholders.</param>
    /// <param name="parameters">Dictionary of parameter names and values.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An async enumerable of rows.</returns>
    public static async IAsyncEnumerable<ClickHouseRow> QueryAsync(
        this ClickHouseConnection connection,
        string sql,
        IDictionary<string, object?> parameters,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await using var command = connection.CreateCommand(sql);
        AddParametersFromDictionary(command.Parameters, parameters);

        await foreach (var row in command.QueryAsync(cancellationToken))
        {
            yield return row;
        }
    }

    #endregion

    #region QueryAsync<T> (typed)

    /// <summary>
    /// Executes a query with anonymous object parameters and returns mapped objects.
    /// </summary>
    /// <typeparam name="T">The type to map rows to. Must have a parameterless constructor.</typeparam>
    /// <param name="connection">The connection.</param>
    /// <param name="sql">The SQL query with @param placeholders.</param>
    /// <param name="parameters">Anonymous object with properties matching parameter names.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An async enumerable of mapped objects.</returns>
    public static async IAsyncEnumerable<T> QueryAsync<T>(
        this ClickHouseConnection connection,
        string sql,
        object parameters,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where T : new()
    {
        await using var command = connection.CreateCommand(sql);
        AddParametersFromObject(command.Parameters, parameters);

        await foreach (var item in command.QueryAsync<T>(cancellationToken))
        {
            yield return item;
        }
    }

    /// <summary>
    /// Executes a query with dictionary parameters and returns mapped objects.
    /// </summary>
    /// <typeparam name="T">The type to map rows to. Must have a parameterless constructor.</typeparam>
    /// <param name="connection">The connection.</param>
    /// <param name="sql">The SQL query with @param placeholders.</param>
    /// <param name="parameters">Dictionary of parameter names and values.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An async enumerable of mapped objects.</returns>
    public static async IAsyncEnumerable<T> QueryAsync<T>(
        this ClickHouseConnection connection,
        string sql,
        IDictionary<string, object?> parameters,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where T : new()
    {
        await using var command = connection.CreateCommand(sql);
        AddParametersFromDictionary(command.Parameters, parameters);

        await foreach (var item in command.QueryAsync<T>(cancellationToken))
        {
            yield return item;
        }
    }

    #endregion

    #region Helper Methods

    private static void AddParametersFromObject(
        ClickHouseParameterCollection collection,
        object parameters)
    {
        if (parameters is null)
            return;

        var properties = parameters.GetType().GetProperties(
            BindingFlags.Public | BindingFlags.Instance);

        foreach (var prop in properties)
        {
            var value = prop.GetValue(parameters);
            collection.Add(prop.Name, value);
        }
    }

    private static void AddParametersFromDictionary(
        ClickHouseParameterCollection collection,
        IDictionary<string, object?> parameters)
    {
        if (parameters is null)
            return;

        foreach (var kvp in parameters)
        {
            collection.Add(kvp.Key, kvp.Value);
        }
    }

    #endregion
}
