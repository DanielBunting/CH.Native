using System.Runtime.CompilerServices;
using CH.Native.Connection;
using CH.Native.Data;
using CH.Native.Mapping;
using CH.Native.Results;

namespace CH.Native.Commands;

/// <summary>
/// Represents a SQL command with parameters to execute against ClickHouse.
/// </summary>
public sealed class ClickHouseCommand : IAsyncDisposable
{
    private readonly ClickHouseConnection _connection;

    /// <summary>
    /// Gets or sets the SQL command text.
    /// </summary>
    public string CommandText { get; set; } = string.Empty;

    /// <summary>
    /// Gets the parameter collection.
    /// </summary>
    public ClickHouseParameterCollection Parameters { get; } = new();

    /// <summary>
    /// Creates a new command associated with the specified connection.
    /// </summary>
    /// <param name="connection">The connection to use for executing the command.</param>
    public ClickHouseCommand(ClickHouseConnection connection)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
    }

    /// <summary>
    /// Creates a new command with the specified SQL text.
    /// </summary>
    /// <param name="connection">The connection to use for executing the command.</param>
    /// <param name="commandText">The SQL command text.</param>
    public ClickHouseCommand(ClickHouseConnection connection, string commandText)
        : this(connection)
    {
        CommandText = commandText;
    }

    /// <summary>
    /// Executes the command and returns the first column of the first row.
    /// </summary>
    /// <typeparam name="T">The expected result type.</typeparam>
    /// <param name="progress">Optional progress reporter.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The scalar result, or default if no rows returned.</returns>
    public Task<T?> ExecuteScalarAsync<T>(
        IProgress<QueryProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        return _connection.ExecuteScalarWithParametersAsync<T>(
            CommandText, Parameters, progress, cancellationToken);
    }

    /// <summary>
    /// Executes the command that does not return rows.
    /// </summary>
    /// <param name="progress">Optional progress reporter.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of rows affected.</returns>
    public Task<long> ExecuteNonQueryAsync(
        IProgress<QueryProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        return _connection.ExecuteNonQueryWithParametersAsync(
            CommandText, Parameters, progress, cancellationToken);
    }

    /// <summary>
    /// Executes the command and returns a data reader for streaming results.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A data reader for iterating through results.</returns>
    public Task<ClickHouseDataReader> ExecuteReaderAsync(
        CancellationToken cancellationToken = default)
    {
        return _connection.ExecuteReaderWithParametersAsync(
            CommandText, Parameters, cancellationToken);
    }

    /// <summary>
    /// Executes the command and returns an async enumerable of rows.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An async enumerable of rows.</returns>
    public async IAsyncEnumerable<ClickHouseRow> QueryAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await using var reader = await ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            yield return new ClickHouseRow(reader);
        }
    }

    /// <summary>
    /// Executes the command and returns an async enumerable of mapped objects.
    /// </summary>
    /// <typeparam name="T">The type to map rows to. Must have a parameterless constructor.</typeparam>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An async enumerable of mapped objects.</returns>
    public async IAsyncEnumerable<T> QueryAsync<T>(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where T : new()
    {
        await using var reader = await ExecuteReaderAsync(cancellationToken);

        // Need to call ReadAsync at least once to initialize schema before creating mapper
        if (!await reader.ReadAsync(cancellationToken))
            yield break;

        // Use reflection-based TypeMapper
        var mapper = new TypeMapper<T>(reader);

        // Map the first row
        yield return mapper.Map(reader);

        // Map remaining rows
        while (await reader.ReadAsync(cancellationToken))
        {
            yield return mapper.Map(reader);
        }
    }

    /// <summary>
    /// Disposes the command asynchronously.
    /// </summary>
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
