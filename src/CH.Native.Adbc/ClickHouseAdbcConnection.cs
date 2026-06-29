using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Ipc;
using CH.Native.Connection;
using CH.Native.Data;

namespace CH.Native.Adbc;

/// <summary>
/// An ADBC connection backed by a single pooled <see cref="ClickHouseConnection"/>.
/// Statements created from this connection execute serially (the native protocol does not
/// multiplex queries over one socket).
/// </summary>
public sealed class ClickHouseAdbcConnection : AdbcConnection
{
    private readonly ClickHouseConnection _connection;

    internal ClickHouseAdbcConnection(ClickHouseConnection connection)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
    }

    internal ClickHouseConnection Inner => _connection;

    /// <inheritdoc />
    public override AdbcStatement CreateStatement() => new ClickHouseAdbcStatement(this);

    /// <inheritdoc />
    public override Schema GetTableSchema(string? catalog, string? dbSchema, string tableName)
    {
        if (string.IsNullOrWhiteSpace(tableName))
            throw new ArgumentException("Table name must be provided.", nameof(tableName));

        // ClickHouse uses database.table; `dbSchema` maps to the database, `catalog` is unused.
        var qualified = string.IsNullOrWhiteSpace(dbSchema)
            ? QuoteIdentifier(tableName)
            : $"{QuoteIdentifier(dbSchema)}.{QuoteIdentifier(tableName)}";

        // A LIMIT 0 query returns the header block with full column metadata and no rows.
        var sql = $"SELECT * FROM {qualified} LIMIT 0";
        return GetSchemaAsync(sql).GetAwaiter().GetResult();
    }

    private async Task<Schema> GetSchemaAsync(string sql)
    {
        Schema? schema = null;

        // Drain the full block stream — a LIMIT 0 query is just a header block plus the trailing
        // EndOfStream. Returning after the first block would leave those bytes unread and poison
        // the connection for its next use; enumerate to completion before returning.
        await foreach (var block in _connection.QueryBlocksAsync(sql).ConfigureAwait(false))
        {
            using (block)
            {
                schema ??= ArrowSchemaMapper.ToSchema(block.ColumnNames, block.ColumnTypes);
            }
        }

        // No block at all (unexpected) — surface an empty schema rather than null.
        return schema ?? new Schema(System.Array.Empty<Field>(), metadata: null);
    }

    private static string QuoteIdentifier(string identifier) =>
        "`" + identifier.Replace("`", "``") + "`";

    /// <inheritdoc />
    public override IArrowArrayStream GetObjects(
        GetObjectsDepth depth,
        string? catalogPattern,
        string? dbSchemaPattern,
        string? tableNamePattern,
        IReadOnlyList<string>? tableTypes,
        string? columnNamePattern) =>
        throw AdbcException.NotImplemented("GetObjects is not yet implemented for the ClickHouse ADBC driver.");

    /// <inheritdoc />
    public override IArrowArrayStream GetTableTypes() =>
        throw AdbcException.NotImplemented("GetTableTypes is not yet implemented for the ClickHouse ADBC driver.");

    /// <inheritdoc />
    public override void Dispose()
    {
        // Returns the connection to the underlying pool.
        _connection.Dispose();
        base.Dispose();
    }
}
