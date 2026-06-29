using Apache.Arrow;
using Apache.Arrow.Adbc;
using CH.Native.Connection;
using CH.Native.Data;

namespace CH.Native.Adbc;

/// <summary>
/// An ADBC statement that executes a SQL string against ClickHouse and returns the result as a
/// lazily-streamed sequence of Arrow record batches (one per ClickHouse block).
/// </summary>
public sealed class ClickHouseAdbcStatement : AdbcStatement
{
    private readonly ClickHouseAdbcConnection _connection;

    internal ClickHouseAdbcStatement(ClickHouseAdbcConnection connection)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
    }

    /// <inheritdoc />
    public override QueryResult ExecuteQuery() =>
        ExecuteQueryAsync().AsTask().GetAwaiter().GetResult();

    /// <inheritdoc />
    public override async ValueTask<QueryResult> ExecuteQueryAsync()
    {
        var sql = RequireSql();
        var inner = _connection.Inner;

        // Open the block stream and pull the first block, which carries column metadata, so the
        // returned QueryResult exposes a concrete schema before any rows are read.
        var enumerator = inner.QueryBlocksAsync(sql).GetAsyncEnumerator();
        Schema schema;
        TypedBlock? pending = null;
        try
        {
            if (await enumerator.MoveNextAsync().ConfigureAwait(false))
            {
                var first = enumerator.Current;
                schema = ArrowSchemaMapper.ToSchema(first.ColumnNames, first.ColumnTypes);

                if (first.RowCount == 0)
                {
                    // Empty header block: schema captured, nothing to emit.
                    first.Dispose();
                }
                else
                {
                    pending = first;
                }
            }
            else
            {
                schema = new Schema(System.Array.Empty<Field>(), metadata: null);
            }
        }
        catch
        {
            await enumerator.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        var stream = new BlockArrowArrayStream(schema, inner, pending, enumerator);
        // Row count is not known up front for a streaming result.
        return new QueryResult(-1L, stream);
    }

    /// <inheritdoc />
    public override Schema ExecuteSchema()
    {
        using var result = ExecuteQuery().Stream;
        return result?.Schema ?? new Schema(System.Array.Empty<Field>(), metadata: null);
    }

    /// <inheritdoc />
    public override UpdateResult ExecuteUpdate() =>
        ExecuteUpdateAsync().GetAwaiter().GetResult();

    /// <inheritdoc />
    public override async Task<UpdateResult> ExecuteUpdateAsync()
    {
        var sql = RequireSql();
        var affected = await _connection.Inner
            .ExecuteNonQueryAsync(sql)
            .ConfigureAwait(false);
        return new UpdateResult(affected);
    }

    private string RequireSql()
    {
        if (string.IsNullOrWhiteSpace(SqlQuery))
            throw new InvalidOperationException("SqlQuery must be set before executing the statement.");
        return SqlQuery!;
    }

    /// <inheritdoc />
    public override void Dispose() => base.Dispose();
}
