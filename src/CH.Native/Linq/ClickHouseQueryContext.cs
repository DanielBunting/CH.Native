using CH.Native.Connection;

namespace CH.Native.Linq;

/// <summary>
/// Holds context information for executing ClickHouse LINQ queries.
/// </summary>
internal sealed class ClickHouseQueryContext
{
    /// <summary>
    /// The connection bound to this context, if any. <c>null</c> when the
    /// queryable was created from a <see cref="ClickHouseDataSource"/> — in
    /// that case <see cref="DataSource"/> is set and a connection is rented
    /// per execution via <see cref="AcquireConnectionAsync"/>.
    /// </summary>
    public ClickHouseConnection? Connection { get; }

    /// <summary>
    /// The data source bound to this context, if any. Mutually exclusive with
    /// <see cref="Connection"/> at construction time; populated only when the
    /// queryable was created via <see cref="ClickHouseDataSource"/>.Table&lt;T&gt;
    /// so reads rent a pooled connection per enumeration.
    /// </summary>
    public ClickHouseDataSource? DataSource { get; private set; }

    /// <summary>
    /// The resolved table name for the root entity.
    /// </summary>
    public string TableName { get; }

    /// <summary>
    /// Column names from the generated mapper (if available).
    /// Used for property-to-column name resolution.
    /// </summary>
    public string[]? ColumnNames { get; }

    /// <summary>
    /// The element type of the queryable.
    /// </summary>
    public Type ElementType { get; }

    /// <summary>
    /// Optional caller-supplied query ID to send on the wire. When null, the driver
    /// generates a GUID per execution. Set via <c>WithQueryId</c>.
    /// </summary>
    public string? QueryId { get; internal set; }

    public ClickHouseQueryContext(
        ClickHouseConnection? connection,
        string tableName,
        Type elementType,
        string[]? columnNames = null)
    {
        // Connection can be null for SQL generation tests - execution will fail but ToSql() works
        Connection = connection;
        DataSource = null;
        TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        ElementType = elementType ?? throw new ArgumentNullException(nameof(elementType));
        ColumnNames = columnNames;
    }

    /// <summary>
    /// Factory for the data-source-bound mode. Kept off the constructor surface
    /// so existing callers passing <c>(null, ...)</c> for SQL-generation-only
    /// contexts don't trip overload-resolution ambiguity against a sibling
    /// ctor taking <see cref="ClickHouseDataSource"/>.
    /// </summary>
    public static ClickHouseQueryContext FromDataSource(
        ClickHouseDataSource dataSource,
        string tableName,
        Type elementType,
        string[]? columnNames = null)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        var ctx = new ClickHouseQueryContext(connection: null, tableName, elementType, columnNames);
        ctx.DataSource = dataSource;
        return ctx;
    }

    /// <summary>
    /// Resolves a connection for query execution. When the context is bound to
    /// a connection, returns a non-owning lease that leaves disposal to the
    /// caller of the original handle. When bound to a data source, rents a
    /// pooled connection — disposing the returned lease returns it to the pool.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the context has neither a connection nor a data source — i.e.
    /// the queryable was created without either, which is only valid for
    /// <c>ToSql()</c>-style translation, not execution.
    /// </exception>
    public async ValueTask<LeasedConnection> AcquireConnectionAsync(CancellationToken cancellationToken = default)
    {
        if (Connection is not null)
            return new LeasedConnection(Connection, owned: false);

        var ds = DataSource ?? throw new InvalidOperationException(
            "Query context has neither a Connection nor a DataSource bound — the queryable cannot be executed. " +
            "This typically means the queryable was created for SQL-generation tests; use ToSql() instead of executing it.");

        var conn = await ds.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        return new LeasedConnection(conn, owned: true);
    }
}

/// <summary>
/// Lease wrapping a <see cref="ClickHouseConnection"/> resolved by
/// <see cref="ClickHouseQueryContext.AcquireConnectionAsync"/>. Disposing returns
/// rented connections to their pool; non-owning leases (the context already held
/// a connection) are no-op on dispose so the original owner's lifetime is unaffected.
/// </summary>
internal readonly struct LeasedConnection : IAsyncDisposable
{
    private readonly ClickHouseConnection _connection;
    private readonly bool _owned;

    public LeasedConnection(ClickHouseConnection connection, bool owned)
    {
        _connection = connection;
        _owned = owned;
    }

    public ClickHouseConnection Connection => _connection;

    public ValueTask DisposeAsync()
        => _owned ? _connection.DisposeAsync() : ValueTask.CompletedTask;
}
