using Apache.Arrow.Adbc;
using CH.Native.Connection;

namespace CH.Native.Adbc;

/// <summary>
/// An ADBC database for ClickHouse. Holds the resolved connection settings and a pooled
/// <see cref="ClickHouseDataSource"/>; each <see cref="Connect"/> rents a connection from the pool.
/// </summary>
public sealed class ClickHouseAdbcDatabase : AdbcDatabase
{
    private readonly ClickHouseDataSource _dataSource;

    internal ClickHouseAdbcDatabase(ClickHouseConnectionSettings settings)
    {
        ArgumentNullException.ThrowIfNull(settings);
        _dataSource = new ClickHouseDataSource(settings);
    }

    /// <inheritdoc />
    public override AdbcConnection Connect(IReadOnlyDictionary<string, string>? options)
    {
        // ADBC's Connect is synchronous; rent (and open) a pooled connection here. The native
        // protocol is single-query-at-a-time per connection, so one ADBC connection maps to one
        // ClickHouseConnection that executes statements serially.
        var connection = _dataSource
            .OpenConnectionAsync()
            .AsTask()
            .GetAwaiter()
            .GetResult();

        return new ClickHouseAdbcConnection(connection);
    }

    /// <inheritdoc />
    public override void Dispose()
    {
        _dataSource.Dispose();
        base.Dispose();
    }
}
