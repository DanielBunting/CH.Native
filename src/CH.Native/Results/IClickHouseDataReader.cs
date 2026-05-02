namespace CH.Native.Results;

/// <summary>
/// Abstraction over <see cref="ClickHouseDataReader"/> exposing the surface
/// consumed by <see cref="Ado.ClickHouseDbDataReader"/>. Existing solely so
/// the ADO.NET wrapper can be unit-tested with a fake transport — production
/// code should construct <see cref="ClickHouseDataReader"/> directly via
/// <see cref="Connection.ClickHouseConnection.ExecuteReaderAsync(string, System.Threading.CancellationToken)"/>.
/// </summary>
public interface IClickHouseDataReader : IAsyncDisposable
{
    /// <summary>
    /// The query ID sent on the wire. Matches the value in ClickHouse's
    /// <c>system.query_log</c>. Null if the underlying reader was constructed
    /// without one (legacy internal test paths only).
    /// </summary>
    string? QueryId { get; }

    /// <summary>The number of columns in the result set.</summary>
    int FieldCount { get; }

    /// <summary>True if the result set has at least one row.</summary>
    bool HasRows { get; }

    /// <summary>Advances to the next row. Returns false when no more rows are available.</summary>
    ValueTask<bool> ReadAsync(CancellationToken cancellationToken = default);

    /// <summary>Gets the value at the specified column ordinal in the current row.</summary>
    object? GetValue(int ordinal);

    /// <summary>Gets a typed value at the specified column ordinal in the current row.</summary>
    T GetFieldValue<T>(int ordinal);

    /// <summary>Looks up the column ordinal for the given name (case-insensitive).</summary>
    int GetOrdinal(string name);

    /// <summary>Gets the column name at the specified ordinal.</summary>
    string GetName(int ordinal);

    /// <summary>Gets the ClickHouse type name (e.g. "Int32", "String") at the specified ordinal.</summary>
    string GetTypeName(int ordinal);

    /// <summary>Gets the CLR type the column will materialise as.</summary>
    Type GetFieldType(int ordinal);

    /// <summary>True if the value at the specified column ordinal in the current row is null.</summary>
    bool IsDBNull(int ordinal);
}
