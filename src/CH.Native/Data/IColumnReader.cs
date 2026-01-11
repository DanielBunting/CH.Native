using CH.Native.Protocol;

namespace CH.Native.Data;

/// <summary>
/// Interface for reading column data from the ClickHouse protocol.
/// Uses typed storage to avoid boxing overhead.
/// </summary>
public interface IColumnReader
{
    /// <summary>
    /// Gets the ClickHouse type name this reader handles (e.g., "Int32", "String").
    /// </summary>
    string TypeName { get; }

    /// <summary>
    /// Gets the CLR type that this reader produces.
    /// </summary>
    Type ClrType { get; }

    /// <summary>
    /// Reads column data into typed storage without boxing value types.
    /// </summary>
    /// <param name="reader">The protocol reader.</param>
    /// <param name="rowCount">The number of rows to read.</param>
    /// <returns>A typed column containing the values.</returns>
    ITypedColumn ReadTypedColumn(ref ProtocolReader reader, int rowCount);
}

/// <summary>
/// Generic interface for strongly-typed column reading.
/// </summary>
/// <typeparam name="T">The CLR type of the column values.</typeparam>
public interface IColumnReader<T> : IColumnReader
{
    /// <summary>
    /// Reads column data into typed storage without boxing.
    /// </summary>
    /// <param name="reader">The protocol reader.</param>
    /// <param name="rowCount">The number of rows to read.</param>
    /// <returns>A typed column containing the values.</returns>
    new TypedColumn<T> ReadTypedColumn(ref ProtocolReader reader, int rowCount);

    /// <summary>
    /// Reads a single value from the column.
    /// </summary>
    /// <param name="reader">The protocol reader.</param>
    /// <returns>The typed value read.</returns>
    T ReadValue(ref ProtocolReader reader);
}
