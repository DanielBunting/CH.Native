using CH.Native.Protocol;

namespace CH.Native.BulkInsert;

/// <summary>
/// Interface for extracting and writing column data directly from source objects.
/// Eliminates intermediate arrays and boxing for value types.
/// </summary>
/// <typeparam name="TRow">The source row type.</typeparam>
public interface IColumnExtractor<TRow>
{
    /// <summary>
    /// Gets the ClickHouse column name.
    /// </summary>
    string ColumnName { get; }

    /// <summary>
    /// Gets the ClickHouse type name.
    /// </summary>
    string TypeName { get; }

    /// <summary>
    /// Extracts and writes column data for all rows directly to the protocol writer.
    /// </summary>
    /// <param name="writer">The protocol writer.</param>
    /// <param name="rows">The source rows.</param>
    /// <param name="rowCount">Number of rows to write.</param>
    void ExtractAndWrite(ref ProtocolWriter writer, IReadOnlyList<TRow> rows, int rowCount);
}
