using CH.Native.Protocol;

namespace CH.Native.Data;

/// <summary>
/// Interface for skipping column data in the ClickHouse protocol without allocation.
/// Used for the scan pass to validate block completeness before parsing.
/// </summary>
public interface IColumnSkipper
{
    /// <summary>
    /// Gets the ClickHouse type name this skipper handles (e.g., "Int32", "String").
    /// </summary>
    string TypeName { get; }

    /// <summary>
    /// Tries to skip column data without allocating arrays or strings.
    /// </summary>
    /// <param name="reader">The protocol reader.</param>
    /// <param name="rowCount">The number of rows to skip.</param>
    /// <returns>True if all data was available and skipped; false if not enough data.</returns>
    bool TrySkipColumn(ref ProtocolReader reader, int rowCount);
}
