using CH.Native.Data;

namespace CH.Native.Mapping;

/// <summary>
/// Interface for mapping rows from typed columns to POCOs.
/// This is the fast path that avoids boxing.
/// </summary>
/// <typeparam name="T">The POCO type to map to.</typeparam>
public interface ITypedRowMapper<T>
{
    /// <summary>
    /// Maps a single row from the typed columns to a POCO instance.
    /// </summary>
    /// <param name="columns">The typed columns.</param>
    /// <param name="rowIndex">The row index to map.</param>
    /// <returns>A new instance of T with values from the row.</returns>
    T MapRow(ITypedColumn[] columns, int rowIndex);
}
