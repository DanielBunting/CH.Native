namespace CH.Native.Data;

/// <summary>
/// Internal shape decision used by <see cref="ColumnReaderFactory"/> to choose
/// between the dictionary and entries reader for a given <c>Map(K, V)</c> column.
/// </summary>
internal enum MapShape
{
    /// <summary>
    /// Use the connection-level default (see <see cref="MapShapeHint.ConnectionDefault"/>).
    /// </summary>
    Default,

    /// <summary>
    /// Force <see cref="System.Collections.Generic.Dictionary{TKey, TValue}"/> materialisation.
    /// </summary>
    Dictionary,

    /// <summary>
    /// Force <see cref="ClickHouseMap{TKey, TValue}"/> materialisation.
    /// </summary>
    Entries,
}
