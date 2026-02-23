namespace CH.Native.Data;

/// <summary>
/// Controls how string column data is materialized during block parsing.
/// </summary>
public enum StringMaterialization
{
    /// <summary>
    /// Decode all strings to System.String objects during block parsing (default).
    /// Best for typed queries (QueryTypedAsync&lt;T&gt;) or when all values will be accessed.
    /// </summary>
    Eager = 0,

    /// <summary>
    /// Store raw UTF-8 bytes and decode to System.String on demand via GetValue().
    /// Reduces memory usage when streaming with ClickHouseDataReader, especially
    /// when not all string columns are accessed.
    /// </summary>
    Lazy = 1
}
