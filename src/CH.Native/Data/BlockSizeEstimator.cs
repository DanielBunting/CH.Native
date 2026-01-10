namespace CH.Native.Data;

/// <summary>
/// Estimates block data sizes based on column types and row count.
/// Used to validate sufficient data is available before starting column parsing.
/// </summary>
public static class BlockSizeEstimator
{
    /// <summary>
    /// Estimates the minimum bytes needed for column data based on column types and row count.
    /// Returns -1 if the block contains variable-length types (String, Array, Map, etc.)
    /// where size cannot be predetermined.
    /// </summary>
    /// <param name="columnTypes">The ClickHouse type names for each column.</param>
    /// <param name="rowCount">The number of rows in the block.</param>
    /// <returns>The minimum bytes needed, or -1 if size cannot be estimated.</returns>
    public static long EstimateMinimumSize(string[] columnTypes, int rowCount)
    {
        if (rowCount == 0)
            return 0;

        long total = 0;
        foreach (var type in columnTypes)
        {
            var size = GetFixedColumnSize(type, rowCount);
            if (size < 0)
                return -1; // Variable-length column
            total += size;
        }
        return total;
    }

    /// <summary>
    /// Gets the fixed size of a column's data, or -1 if the type is variable-length.
    /// </summary>
    private static long GetFixedColumnSize(string type, int rowCount)
    {
        // Handle Nullable wrapper
        if (type.StartsWith("Nullable("))
        {
            var innerType = type[9..^1];
            var innerSize = GetFixedColumnSize(innerType, rowCount);
            // Nullable has a byte array prefix (null bitmap) plus the inner data
            return innerSize < 0 ? -1 : rowCount + innerSize;
        }

        // Fixed-size types
        return type switch
        {
            // 1-byte types
            "Int8" or "UInt8" or "Bool" => rowCount,

            // 2-byte types
            "Int16" or "UInt16" => rowCount * 2L,

            // 4-byte types
            "Int32" or "UInt32" or "Float32" or "Date" or "IPv4" => rowCount * 4L,

            // 8-byte types
            "Int64" or "UInt64" or "Float64" or "DateTime" or "Date32" => rowCount * 8L,

            // 16-byte types
            "Int128" or "UInt128" or "UUID" or "IPv6" => rowCount * 16L,

            // 32-byte types
            "Int256" or "UInt256" => rowCount * 32L,

            // Decimal types (size depends on precision)
            var t when t.StartsWith("Decimal32") => rowCount * 4L,
            var t when t.StartsWith("Decimal64") => rowCount * 8L,
            var t when t.StartsWith("Decimal128") => rowCount * 16L,
            var t when t.StartsWith("Decimal256") => rowCount * 32L,

            // DateTime64 is 8 bytes
            var t when t.StartsWith("DateTime64") => rowCount * 8L,

            // FixedString has a known size
            var t when t.StartsWith("FixedString(") => rowCount * (long)ParseFixedStringLength(t),

            // Enum types
            var t when t.StartsWith("Enum8") => rowCount,
            var t when t.StartsWith("Enum16") => rowCount * 2L,

            // Variable-length types - cannot estimate
            // String, Array, Map, Tuple, LowCardinality, Nested
            _ => -1
        };
    }

    /// <summary>
    /// Parses the length from a FixedString type name like "FixedString(32)".
    /// </summary>
    private static int ParseFixedStringLength(string typeName)
    {
        // Format: FixedString(N)
        var startIndex = "FixedString(".Length;
        var endIndex = typeName.IndexOf(')', startIndex);
        if (endIndex < 0)
            return 0;

        var lengthStr = typeName[startIndex..endIndex];
        return int.TryParse(lengthStr, out var length) ? length : 0;
    }
}
