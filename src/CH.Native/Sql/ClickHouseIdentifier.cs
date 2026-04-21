namespace CH.Native.Sql;

/// <summary>
/// Helpers for emitting ClickHouse-safe SQL fragments.
/// </summary>
public static class ClickHouseIdentifier
{
    /// <summary>
    /// Quotes an identifier for ClickHouse SQL using backticks, doubling any embedded
    /// backtick so the resulting token survives the server's parser and can never be
    /// interpreted as a SQL-injection vector. Matches the quoting rule used by
    /// ClickHouse's own parser (src/Parsers/ExpressionElementParsers.cpp).
    /// </summary>
    /// <param name="identifier">Raw identifier, e.g. a role, table, or column name.</param>
    /// <returns>Backtick-quoted identifier safe for interpolation into SQL.</returns>
    public static string Quote(string identifier)
    {
        ArgumentNullException.ThrowIfNull(identifier);
        return "`" + identifier.Replace("`", "``") + "`";
    }
}
