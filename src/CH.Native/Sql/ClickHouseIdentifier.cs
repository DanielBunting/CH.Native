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

    /// <summary>
    /// Quotes a possibly-qualified table reference. A bare name (no dot) is quoted
    /// like <see cref="Quote"/>; a <c>database.table</c> form is split on the single
    /// dot and each segment is quoted independently so the rendered SQL addresses
    /// the right database (<c>`db`.`table`</c>, not <c>`db.table`</c>).
    /// </summary>
    /// <param name="identifier">Bare table name or <c>database.table</c>.</param>
    /// <returns>Backtick-quoted identifier safe for interpolation into SQL.</returns>
    /// <exception cref="ArgumentException">
    /// Thrown for empty input, more than one dot (ambiguous parse), or an empty
    /// segment on either side of the dot. Tables whose name legitimately contains
    /// a dot must be addressed via the explicit <c>(database, table)</c> overloads
    /// on <c>BulkInserter</c> / <c>ClickHouseConnection.CreateBulkInserter</c>.
    /// </exception>
    public static string QuoteQualifiedName(string identifier)
    {
        ArgumentNullException.ThrowIfNull(identifier);
        if (identifier.Length == 0)
            throw new ArgumentException("Identifier must be non-empty.", nameof(identifier));

        var dot = identifier.IndexOf('.');
        if (dot < 0)
            return Quote(identifier);

        if (identifier.IndexOf('.', dot + 1) >= 0)
            throw new ArgumentException(
                $"Qualified table name '{identifier}' has more than one dot. Use the " +
                "(database, table) overload to insert into a table whose name contains a dot.",
                nameof(identifier));

        if (dot == 0 || dot == identifier.Length - 1)
            throw new ArgumentException(
                $"Qualified table name '{identifier}' has an empty database or table segment.",
                nameof(identifier));

        var database = identifier.Substring(0, dot);
        var table = identifier.Substring(dot + 1);
        return Quote(database) + "." + Quote(table);
    }

    /// <summary>
    /// Splits a possibly-qualified table reference into a (database, table) tuple,
    /// applying the same single-dot rule as <see cref="QuoteQualifiedName"/>. Returns
    /// <paramref name="defaultDatabase"/> for the database segment when the input is
    /// unqualified.
    /// </summary>
    internal static (string Database, string Table) SplitQualifiedName(string identifier, string defaultDatabase)
    {
        ArgumentNullException.ThrowIfNull(identifier);
        if (identifier.Length == 0)
            throw new ArgumentException("Identifier must be non-empty.", nameof(identifier));

        var dot = identifier.IndexOf('.');
        if (dot < 0)
            return (defaultDatabase, identifier);

        if (identifier.IndexOf('.', dot + 1) >= 0)
            throw new ArgumentException(
                $"Qualified table name '{identifier}' has more than one dot. Use the " +
                "(database, table) overload to insert into a table whose name contains a dot.",
                nameof(identifier));

        if (dot == 0 || dot == identifier.Length - 1)
            throw new ArgumentException(
                $"Qualified table name '{identifier}' has an empty database or table segment.",
                nameof(identifier));

        return (identifier.Substring(0, dot), identifier.Substring(dot + 1));
    }
}
