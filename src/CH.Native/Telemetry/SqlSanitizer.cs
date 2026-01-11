using System.Text.RegularExpressions;

namespace CH.Native.Telemetry;

/// <summary>
/// Sanitizes SQL statements by replacing literal values with placeholders.
/// </summary>
public static partial class SqlSanitizer
{
    /// <summary>
    /// Pattern for single-quoted strings (handles escaped quotes like 'it''s').
    /// Matches: 'hello', 'it''s', 'secret value'
    /// </summary>
    [GeneratedRegex(@"'(?:[^']|'')*'", RegexOptions.Compiled)]
    private static partial Regex StringLiteralPattern();

    /// <summary>
    /// Pattern for numeric literals (integers, decimals, scientific notation).
    /// Matches: 123, 3.14, -42, +1.5, 1e10, 2.5E-3
    /// Uses word boundaries to avoid matching parts of identifiers.
    /// </summary>
    [GeneratedRegex(@"(?<![a-zA-Z_\.])[+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?(?![a-zA-Z_\d])", RegexOptions.Compiled)]
    private static partial Regex NumericLiteralPattern();

    /// <summary>
    /// Sanitizes a SQL statement by replacing literal values with placeholders.
    /// </summary>
    /// <param name="sql">The SQL statement to sanitize.</param>
    /// <returns>The sanitized SQL statement with literals replaced by '?'.</returns>
    public static string Sanitize(string sql)
    {
        if (string.IsNullOrEmpty(sql))
            return sql;

        // Replace string literals first (they might contain numbers)
        var result = StringLiteralPattern().Replace(sql, "?");

        // Then replace numeric literals
        result = NumericLiteralPattern().Replace(result, "?");

        return result;
    }
}
