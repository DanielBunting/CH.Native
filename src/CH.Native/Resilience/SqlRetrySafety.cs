namespace CH.Native.Resilience;

/// <summary>
/// Classifies SQL statements as retry-safe (idempotent reads) or retry-unsafe
/// (writes / DDL / session-modifying). The resilience layer consults this so a
/// transient failure in <c>INSERT</c>, <c>ALTER</c>, <c>OPTIMIZE</c>, etc.
/// doesn't trigger an automatic retry that would duplicate rows or repeat side
/// effects against a non-idempotent statement.
/// </summary>
/// <remarks>
/// <para>
/// The check is intentionally allow-list based. Anything other than a clean
/// read verb (<c>SELECT</c>, <c>SHOW</c>, <c>DESCRIBE</c>/<c>DESC</c>,
/// <c>EXPLAIN</c>, <c>EXISTS</c>, <c>CHECK</c>) is classified as unsafe — new
/// or unfamiliar verbs default to "do not retry", which is the safe failure
/// mode for a data-integrity question.
/// </para>
/// <para>
/// The classifier strips leading whitespace and SQL comments (<c>--</c> line
/// comments and <c>/* … */</c> block comments) before inspecting the first
/// identifier. <c>WITH</c>-prefixed statements (CTEs) are scanned for any
/// write verb anywhere in the (comment-stripped) text — if none are present
/// the statement is treated as a read CTE and is retry-safe; if any write
/// verb appears the whole statement is unsafe, regardless of which side of
/// the CTE the write lives on.
/// </para>
/// </remarks>
internal static class SqlRetrySafety
{
    // Verbs that flip a statement's retry safety to "no". Any one of these
    // appearing as a standalone keyword anywhere in the comment-stripped SQL
    // (especially after a leading WITH …) makes the whole statement unsafe.
    private static readonly string[] WriteVerbs =
    {
        "INSERT", "UPDATE", "DELETE", "ALTER", "CREATE", "DROP",
        "RENAME", "TRUNCATE", "ATTACH", "DETACH", "OPTIMIZE",
        "GRANT", "REVOKE", "SET", "USE", "SYSTEM", "KILL",
        "EXCHANGE", "MOVE", "REPLACE",
    };

    /// <summary>
    /// Returns <c>true</c> when <paramref name="sql"/>'s leading verb is a
    /// known read-only ClickHouse statement and therefore safe to retry on
    /// transient failure.
    /// </summary>
    public static bool IsRetrySafe(string? sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return false;

        var span = sql.AsSpan();
        var i = SkipWhitespaceAndComments(span, 0);
        if (i >= span.Length)
            return false;

        // Read the first identifier (alpha-only is enough — ClickHouse verbs
        // are ASCII letters).
        var start = i;
        while (i < span.Length && IsAsciiLetter(span[i]))
            i++;
        if (i == start)
            return false;

        var verb = span[start..i];

        // WITH-prefixed CTEs: scan the rest of the statement for any write
        // verb. Read-only CTEs (`WITH cte AS (SELECT …) SELECT …`) are
        // retry-safe; write CTEs (`WITH cte AS (…) INSERT INTO …`) are not.
        if (verb.Equals("WITH", StringComparison.OrdinalIgnoreCase))
            return !ContainsWriteVerb(span[i..]);

        return verb.Equals("SELECT", StringComparison.OrdinalIgnoreCase)
            || verb.Equals("SHOW", StringComparison.OrdinalIgnoreCase)
            || verb.Equals("DESCRIBE", StringComparison.OrdinalIgnoreCase)
            || verb.Equals("DESC", StringComparison.OrdinalIgnoreCase)
            || verb.Equals("EXPLAIN", StringComparison.OrdinalIgnoreCase)
            || verb.Equals("EXISTS", StringComparison.OrdinalIgnoreCase)
            || verb.Equals("CHECK", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Walks <paramref name="span"/> identifier-by-identifier, skipping
    /// comments and string literals, and returns true if any token matches
    /// a known write verb (case-insensitive). Any literal or comment content
    /// that mentions a write verb is correctly ignored — the scan only
    /// inspects code, not data.
    /// </summary>
    private static bool ContainsWriteVerb(ReadOnlySpan<char> span)
    {
        var i = 0;
        while (i < span.Length)
        {
            // Comments and whitespace.
            var skipped = SkipWhitespaceAndComments(span, i);
            if (skipped > i) { i = skipped; continue; }

            // String literals — skip them. ClickHouse uses single quotes for
            // strings and backticks for identifiers; both can contain anything.
            if (span[i] == '\'' || span[i] == '`' || span[i] == '"')
            {
                var quote = span[i++];
                while (i < span.Length && span[i] != quote)
                {
                    if (span[i] == '\\' && i + 1 < span.Length) i += 2;
                    else i++;
                }
                if (i < span.Length) i++;
                continue;
            }

            // Identifier-like sequence — extract the alpha run.
            if (IsAsciiLetter(span[i]))
            {
                var start = i;
                while (i < span.Length && (IsAsciiLetter(span[i]) || (span[i] >= '0' && span[i] <= '9') || span[i] == '_'))
                    i++;
                var token = span[start..i];
                foreach (var verb in WriteVerbs)
                {
                    if (token.Equals(verb.AsSpan(), StringComparison.OrdinalIgnoreCase))
                        return true;
                }
                continue;
            }

            i++;
        }
        return false;
    }

    private static int SkipWhitespaceAndComments(ReadOnlySpan<char> s, int i)
    {
        while (i < s.Length)
        {
            if (char.IsWhiteSpace(s[i]))
            {
                i++;
            }
            else if (i + 1 < s.Length && s[i] == '-' && s[i + 1] == '-')
            {
                // Line comment — skip to newline.
                i += 2;
                while (i < s.Length && s[i] != '\n') i++;
            }
            else if (i + 1 < s.Length && s[i] == '/' && s[i + 1] == '*')
            {
                // Block comment — skip to closing */.
                i += 2;
                while (i + 1 < s.Length && !(s[i] == '*' && s[i + 1] == '/')) i++;
                if (i + 1 < s.Length) i += 2; // consume the closing */
                else i = s.Length;
            }
            else
            {
                break;
            }
        }
        return i;
    }

    private static bool IsAsciiLetter(char c) =>
        (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}
