using System.Collections;
using System.Globalization;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using CH.Native.Commands;

namespace CH.Native.Linq;

/// <summary>
/// Renders a parameterized SQL string with inline literals — the inverse of the
/// LINQ visitor's parameter-emission step. Used by <c>ClickHouseQueryable.ToSql()</c>
/// so callers can paste the returned SQL into <c>clickhouse-client</c> or another
/// raw-query path without needing to bind parameters separately.
/// </summary>
/// <remarks>
/// <para>
/// This is a debug/diagnostic formatter, not a query-execution path. The
/// production execution path uses <c>{name:Type}</c> placeholders with
/// server-side parameter binding (which is also the SQL-injection-safe
/// path); inlining here is purely for human-readable output.
/// </para>
/// <para>
/// String literals are escaped using ClickHouse single-quote rules
/// (<c>'</c> → <c>\'</c>, <c>\</c> → <c>\\</c>) so the resulting SQL is
/// still injection-safe — but callers should not feed the output of
/// <c>ToSql()</c> into a privileged execution path; use the parameterized
/// form for that.
/// </para>
/// </remarks>
internal static partial class SqlLiteralFormatter
{
    // Matches `{name:Type}` placeholders. Type may include nested parentheses
    // (e.g. `Decimal(18, 2)`, `Array(Nullable(String))`), so we use a
    // balanced-parens-friendly pattern: name = identifier, then optional
    // colon-prefixed type that can contain anything except a closing brace.
    [GeneratedRegex(@"\{(?<name>[a-zA-Z_][a-zA-Z0-9_]*)(?::(?<type>[^}]+))?\}", RegexOptions.Compiled)]
    private static partial Regex PlaceholderPattern();

    /// <summary>
    /// Substitutes every <c>{name:Type}</c> placeholder in <paramref name="sql"/>
    /// with the SQL-literal form of the parameter value. Placeholders without a
    /// matching parameter are left intact (which produces a clearly-broken SQL
    /// — better than a silently-wrong one).
    /// </summary>
    public static string RenderInline(string sql, ClickHouseParameterCollection parameters)
    {
        if (parameters.Count == 0)
            return sql;

        return PlaceholderPattern().Replace(sql, match =>
        {
            var name = match.Groups["name"].Value;
            if (!parameters.Contains(name))
                return match.Value; // leave broken placeholder so the failure is visible
            return FormatLiteral(parameters[name].Value);
        });
    }

    private static string FormatLiteral(object? value)
    {
        if (value is null) return "NULL";

        return value switch
        {
            string s => "'" + EscapeStringLiteral(s) + "'",
            bool b => b ? "1" : "0",

            sbyte sb => sb.ToString(CultureInfo.InvariantCulture),
            short sh => sh.ToString(CultureInfo.InvariantCulture),
            int i => i.ToString(CultureInfo.InvariantCulture),
            long l => l.ToString(CultureInfo.InvariantCulture),
            Int128 i128 => i128.ToString(CultureInfo.InvariantCulture),
            byte by => by.ToString(CultureInfo.InvariantCulture),
            ushort us => us.ToString(CultureInfo.InvariantCulture),
            uint ui => ui.ToString(CultureInfo.InvariantCulture),
            ulong ul => ul.ToString(CultureInfo.InvariantCulture),
            UInt128 ui128 => ui128.ToString(CultureInfo.InvariantCulture),

            float f => FormatFloat(f),
            double d => FormatDouble(d),
            decimal dec => dec.ToString(CultureInfo.InvariantCulture),
            CH.Native.Numerics.ClickHouseDecimal chDec => chDec.ToString(format: null, CultureInfo.InvariantCulture),

            DateTime dt => "'" + dt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) + "'",
            DateTimeOffset dto => "'" + dto.UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) + "'",
            DateOnly date => "'" + date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) + "'",

            Guid g => "'" + g.ToString("D") + "'",
            IPAddress ip => "'" + ip + "'",

            // Arrays / collections — recursive formatting.
            IEnumerable enumerable when value is not string => FormatArray(enumerable),

            // Fall-through: best-effort culture-invariant ToString.
            _ => "'" + EscapeStringLiteral(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty) + "'",
        };
    }

    private static string FormatArray(IEnumerable enumerable)
    {
        var sb = new StringBuilder();
        sb.Append('[');
        var first = true;
        foreach (var item in enumerable)
        {
            if (!first) sb.Append(", ");
            sb.Append(FormatLiteral(item));
            first = false;
        }
        sb.Append(']');
        return sb.ToString();
    }

    private static string FormatFloat(float f)
    {
        if (float.IsNaN(f)) return "nan";
        if (float.IsPositiveInfinity(f)) return "inf";
        if (float.IsNegativeInfinity(f)) return "-inf";
        return f.ToString("R", CultureInfo.InvariantCulture);
    }

    private static string FormatDouble(double d)
    {
        if (double.IsNaN(d)) return "nan";
        if (double.IsPositiveInfinity(d)) return "inf";
        if (double.IsNegativeInfinity(d)) return "-inf";
        return d.ToString("R", CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Escapes a string for safe inclusion as a single-quoted SQL literal.
    /// Mirrors the ClickHouse parser's expectations:
    /// <c>'</c> → <c>\'</c>, <c>\</c> → <c>\\</c>, plus newline / tab control chars.
    /// </summary>
    private static string EscapeStringLiteral(string s)
    {
        var sb = new StringBuilder(s.Length);
        foreach (var c in s)
        {
            switch (c)
            {
                case '\\': sb.Append(@"\\"); break;
                case '\'': sb.Append(@"\'"); break;
                case '\n': sb.Append(@"\n"); break;
                case '\r': sb.Append(@"\r"); break;
                case '\t': sb.Append(@"\t"); break;
                case '\0': sb.Append(@"\0"); break;
                default: sb.Append(c); break;
            }
        }
        return sb.ToString();
    }
}
