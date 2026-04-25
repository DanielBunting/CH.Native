using System.Collections;
using System.Globalization;
using System.Text;

namespace CH.Native.Parameters;

/// <summary>
/// Serializes parameter values to ClickHouse wire format for settings.
/// Critical for SQL injection prevention - all string escaping is handled here.
/// </summary>
public static class ParameterSerializer
{
    /// <summary>
    /// Serializes a parameter value for the native protocol parameters section.
    /// Uses ClickHouse Field dump format - all values as quoted strings.
    /// The type information is specified in the SQL placeholder (e.g., {param:Int32}).
    /// </summary>
    /// <param name="value">The value to serialize.</param>
    /// <param name="clickHouseType">The target ClickHouse type.</param>
    /// <returns>The serialized value string in Field dump format.</returns>
    public static string Serialize(object? value, string clickHouseType)
    {
        if (value is null)
            return SerializeNull(clickHouseType);

        // All values are serialized as quoted strings in field dump format.
        // The SQL placeholder contains the type information (e.g., {param:Int32}).
        // The wire format is consumed by ClickHouse's two-pass parameter decode, so
        // strings are wrapped with EscapeStringForParameter (double-escape for
        // backslash/control chars) instead of the single-pass EscapeString used by
        // the LINQ provider when inlining literals into generated SQL text.
        return value switch
        {
            // Strings - quoted with single quotes and escaped (two-pass-safe)
            string s => EscapeStringForParameter(s),

            // Booleans - as string "1" or "0"
            bool b => EscapeStringForParameter(b ? "1" : "0"),

            // Integers (signed) - as quoted string
            sbyte sb => EscapeStringForParameter(sb.ToString(CultureInfo.InvariantCulture)),
            short sh => EscapeStringForParameter(sh.ToString(CultureInfo.InvariantCulture)),
            int i => EscapeStringForParameter(i.ToString(CultureInfo.InvariantCulture)),
            long l => EscapeStringForParameter(l.ToString(CultureInfo.InvariantCulture)),
            Int128 i128 => EscapeStringForParameter(i128.ToString(CultureInfo.InvariantCulture)),

            // Integers (unsigned) - as quoted string
            byte by => EscapeStringForParameter(by.ToString(CultureInfo.InvariantCulture)),
            ushort us => EscapeStringForParameter(us.ToString(CultureInfo.InvariantCulture)),
            uint ui => EscapeStringForParameter(ui.ToString(CultureInfo.InvariantCulture)),
            ulong ul => EscapeStringForParameter(ul.ToString(CultureInfo.InvariantCulture)),
            UInt128 ui128 => EscapeStringForParameter(ui128.ToString(CultureInfo.InvariantCulture)),

            // Floating point - as quoted string
            float f => EscapeStringForParameter(SerializeFloatValue(f)),
            double d => EscapeStringForParameter(SerializeDoubleValue(d)),
            decimal dec => EscapeStringForParameter(dec.ToString(CultureInfo.InvariantCulture)),

            // Date/Time - quoted string format
            DateTime dt => EscapeStringForParameter(SerializeDateTimeRaw(dt, clickHouseType)),
            DateTimeOffset dto => EscapeStringForParameter(SerializeDateTimeOffsetRaw(dto)),
            DateOnly date => EscapeStringForParameter(date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)),

            // Guid - quoted UUID format
            Guid g => EscapeStringForParameter(g.ToString("D")),

            // IP addresses - quoted format
            System.Net.IPAddress ip => EscapeStringForParameter(ip.ToString()),

            // Arrays/Collections (but not string which is IEnumerable<char>)
            IEnumerable enumerable when value is not string => SerializeArray(enumerable, clickHouseType),

            _ => throw new NotSupportedException(
                $"Cannot serialize value of type '{value.GetType().FullName}' to ClickHouse.")
        };
    }

    /// <summary>
    /// Escapes a string value for safe use in ClickHouse.
    /// This is CRITICAL for SQL injection prevention.
    /// </summary>
    /// <param name="value">The string value to escape.</param>
    /// <returns>The escaped string with surrounding quotes.</returns>
    public static string EscapeString(string value)
    {
        // SQL-literal form: surrounded by single quotes with standard SQL-style
        // escapes. Used by the LINQ provider to inline string literals into the
        // generated SQL text, where ClickHouse's SQL parser does a SINGLE decode
        // pass. For the query-parameter wire protocol, use
        // <see cref="EscapeStringForParameter"/> instead — that path requires a
        // second layer of escaping to survive the server's two-pass decode.

        var sb = new StringBuilder(value.Length + 10);
        sb.Append('\'');
        foreach (var c in value)
        {
            switch (c)
            {
                case '\'': sb.Append("\\'"); break;
                case '\\': sb.Append("\\\\"); break;
                case '\t': sb.Append("\\t"); break;
                case '\n': sb.Append("\\n"); break;
                case '\r': sb.Append("\\r"); break;
                case '\0': sb.Append("\\0"); break;
                case '\b': sb.Append("\\b"); break;
                case '\f': sb.Append("\\f"); break;
                default: sb.Append(c); break;
            }
        }
        sb.Append('\'');
        return sb.ToString();
    }

    /// <summary>
    /// Escapes a string value for the ClickHouse query-parameter wire protocol.
    /// The server decodes parameter values in two passes (readQuotedString then
    /// deserializeTextEscaped), so control characters and backslashes must be
    /// DOUBLE-escaped on the wire to survive both passes intact. Single quotes
    /// are single-escaped because pass (b) treats them as literal.
    /// </summary>
    /// <param name="value">The string value to escape.</param>
    /// <returns>The quoted, double-escaped representation for the parameter wire.</returns>
    public static string EscapeStringForParameter(string value)
    {
        var sb = new StringBuilder(value.Length + 10);
        sb.Append('\'');
        foreach (var c in value)
        {
            switch (c)
            {
                case '\'': sb.Append("\\'"); break;
                case '\\': sb.Append("\\\\\\\\"); break;
                case '\t': sb.Append("\\\\t"); break;
                case '\n': sb.Append("\\\\n"); break;
                case '\r': sb.Append("\\\\r"); break;
                case '\0': sb.Append("\\\\0"); break;
                case '\b': sb.Append("\\\\b"); break;
                case '\f': sb.Append("\\\\f"); break;
                default: sb.Append(c); break;
            }
        }
        sb.Append('\'');
        return sb.ToString();
    }

    private static string SerializeNull(string clickHouseType)
    {
        // For Nullable types, return NULL
        if (clickHouseType.StartsWith("Nullable(", StringComparison.Ordinal))
            return "NULL";

        throw new InvalidOperationException(
            $"Cannot pass NULL value for non-nullable type '{clickHouseType}'. " +
            "Use Nullable(T) type or provide a non-null value.");
    }

    private static string SerializeFloatValue(float value)
    {
        if (float.IsNaN(value)) return "nan";
        if (float.IsPositiveInfinity(value)) return "inf";
        if (float.IsNegativeInfinity(value)) return "-inf";
        return value.ToString("G9", CultureInfo.InvariantCulture);
    }

    private static string SerializeDoubleValue(double value)
    {
        if (double.IsNaN(value)) return "nan";
        if (double.IsPositiveInfinity(value)) return "inf";
        if (double.IsNegativeInfinity(value)) return "-inf";
        return value.ToString("G17", CultureInfo.InvariantCulture);
    }

    private static string SerializeDateTimeRaw(DateTime dt, string clickHouseType)
    {
        // DateTime64 gets microsecond precision
        if (clickHouseType.StartsWith("DateTime64", StringComparison.Ordinal))
        {
            return dt.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture);
        }
        // Regular DateTime gets second precision
        return dt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
    }

    private static string SerializeDateTimeOffsetRaw(DateTimeOffset dto)
    {
        // Convert to UTC for storage with microsecond precision
        return dto.UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture);
    }

    private static string SerializeArray(IEnumerable enumerable, string clickHouseType)
    {
        // Extract element type from Array(X)
        var elementType = ExtractArrayElementType(clickHouseType);

        var elements = new List<string>();
        foreach (var item in enumerable)
        {
            // Use raw value serialization for array elements
            elements.Add(SerializeRawValue(item, elementType));
        }

        // Return array as quoted string — double-escape for the parameter-wire
        // two-pass decode (matches the String path in Serialize).
        return EscapeStringForParameter($"[{string.Join(", ", elements)}]");
    }

    /// <summary>
    /// Serializes a value without quoting for use within array literals.
    /// </summary>
    private static string SerializeRawValue(object? value, string clickHouseType)
    {
        if (value is null)
            return "NULL";

        return value switch
        {
            // Strings within arrays need escaping but format differs
            string s => EscapeStringForArray(s),

            // Booleans
            bool b => b ? "1" : "0",

            // Integers and other numeric types - raw string
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
            float f => SerializeFloatValue(f),
            double d => SerializeDoubleValue(d),
            decimal dec => dec.ToString(CultureInfo.InvariantCulture),

            // Date/Time
            DateTime dt => $"'{SerializeDateTimeRaw(dt, clickHouseType)}'",
            DateTimeOffset dto => $"'{SerializeDateTimeOffsetRaw(dto)}'",
            DateOnly date => $"'{date:yyyy-MM-dd}'",

            // Guid
            Guid g => $"'{g:D}'",

            _ => throw new NotSupportedException(
                $"Cannot serialize array element of type '{value.GetType().FullName}' to ClickHouse.")
        };
    }

    /// <summary>
    /// Escapes a string for use within an array literal.
    /// </summary>
    private static string EscapeStringForArray(string value)
    {
        // Array elements are embedded inside a [..]-delimited literal which is itself
        // then wrapped and escaped by EscapeString, so each element only needs the
        // pass-(a) escape set (single-quoted with SQL-style `\'` / `\\` escapes). The
        // outer EscapeString handles the second pass.
        var sb = new StringBuilder(value.Length + 10);
        sb.Append('\'');
        foreach (var c in value)
        {
            switch (c)
            {
                case '\'': sb.Append("\\'"); break;
                case '\\': sb.Append("\\\\"); break;
                case '\t': sb.Append("\\t"); break;
                case '\n': sb.Append("\\n"); break;
                case '\r': sb.Append("\\r"); break;
                case '\0': sb.Append("\\0"); break;
                case '\b': sb.Append("\\b"); break;
                case '\f': sb.Append("\\f"); break;
                default: sb.Append(c); break;
            }
        }
        sb.Append('\'');
        return sb.ToString();
    }

    private static string ExtractArrayElementType(string arrayType)
    {
        // Array(Int32) -> Int32
        // Array(Array(Int32)) -> Array(Int32)
        if (arrayType.StartsWith("Array(", StringComparison.Ordinal) &&
            arrayType.EndsWith(")", StringComparison.Ordinal))
        {
            return arrayType[6..^1];
        }
        return "String"; // Default fallback
    }
}
