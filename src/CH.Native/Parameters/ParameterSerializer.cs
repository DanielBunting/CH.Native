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
        return value switch
        {
            // Strings - quoted with single quotes and escaped
            string s => EscapeString(s),

            // Booleans - as string "1" or "0"
            bool b => EscapeString(b ? "1" : "0"),

            // Integers (signed) - as quoted string
            sbyte sb => EscapeString(sb.ToString(CultureInfo.InvariantCulture)),
            short sh => EscapeString(sh.ToString(CultureInfo.InvariantCulture)),
            int i => EscapeString(i.ToString(CultureInfo.InvariantCulture)),
            long l => EscapeString(l.ToString(CultureInfo.InvariantCulture)),
            Int128 i128 => EscapeString(i128.ToString(CultureInfo.InvariantCulture)),

            // Integers (unsigned) - as quoted string
            byte by => EscapeString(by.ToString(CultureInfo.InvariantCulture)),
            ushort us => EscapeString(us.ToString(CultureInfo.InvariantCulture)),
            uint ui => EscapeString(ui.ToString(CultureInfo.InvariantCulture)),
            ulong ul => EscapeString(ul.ToString(CultureInfo.InvariantCulture)),
            UInt128 ui128 => EscapeString(ui128.ToString(CultureInfo.InvariantCulture)),

            // Floating point - as quoted string
            float f => EscapeString(SerializeFloatValue(f)),
            double d => EscapeString(SerializeDoubleValue(d)),
            decimal dec => EscapeString(dec.ToString(CultureInfo.InvariantCulture)),

            // Date/Time - quoted string format
            DateTime dt => EscapeString(SerializeDateTimeRaw(dt, clickHouseType)),
            DateTimeOffset dto => EscapeString(SerializeDateTimeOffsetRaw(dto)),
            DateOnly date => EscapeString(date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)),

            // Guid - quoted UUID format
            Guid g => EscapeString(g.ToString("D")),

            // IP addresses - quoted format
            System.Net.IPAddress ip => EscapeString(ip.ToString()),

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
        // ClickHouse Field dump format escaping rules:
        // - Single quotes are escaped as \'
        // - Backslashes are escaped as \\
        // - Control characters are passed through as-is (ClickHouse handles them)

        var sb = new StringBuilder(value.Length + 10);
        sb.Append('\'');

        foreach (var c in value)
        {
            switch (c)
            {
                case '\'':
                    sb.Append("\\'");
                    break;
                case '\\':
                    sb.Append("\\\\");
                    break;
                default:
                    sb.Append(c);
                    break;
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

        // Return array as quoted string
        return EscapeString($"[{string.Join(", ", elements)}]");
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
        var sb = new StringBuilder(value.Length + 10);
        sb.Append('\'');
        foreach (var c in value)
        {
            switch (c)
            {
                case '\'':
                    sb.Append("\\'");
                    break;
                case '\\':
                    sb.Append("\\\\");
                    break;
                default:
                    sb.Append(c);
                    break;
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
