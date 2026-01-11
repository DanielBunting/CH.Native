using System.Text.Json;

namespace CH.Native.Parameters;

/// <summary>
/// Maps .NET types to ClickHouse type names.
/// </summary>
public static class ClickHouseTypeMapper
{
    private static readonly Dictionary<Type, string> TypeMappings = new()
    {
        // Integers (signed)
        { typeof(sbyte), "Int8" },
        { typeof(short), "Int16" },
        { typeof(int), "Int32" },
        { typeof(long), "Int64" },
        { typeof(Int128), "Int128" },

        // Integers (unsigned)
        { typeof(byte), "UInt8" },
        { typeof(ushort), "UInt16" },
        { typeof(uint), "UInt32" },
        { typeof(ulong), "UInt64" },
        { typeof(UInt128), "UInt128" },

        // Floating point
        { typeof(float), "Float32" },
        { typeof(double), "Float64" },
        { typeof(decimal), "Decimal128(18)" },

        // Boolean
        { typeof(bool), "Bool" },

        // String
        { typeof(string), "String" },

        // Date/Time
        { typeof(DateTime), "DateTime" },
        { typeof(DateTimeOffset), "DateTime64(6)" },
        { typeof(DateOnly), "Date" },

        // Guid
        { typeof(Guid), "UUID" },

        // Network
        { typeof(System.Net.IPAddress), "IPv6" },

        // JSON
        { typeof(JsonDocument), "JSON" },
        { typeof(JsonElement), "JSON" },
    };

    /// <summary>
    /// Infers the ClickHouse type from a .NET value.
    /// </summary>
    /// <param name="value">The value to infer type from.</param>
    /// <returns>The ClickHouse type name.</returns>
    /// <exception cref="ArgumentException">Thrown when value is null.</exception>
    /// <exception cref="NotSupportedException">Thrown when type cannot be inferred.</exception>
    public static string InferType(object? value)
    {
        if (value is null)
            throw new ArgumentException("Cannot infer type from null value. Provide an explicit ClickHouseType.", nameof(value));

        var type = value.GetType();
        return InferTypeFromClrType(type);
    }

    /// <summary>
    /// Infers the ClickHouse type from a CLR Type.
    /// </summary>
    /// <param name="type">The CLR type.</param>
    /// <returns>The ClickHouse type name.</returns>
    /// <exception cref="NotSupportedException">Thrown when type cannot be inferred.</exception>
    public static string InferTypeFromClrType(Type type)
    {
        // Handle Nullable<T>
        var underlyingType = Nullable.GetUnderlyingType(type) ?? type;

        // Direct type match
        if (TypeMappings.TryGetValue(underlyingType, out var clickHouseType))
            return clickHouseType;

        // Handle arrays
        if (underlyingType.IsArray)
        {
            var elementType = underlyingType.GetElementType()!;
            var elementClickHouseType = InferTypeFromClrType(elementType);
            return $"Array({elementClickHouseType})";
        }

        // Handle IEnumerable<T> (but not string)
        if (underlyingType != typeof(string))
        {
            var enumerableInterface = underlyingType.GetInterfaces()
                .FirstOrDefault(i => i.IsGenericType &&
                                     i.GetGenericTypeDefinition() == typeof(IEnumerable<>));
            if (enumerableInterface != null)
            {
                var elementType = enumerableInterface.GetGenericArguments()[0];
                var elementClickHouseType = InferTypeFromClrType(elementType);
                return $"Array({elementClickHouseType})";
            }
        }

        throw new NotSupportedException(
            $"Cannot infer ClickHouse type for .NET type '{type.FullName}'. " +
            "Provide an explicit ClickHouseType.");
    }

    /// <summary>
    /// Checks if the given type is a supported numeric type.
    /// </summary>
    internal static bool IsNumericType(Type type)
    {
        var underlyingType = Nullable.GetUnderlyingType(type) ?? type;
        return underlyingType == typeof(sbyte) ||
               underlyingType == typeof(byte) ||
               underlyingType == typeof(short) ||
               underlyingType == typeof(ushort) ||
               underlyingType == typeof(int) ||
               underlyingType == typeof(uint) ||
               underlyingType == typeof(long) ||
               underlyingType == typeof(ulong) ||
               underlyingType == typeof(Int128) ||
               underlyingType == typeof(UInt128) ||
               underlyingType == typeof(float) ||
               underlyingType == typeof(double) ||
               underlyingType == typeof(decimal);
    }
}
