using System.Text.Json;
using CH.Native.Numerics;

namespace CH.Native.Parameters;

/// <summary>
/// Maps .NET types to ClickHouse type names.
/// </summary>
/// <remarks>
/// Security invariant: every value emitted by this class is a constant baked into
/// <see cref="TypeMappings"/> or recursively composed via <c>Array(...)</c>. Strings
/// from external input must not be substituted in. If a future overload accepts a
/// type name from user input, route it through <see cref="Data.Types.ClickHouseTypeParser.Parse"/>
/// (the same gate used by <c>ClickHouseParameter.ClickHouseType</c>) before storing
/// or emitting it — otherwise the wire {name:Type} placeholder becomes a SQL-injection
/// vector.
/// </remarks>
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
        { typeof(ClickHouseDecimal), "Decimal128(18)" },

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

        // Value-level refinements the CLR type alone cannot express:
        if (value is DBNull)
            return "Nullable(Nothing)";
        if (value is System.Net.IPAddress ip)
            return ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork ? "IPv4" : "IPv6";

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

        // .NET enum -> Enum8/Enum16 built from the enum's members. Enum8 when every value
        // fits a signed byte, otherwise Enum16 (ClickHouse has no wider enum). The member
        // name is used as the label, matching how ParameterSerializer emits an enum value.
        if (underlyingType.IsEnum)
            return InferEnumType(underlyingType);

        // Tuple / ValueTuple -> Tuple(...). Flattens the >7-element TRest nesting
        // (ValueTuple<T1..T7, TRest>) so an 8+-element tuple maps to a flat Tuple(...).
        if (IsTupleType(underlyingType))
        {
            var inner = string.Join(", ", FlattenTupleTypes(underlyingType).Select(InferTypeFromClrType));
            return $"Tuple({inner})";
        }

        // Handle arrays (jagged and rectangular). Rectangular arrays (T[,], T[,,])
        // map to nested Array(...) on the wire — the rank-N rect is unwrapped into
        // N levels of Array. Hybrid shapes that mix rectangular and jagged at
        // different levels (T[,][], T[][,], T[,][,]) are rejected: the boundary
        // converter only handles a single rectangular section.
        if (underlyingType.IsArray)
        {
            var elementType = underlyingType.GetElementType()!;
            var rank = underlyingType.GetArrayRank();

            if (rank > 1 && elementType.IsArray)
            {
                throw new NotSupportedException(
                    $"Hybrid array shapes are not supported (type '{type.FullName}'). " +
                    "Use either pure jagged (e.g., int[][]) or pure rectangular (e.g., int[,,]).");
            }
            if (rank == 1 && elementType.IsArray && elementType.GetArrayRank() > 1)
            {
                throw new NotSupportedException(
                    $"Hybrid array shapes are not supported (type '{type.FullName}'). " +
                    "Use either pure jagged (e.g., int[][]) or pure rectangular (e.g., int[,,]).");
            }

            var inner = InferTypeFromClrType(elementType);
            for (int i = 0; i < rank; i++)
                inner = $"Array({inner})";
            return inner;
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

    private static string InferEnumType(Type enumType)
    {
        var names = Enum.GetNames(enumType);
        var values = new long[names.Length];
        var fitsInt8 = true;
        for (int i = 0; i < names.Length; i++)
        {
            values[i] = Convert.ToInt64(Enum.Parse(enumType, names[i]), System.Globalization.CultureInfo.InvariantCulture);
            if (values[i] is < sbyte.MinValue or > sbyte.MaxValue)
                fitsInt8 = false;
            if (values[i] is < short.MinValue or > short.MaxValue)
                throw new NotSupportedException(
                    $"Enum '{enumType.FullName}' has value {values[i]} outside the ClickHouse Enum16 range.");
        }

        var kind = fitsInt8 ? "Enum8" : "Enum16";
        var members = new string[names.Length];
        for (int i = 0; i < names.Length; i++)
            members[i] = $"'{names[i]}' = {values[i].ToString(System.Globalization.CultureInfo.InvariantCulture)}";
        return $"{kind}({string.Join(", ", members)})";
    }

    private static bool IsTupleType(Type type) =>
        type.IsGenericType && typeof(System.Runtime.CompilerServices.ITuple).IsAssignableFrom(type);

    private static IEnumerable<Type> FlattenTupleTypes(Type tupleType)
    {
        var args = tupleType.GetGenericArguments();
        for (int i = 0; i < args.Length; i++)
        {
            // Element 8 of a ValueTuple is the TRest continuation holding elements 8+.
            if (i == 7 && IsTupleType(args[i]))
            {
                foreach (var rest in FlattenTupleTypes(args[i]))
                    yield return rest;
            }
            else
            {
                yield return args[i];
            }
        }
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
               underlyingType == typeof(decimal) ||
               underlyingType == typeof(ClickHouseDecimal);
    }
}
