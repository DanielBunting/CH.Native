namespace CH.Native.Data.Types;

/// <summary>
/// Represents a parsed ClickHouse type with optional type arguments and parameters.
/// </summary>
/// <remarks>
/// Examples:
/// - Simple: "Int32" -> BaseName="Int32", no arguments
/// - Parameterized: "DateTime64(3)" -> BaseName="DateTime64", Parameters=["3"]
/// - Nested: "Nullable(Int32)" -> BaseName="Nullable", TypeArguments=[Int32]
/// - Complex: "Map(String, Array(Int32))" -> BaseName="Map", TypeArguments=[String, Array(Int32)]
/// - Named Tuple: "Tuple(id UInt64, name String)" -> BaseName="Tuple", TypeArguments=[UInt64, String], FieldNames=["id", "name"]
/// </remarks>
public sealed class ClickHouseType
{
    /// <summary>
    /// The base type name (e.g., "Nullable", "Array", "Int32").
    /// </summary>
    public string BaseName { get; }

    /// <summary>
    /// Nested type arguments for composite types (e.g., the Int32 in Nullable(Int32)).
    /// </summary>
    public IReadOnlyList<ClickHouseType> TypeArguments { get; }

    /// <summary>
    /// Non-type parameters like scale, precision, or enum definitions.
    /// </summary>
    public IReadOnlyList<string> Parameters { get; }

    /// <summary>
    /// Field names for named tuples and nested types (e.g., ["id", "name"] for Tuple(id UInt64, name String)).
    /// Empty if the tuple uses positional syntax.
    /// </summary>
    public IReadOnlyList<string> FieldNames { get; }

    /// <summary>
    /// The original type name string as received from ClickHouse.
    /// </summary>
    public string OriginalTypeName { get; }

    public ClickHouseType(
        string baseName,
        IReadOnlyList<ClickHouseType>? typeArguments = null,
        IReadOnlyList<string>? parameters = null,
        string? originalTypeName = null,
        IReadOnlyList<string>? fieldNames = null)
    {
        BaseName = baseName;
        TypeArguments = typeArguments ?? Array.Empty<ClickHouseType>();
        Parameters = parameters ?? Array.Empty<string>();
        OriginalTypeName = originalTypeName ?? baseName;
        FieldNames = fieldNames ?? Array.Empty<string>();
    }

    /// <summary>
    /// Whether this tuple or nested type has named fields.
    /// </summary>
    public bool HasFieldNames => FieldNames.Count > 0;

    /// <summary>
    /// Whether this type has any arguments or parameters.
    /// </summary>
    public bool IsParameterized => TypeArguments.Count > 0 || Parameters.Count > 0;

    /// <summary>
    /// Whether this is a Nullable wrapper type.
    /// </summary>
    public bool IsNullable => BaseName == "Nullable";

    /// <summary>
    /// Whether this is an Array type.
    /// </summary>
    public bool IsArray => BaseName == "Array";

    /// <summary>
    /// Whether this is a Map type.
    /// </summary>
    public bool IsMap => BaseName == "Map";

    /// <summary>
    /// Whether this is a Tuple type.
    /// </summary>
    public bool IsTuple => BaseName == "Tuple";

    /// <summary>
    /// Whether this is a LowCardinality wrapper type.
    /// </summary>
    public bool IsLowCardinality => BaseName == "LowCardinality";

    /// <summary>
    /// Whether this is a FixedString type.
    /// </summary>
    public bool IsFixedString => BaseName == "FixedString";

    /// <summary>
    /// Whether this is a DateTime64 type.
    /// </summary>
    public bool IsDateTime64 => BaseName == "DateTime64";

    /// <summary>
    /// Whether this is a Decimal type (any precision).
    /// </summary>
    public bool IsDecimal => BaseName is "Decimal32" or "Decimal64" or "Decimal128" or "Decimal256" or "Decimal";

    /// <summary>
    /// Whether this is an Enum type.
    /// </summary>
    public bool IsEnum => BaseName is "Enum8" or "Enum16";

    /// <summary>
    /// Whether this is a Nested type.
    /// </summary>
    public bool IsNested => BaseName == "Nested";

    public override string ToString()
    {
        if (!IsParameterized)
            return BaseName;

        var args = new List<string>();

        // Handle named tuples/nested types
        if (HasFieldNames && FieldNames.Count == TypeArguments.Count)
        {
            for (int i = 0; i < TypeArguments.Count; i++)
            {
                args.Add($"{FieldNames[i]} {TypeArguments[i]}");
            }
        }
        else
        {
            args.AddRange(TypeArguments.Select(t => t.ToString()));
        }

        args.AddRange(Parameters);

        return $"{BaseName}({string.Join(", ", args)})";
    }
}
