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

    /// <summary>
    /// For <c>AggregateFunction</c> and <c>SimpleAggregateFunction</c> types, the name of the
    /// aggregate function (e.g., "sum", "quantilesState", "count"). Null for all other types.
    /// </summary>
    public string? AggregateFunctionName { get; }

    /// <summary>
    /// For <c>AggregateFunction</c> types whose function takes literal parameters (e.g.,
    /// <c>quantilesState(0.5, 0.9)</c>), the parameter list as preserved source strings.
    /// Empty for functions without literal parameters and for non-aggregate types.
    /// </summary>
    public IReadOnlyList<string> AggregateFunctionParameters { get; }

    public ClickHouseType(
        string baseName,
        IReadOnlyList<ClickHouseType>? typeArguments = null,
        IReadOnlyList<string>? parameters = null,
        string? originalTypeName = null,
        IReadOnlyList<string>? fieldNames = null)
        : this(baseName, typeArguments, parameters, originalTypeName, fieldNames, aggregateFunctionName: null, aggregateFunctionParameters: null)
    {
    }

    /// <summary>
    /// Internal constructor used by the parser to populate the aggregate-function
    /// descriptor fields on <c>AggregateFunction</c> / <c>SimpleAggregateFunction</c>
    /// types. Public callers should use the 5-arg overload; the aggregate-function
    /// fields default to null/empty and aren't relevant for non-aggregate types.
    /// </summary>
    internal ClickHouseType(
        string baseName,
        IReadOnlyList<ClickHouseType>? typeArguments,
        IReadOnlyList<string>? parameters,
        string? originalTypeName,
        IReadOnlyList<string>? fieldNames,
        string? aggregateFunctionName,
        IReadOnlyList<string>? aggregateFunctionParameters)
    {
        BaseName = baseName;
        TypeArguments = typeArguments ?? Array.Empty<ClickHouseType>();
        Parameters = parameters ?? Array.Empty<string>();
        OriginalTypeName = originalTypeName ?? baseName;
        FieldNames = fieldNames ?? Array.Empty<string>();
        AggregateFunctionName = aggregateFunctionName;
        AggregateFunctionParameters = aggregateFunctionParameters ?? Array.Empty<string>();
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

    /// <summary>
    /// Whether this is a Variant(T1, T2, …) tagged-union type.
    /// </summary>
    public bool IsVariant => BaseName == "Variant";

    /// <summary>
    /// Whether this is a Dynamic or Dynamic(max_types=N) self-describing variant type.
    /// </summary>
    public bool IsDynamic => BaseName == "Dynamic";

    /// <summary>
    /// Whether this is an <c>AggregateFunction(name, T...)</c> type — opaque per-row
    /// aggregate state, exposed as <c>ClickHouseAggregateState</c>.
    /// </summary>
    public bool IsAggregateFunction => BaseName == "AggregateFunction";

    /// <summary>
    /// Whether this is a <c>SimpleAggregateFunction(name, T)</c> type — a transparent
    /// wire-format pass-through of the inner type <c>T</c>.
    /// </summary>
    public bool IsSimpleAggregateFunction => BaseName == "SimpleAggregateFunction";

    /// <summary>
    /// Returns the max_types parameter for a Dynamic type, defaulting to 32 when unspecified.
    /// </summary>
    public int GetDynamicMaxTypes()
    {
        if (!IsDynamic)
            throw new InvalidOperationException($"GetDynamicMaxTypes is only valid on Dynamic types, not {BaseName}.");

        foreach (var param in Parameters)
        {
            var eq = param.IndexOf('=');
            if (eq < 0) continue;
            var key = param.AsSpan(0, eq).Trim();
            if (key.SequenceEqual("max_types"))
            {
                var value = param.AsSpan(eq + 1).Trim();
                if (int.TryParse(value, out var n))
                    return n;
            }
        }
        return 32;
    }

    public override string ToString()
    {
        // AggregateFunction(name, T...) / SimpleAggregateFunction(name, T): the function
        // descriptor leads the argument list and is special-cased — it isn't a type and
        // isn't a parameter, it's a function reference with optional literal params.
        if (AggregateFunctionName is not null)
        {
            var descriptor = AggregateFunctionParameters.Count > 0
                ? $"{AggregateFunctionName}({string.Join(", ", AggregateFunctionParameters)})"
                : AggregateFunctionName;

            if (TypeArguments.Count == 0)
                return $"{BaseName}({descriptor})";

            var aggArgs = new List<string>(1 + TypeArguments.Count) { descriptor };
            aggArgs.AddRange(TypeArguments.Select(t => t.ToString()));
            return $"{BaseName}({string.Join(", ", aggArgs)})";
        }

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
