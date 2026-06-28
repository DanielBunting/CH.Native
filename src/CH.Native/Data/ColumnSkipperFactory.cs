using CH.Native.Data.ColumnSkippers;
using CH.Native.Data.Types;

namespace CH.Native.Data;

/// <summary>
/// Factory for creating column skippers based on ClickHouse type names.
/// Handles parameterized and composite types that the registry cannot directly resolve.
/// </summary>
public sealed class ColumnSkipperFactory
{
    private readonly ColumnSkipperRegistry _registry;

    /// <summary>
    /// Initializes a new <see cref="ColumnSkipperFactory"/>.
    /// </summary>
    /// <param name="registry">The registry consulted to resolve skippers for simple, non-composite types.</param>
    public ColumnSkipperFactory(ColumnSkipperRegistry registry)
    {
        _registry = registry;
    }

    /// <summary>
    /// Creates a column skipper for the specified ClickHouse type name.
    /// </summary>
    /// <param name="typeName">The ClickHouse type name (e.g., "Nullable(Int32)", "Array(String)").</param>
    /// <returns>A column skipper for the type.</returns>
    /// <exception cref="NotSupportedException">Thrown if the type is not supported.</exception>
    public IColumnSkipper CreateSkipper(string typeName)
    {
        // JSON needs the factory reference to skip binary (v0/v3) formats.
        if (typeName == "JSON")
            return new JsonColumnSkipper(this);

        // Try direct registry lookup first for simple types
        if (_registry.TryGetSkipper(typeName, out var directSkipper))
            return directSkipper!;

        // Parse the type for complex handling
        var type = ClickHouseTypeParser.Parse(typeName);
        return CreateSkipperForType(type);
    }

    private IColumnSkipper CreateSkipperForType(ClickHouseType type)
    {
        // Handle composite/wrapper types
        return type.BaseName switch
        {
            "Nullable" => CreateNullableSkipper(type),
            "Array" => CreateArraySkipper(type),
            "Map" => CreateMapSkipper(type),
            "Tuple" => CreateTupleSkipper(type),
            "Nested" => CreateNestedSkipper(type),
            "LowCardinality" => CreateLowCardinalitySkipper(type),
            "Variant" => CreateVariantSkipper(type),
            "Dynamic" => new DynamicColumnSkipper(this, type.OriginalTypeName),
            "SimpleAggregateFunction" => CreateSimpleAggregateFunctionSkipper(type),
            "AggregateFunction" => CreateAggregateFunctionSkipper(type),
            "JSON" => new JsonColumnSkipper(this),

            // Parameterized simple types
            "FixedString" => CreateFixedStringSkipper(type),
            "DateTime" => CreateDateTimeSkipper(type),
            "DateTime64" => CreateDateTime64Skipper(type),
            "Time64" => new Time64ColumnSkipper(),
            "Decimal32" => new Decimal32ColumnSkipper(),
            "Decimal64" => new Decimal64ColumnSkipper(),
            "Decimal128" => new Decimal128ColumnSkipper(),
            "Decimal256" => new Decimal256ColumnSkipper(),
            "Decimal" => CreateDecimalSkipper(type),

            // Fall back to registry for simple types
            _ => GetSimpleSkipper(type)
        };
    }

    private IColumnSkipper GetSimpleSkipper(ClickHouseType type)
    {
        // Try exact match first
        if (_registry.TryGetSkipper(type.OriginalTypeName, out var skipper))
            return skipper!;

        // Try base name (for types like Enum8('a' = 1) that map to Enum8)
        if (_registry.TryGetSkipper(type.BaseName, out skipper))
            return skipper!;

        throw new NotSupportedException($"Column type '{type.OriginalTypeName}' is not supported for skipping.");
    }

    private IColumnSkipper CreateNullableSkipper(ClickHouseType type)
    {
        if (type.TypeArguments.Count != 1)
            throw new FormatException($"Nullable requires exactly one type argument, got: {type.OriginalTypeName}");

        var innerType = type.TypeArguments[0];
        NullableInnerValidator.EnsureAllowedInsideNullable(innerType);

        var innerSkipper = CreateSkipperForType(innerType);

        return new NullableColumnSkipper(innerSkipper, innerType.OriginalTypeName);
    }

    private IColumnSkipper CreateArraySkipper(ClickHouseType type)
    {
        if (type.TypeArguments.Count != 1)
            throw new FormatException($"Array requires exactly one type argument, got: {type.OriginalTypeName}");

        var elementType = type.TypeArguments[0];
        var elementSkipper = CreateSkipperForType(elementType);

        return new ArrayColumnSkipper(elementSkipper, elementType.OriginalTypeName);
    }

    private IColumnSkipper CreateMapSkipper(ClickHouseType type)
    {
        if (type.TypeArguments.Count != 2)
            throw new FormatException($"Map requires exactly two type arguments, got: {type.OriginalTypeName}");

        var keyType = type.TypeArguments[0];
        var valueType = type.TypeArguments[1];
        var keySkipper = CreateSkipperForType(keyType);
        var valueSkipper = CreateSkipperForType(valueType);

        return new MapColumnSkipper(keySkipper, valueSkipper, keyType.OriginalTypeName, valueType.OriginalTypeName);
    }

    private IColumnSkipper CreateTupleSkipper(ClickHouseType type)
    {
        if (type.TypeArguments.Count == 0)
            throw new FormatException($"Tuple requires at least one type argument, got: {type.OriginalTypeName}");

        var elementSkippers = type.TypeArguments
            .Select(CreateSkipperForType)
            .ToArray();

        var elementTypeNames = type.TypeArguments
            .Select(t => t.OriginalTypeName)
            .ToArray();

        return new TupleColumnSkipper(elementSkippers, elementTypeNames);
    }

    private IColumnSkipper CreateNestedSkipper(ClickHouseType type)
    {
        if (type.TypeArguments.Count == 0)
            throw new FormatException($"Nested requires at least one field, got: {type.OriginalTypeName}");

        // A Nested column is parallel arrays sharing one offsets block. The skipper owns
        // the shared offsets, so it takes the field ELEMENT skippers (the inner field
        // types), not Array(fieldType) skippers.
        var fieldElementSkippers = type.TypeArguments
            .Select(CreateSkipperForType)
            .ToArray();

        return new NestedColumnSkipper(fieldElementSkippers, type.OriginalTypeName);
    }

    private IColumnSkipper CreateLowCardinalitySkipper(ClickHouseType type)
    {
        if (type.TypeArguments.Count != 1)
            throw new FormatException($"LowCardinality requires exactly one type argument, got: {type.OriginalTypeName}");

        var innerType = type.TypeArguments[0];
        LowCardinalityInnerValidator.EnsureAllowedInsideLowCardinality(innerType);

        // For Nullable inner types, ClickHouse serializes the LowCardinality dictionary
        // using the base type (without the Nullable wrapper). Strip it for correct skipping.
        if (innerType.BaseName == "Nullable" && innerType.TypeArguments.Count == 1)
        {
            var baseType = innerType.TypeArguments[0];
            var baseSkipper = CreateSkipperForType(baseType);
            return new LowCardinalityColumnSkipper(baseSkipper, innerType.OriginalTypeName);
        }

        var innerSkipper = CreateSkipperForType(innerType);
        return new LowCardinalityColumnSkipper(innerSkipper, innerType.OriginalTypeName);
    }

    private IColumnSkipper CreateVariantSkipper(ClickHouseType type)
    {
        if (type.TypeArguments.Count == 0)
            throw new FormatException($"Variant requires at least one arm, got: {type.OriginalTypeName}");

        foreach (var arm in type.TypeArguments)
            VariantArmValidator.EnsureAllowedAsVariantArm(arm);

        var innerSkippers = type.TypeArguments
            .Select(CreateSkipperForType)
            .ToArray();
        var innerTypeNames = type.TypeArguments
            .Select(t => t.OriginalTypeName)
            .ToArray();

        return new VariantColumnSkipper(innerSkippers, innerTypeNames);
    }

    private IColumnSkipper CreateSimpleAggregateFunctionSkipper(ClickHouseType type)
    {
        // SimpleAggregateFunction(name, T) is a wire-format pass-through for T —
        // route directly to the inner type's skipper, no wrapper class.
        if (type.TypeArguments.Count != 1)
            throw new FormatException(
                $"SimpleAggregateFunction requires exactly one type argument, got: {type.OriginalTypeName}");
        return CreateSkipperForType(type.TypeArguments[0]);
    }

    private IColumnSkipper CreateAggregateFunctionSkipper(ClickHouseType type)
    {
        if (type.AggregateFunctionName is null)
            throw new FormatException(
                $"AggregateFunction missing function name: {type.OriginalTypeName}");
        var format = AggregateState.AggregateFunctionStateFormatRegistry.Resolve(
            type.AggregateFunctionName, type.TypeArguments);
        return new AggregateFunctionColumnSkipper(type.OriginalTypeName, format);
    }

    private IColumnSkipper CreateFixedStringSkipper(ClickHouseType type)
    {
        if (type.Parameters.Count != 1)
            throw new FormatException($"FixedString requires exactly one parameter, got: {type.OriginalTypeName}");

        var length = int.Parse(type.Parameters[0]);
        return new FixedStringColumnSkipper(length);
    }

    private IColumnSkipper CreateDateTimeSkipper(ClickHouseType type)
    {
        // DateTime with or without timezone is still 4 bytes
        return new DateTimeColumnSkipper();
    }

    private IColumnSkipper CreateDateTime64Skipper(ClickHouseType type)
    {
        // DateTime64 is always 8 bytes regardless of precision/timezone
        return new DateTime64ColumnSkipper();
    }

    private IColumnSkipper CreateDecimalSkipper(ClickHouseType type)
    {
        // Generic Decimal(P, S) - determine size from precision
        if (type.Parameters.Count < 2)
            throw new FormatException($"Decimal requires precision and scale parameters, got: {type.OriginalTypeName}");

        var precision = int.Parse(type.Parameters[0]);

        // Choose appropriate decimal type based on precision
        return precision switch
        {
            <= 9 => new Decimal32ColumnSkipper(),
            <= 18 => new Decimal64ColumnSkipper(),
            <= 38 => new Decimal128ColumnSkipper(),
            _ => new Decimal256ColumnSkipper()
        };
    }
}
