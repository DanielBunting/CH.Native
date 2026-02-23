using CH.Native.Data.ColumnReaders;
using CH.Native.Data.Types;

namespace CH.Native.Data;

/// <summary>
/// Factory for creating column readers based on ClickHouse type names.
/// Handles parameterized and composite types that the registry cannot directly resolve.
/// </summary>
public sealed class ColumnReaderFactory
{
    private readonly ColumnReaderRegistry _registry;

    public ColumnReaderFactory(ColumnReaderRegistry registry)
    {
        _registry = registry;
    }

    /// <summary>
    /// Creates a column reader for the specified ClickHouse type name.
    /// </summary>
    /// <param name="typeName">The ClickHouse type name (e.g., "Nullable(Int32)", "Array(String)").</param>
    /// <returns>A column reader for the type.</returns>
    /// <exception cref="NotSupportedException">Thrown if the type is not supported.</exception>
    public IColumnReader CreateReader(string typeName)
    {
        // Try direct registry lookup first for simple types
        if (_registry.TryGetReader(typeName, out var directReader))
            return directReader!;

        // Parse the type for complex handling
        var type = ClickHouseTypeParser.Parse(typeName);
        return CreateReaderForType(type);
    }

    private IColumnReader CreateReaderForType(ClickHouseType type)
    {
        // Handle composite/wrapper types
        return type.BaseName switch
        {
            "Nullable" => CreateNullableReader(type),
            "Array" => CreateArrayReader(type),
            "Map" => CreateMapReader(type),
            "Tuple" => CreateTupleReader(type),
            "Nested" => CreateNestedReader(type),
            "LowCardinality" => CreateLowCardinalityReader(type),
            "JSON" => new ColumnReaders.JsonColumnReader(),

            // Parameterized simple types
            "FixedString" => CreateFixedStringReader(type),
            "DateTime" => CreateDateTimeReader(type),
            "DateTime64" => CreateDateTime64Reader(type),
            "Decimal32" => CreateDecimal32Reader(type),
            "Decimal64" => CreateDecimal64Reader(type),
            "Decimal128" => CreateDecimal128Reader(type),
            "Decimal256" => CreateDecimal256Reader(type),
            "Decimal" => CreateDecimalReader(type),

            // Fall back to registry for simple types
            _ => GetSimpleReader(type)
        };
    }

    private IColumnReader GetSimpleReader(ClickHouseType type)
    {
        // Try exact match first
        if (_registry.TryGetReader(type.OriginalTypeName, out var reader))
            return reader!;

        // Try base name (for types like Enum8('a' = 1) that map to Enum8)
        if (_registry.TryGetReader(type.BaseName, out reader))
            return reader!;

        throw new NotSupportedException($"Column type '{type.OriginalTypeName}' is not supported.");
    }

    private IColumnReader CreateNullableReader(ClickHouseType type)
    {
        if (type.TypeArguments.Count != 1)
            throw new FormatException($"Nullable requires exactly one type argument, got: {type.OriginalTypeName}");

        var innerType = type.TypeArguments[0];
        var innerReader = CreateReaderForType(innerType);

        // For lazy string mode, use the specialized lazy nullable string reader
        if (_registry.Strategy == StringMaterialization.Lazy && innerReader is StringColumnReader { IsLazy: true } lazyStringReader)
        {
            return new LazyNullableStringColumnReader(lazyStringReader);
        }

        // Use reflection to create the appropriate generic nullable reader
        var innerClrType = innerReader.ClrType;

        if (innerClrType.IsValueType)
        {
            var readerType = typeof(NullableColumnReader<>).MakeGenericType(innerClrType);
            return (IColumnReader)Activator.CreateInstance(readerType, innerReader)!;
        }
        else
        {
            // Reference types - use the reference type nullable reader
            var readerType = typeof(NullableRefColumnReader<>).MakeGenericType(innerClrType);
            return (IColumnReader)Activator.CreateInstance(readerType, innerReader)!;
        }
    }

    private IColumnReader CreateArrayReader(ClickHouseType type)
    {
        if (type.TypeArguments.Count != 1)
            throw new FormatException($"Array requires exactly one type argument, got: {type.OriginalTypeName}");

        var elementType = type.TypeArguments[0];
        var elementReader = CreateReaderForType(elementType);

        var readerType = typeof(ArrayColumnReader<>).MakeGenericType(elementReader.ClrType);
        return (IColumnReader)Activator.CreateInstance(readerType, elementReader)!;
    }

    private IColumnReader CreateMapReader(ClickHouseType type)
    {
        if (type.TypeArguments.Count != 2)
            throw new FormatException($"Map requires exactly two type arguments, got: {type.OriginalTypeName}");

        var keyType = type.TypeArguments[0];
        var valueType = type.TypeArguments[1];
        var keyReader = CreateReaderForType(keyType);
        var valueReader = CreateReaderForType(valueType);

        var readerType = typeof(MapColumnReader<,>).MakeGenericType(keyReader.ClrType, valueReader.ClrType);
        return (IColumnReader)Activator.CreateInstance(readerType, keyReader, valueReader)!;
    }

    private IColumnReader CreateTupleReader(ClickHouseType type)
    {
        if (type.TypeArguments.Count == 0)
            throw new FormatException($"Tuple requires at least one type argument, got: {type.OriginalTypeName}");

        var elementReaders = type.TypeArguments
            .Select(CreateReaderForType)
            .ToArray();

        // Pass field names if this is a named tuple
        var fieldNames = type.HasFieldNames ? type.FieldNames.ToArray() : null;

        return new TupleColumnReader(elementReaders, fieldNames);
    }

    private IColumnReader CreateNestedReader(ClickHouseType type)
    {
        if (type.TypeArguments.Count == 0)
            throw new FormatException($"Nested requires at least one field, got: {type.OriginalTypeName}");

        if (!type.HasFieldNames)
            throw new FormatException($"Nested type requires named fields, got: {type.OriginalTypeName}");

        // Each field in Nested is wrapped in Array
        var fieldReaders = type.TypeArguments
            .Select(fieldType =>
            {
                // Wrap each field type in Array
                var arrayType = new Types.ClickHouseType(
                    "Array",
                    typeArguments: new[] { fieldType },
                    originalTypeName: $"Array({fieldType.OriginalTypeName})");
                return CreateReaderForType(arrayType);
            })
            .ToArray();

        var fieldNames = type.FieldNames.ToArray();

        return new NestedColumnReader(fieldReaders, fieldNames);
    }

    private IColumnReader CreateLowCardinalityReader(ClickHouseType type)
    {
        if (type.TypeArguments.Count != 1)
            throw new FormatException($"LowCardinality requires exactly one type argument, got: {type.OriginalTypeName}");

        var innerType = type.TypeArguments[0];
        var innerReader = CreateReaderForType(innerType);

        var readerType = typeof(LowCardinalityColumnReader<>).MakeGenericType(innerReader.ClrType);
        return (IColumnReader)Activator.CreateInstance(readerType, innerReader)!;
    }

    private IColumnReader CreateFixedStringReader(ClickHouseType type)
    {
        if (type.Parameters.Count != 1)
            throw new FormatException($"FixedString requires exactly one parameter, got: {type.OriginalTypeName}");

        var length = int.Parse(type.Parameters[0]);
        return new FixedStringColumnReader(length);
    }

    private IColumnReader CreateDateTimeReader(ClickHouseType type)
    {
        // DateTime without parameters - use default reader from registry
        if (type.Parameters.Count == 0)
            return new DateTimeColumnReader();

        // DateTime('timezone') - use timezone-aware reader
        var timezone = type.Parameters[0].Trim('\'');
        return new DateTimeWithTimezoneColumnReader(timezone);
    }

    private IColumnReader CreateDateTime64Reader(ClickHouseType type)
    {
        if (type.Parameters.Count < 1)
            throw new FormatException($"DateTime64 requires at least one parameter (precision), got: {type.OriginalTypeName}");

        var precision = int.Parse(type.Parameters[0]);
        string? timezone = null;

        if (type.Parameters.Count > 1)
        {
            // Remove surrounding quotes from timezone
            timezone = type.Parameters[1].Trim('\'');
        }

        return new DateTime64ColumnReader(precision, timezone);
    }

    private IColumnReader CreateDecimal32Reader(ClickHouseType type)
    {
        var scale = type.Parameters.Count > 0 ? int.Parse(type.Parameters[0]) : 0;
        return new Decimal32ColumnReader(scale);
    }

    private IColumnReader CreateDecimal64Reader(ClickHouseType type)
    {
        var scale = type.Parameters.Count > 0 ? int.Parse(type.Parameters[0]) : 0;
        return new Decimal64ColumnReader(scale);
    }

    private IColumnReader CreateDecimal128Reader(ClickHouseType type)
    {
        var scale = type.Parameters.Count > 0 ? int.Parse(type.Parameters[0]) : 0;
        return new Decimal128ColumnReader(scale);
    }

    private IColumnReader CreateDecimal256Reader(ClickHouseType type)
    {
        var scale = type.Parameters.Count > 0 ? int.Parse(type.Parameters[0]) : 0;
        return new Decimal256ColumnReader(scale);
    }

    private IColumnReader CreateDecimalReader(ClickHouseType type)
    {
        // Generic Decimal(P, S) - determine size from precision
        if (type.Parameters.Count < 2)
            throw new FormatException($"Decimal requires precision and scale parameters, got: {type.OriginalTypeName}");

        var precision = int.Parse(type.Parameters[0]);
        var scale = int.Parse(type.Parameters[1]);

        // Choose appropriate decimal type based on precision
        return precision switch
        {
            <= 9 => new Decimal32ColumnReader(scale),
            <= 18 => new Decimal64ColumnReader(scale),
            <= 38 => new Decimal128ColumnReader(scale),
            _ => new Decimal256ColumnReader(scale)
        };
    }
}
