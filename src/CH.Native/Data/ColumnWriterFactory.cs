using CH.Native.Data.ColumnWriters;
using CH.Native.Data.Types;

namespace CH.Native.Data;

/// <summary>
/// Factory for creating column writers based on ClickHouse type names.
/// Handles parameterized and composite types that the registry cannot directly resolve.
/// </summary>
public sealed class ColumnWriterFactory
{
    private readonly ColumnWriterRegistry _registry;

    public ColumnWriterFactory(ColumnWriterRegistry registry)
    {
        _registry = registry;
    }

    /// <summary>
    /// Creates a column writer for the specified ClickHouse type name.
    /// </summary>
    /// <param name="typeName">The ClickHouse type name (e.g., "Nullable(Int32)", "Array(String)").</param>
    /// <returns>A column writer for the type.</returns>
    /// <exception cref="NotSupportedException">Thrown if the type is not supported.</exception>
    public IColumnWriter CreateWriter(string typeName)
    {
        // Try direct registry lookup first for simple types
        if (_registry.TryGetWriter(typeName, out var directWriter))
            return directWriter!;

        // Parse the type for complex handling
        var type = ClickHouseTypeParser.Parse(typeName);
        return CreateWriterForType(type);
    }

    private IColumnWriter CreateWriterForType(ClickHouseType type)
    {
        // Handle composite/wrapper types
        return type.BaseName switch
        {
            "Nullable" => CreateNullableWriter(type),
            "Array" => CreateArrayWriter(type),
            "Map" => CreateMapWriter(type),
            "Tuple" => CreateTupleWriter(type),
            "LowCardinality" => CreateLowCardinalityWriter(type),

            // Parameterized simple types
            "FixedString" => CreateFixedStringWriter(type),
            "DateTime" => CreateDateTimeWriter(type),
            "DateTime64" => CreateDateTime64Writer(type),
            "Decimal32" => CreateDecimal32Writer(type),
            "Decimal64" => CreateDecimal64Writer(type),
            "Decimal128" => CreateDecimal128Writer(type),
            "Decimal256" => CreateDecimal256Writer(type),
            "Decimal" => CreateDecimalWriter(type),

            // Fall back to registry for simple types
            _ => GetSimpleWriter(type)
        };
    }

    private IColumnWriter GetSimpleWriter(ClickHouseType type)
    {
        // Try exact match first
        if (_registry.TryGetWriter(type.OriginalTypeName, out var writer))
            return writer!;

        // Try base name (for types like Enum8('foo' = 1) that map to Enum8)
        if (_registry.TryGetWriter(type.BaseName, out writer))
            return writer!;

        throw new NotSupportedException($"Column type '{type.OriginalTypeName}' is not supported for writing.");
    }

    private IColumnWriter CreateNullableWriter(ClickHouseType type)
    {
        if (type.TypeArguments.Count != 1)
            throw new FormatException($"Nullable requires exactly one type argument, got: {type.OriginalTypeName}");

        var innerType = type.TypeArguments[0];
        var innerWriter = CreateWriterForType(innerType);

        // Use reflection to create the appropriate generic nullable writer
        var innerClrType = innerWriter.ClrType;

        if (innerClrType.IsValueType)
        {
            var writerType = typeof(NullableColumnWriter<>).MakeGenericType(innerClrType);
            return (IColumnWriter)Activator.CreateInstance(writerType, innerWriter)!;
        }
        else
        {
            // Reference types - use the reference type nullable writer
            var writerType = typeof(NullableRefColumnWriter<>).MakeGenericType(innerClrType);
            return (IColumnWriter)Activator.CreateInstance(writerType, innerWriter)!;
        }
    }

    private IColumnWriter CreateArrayWriter(ClickHouseType type)
    {
        if (type.TypeArguments.Count != 1)
            throw new FormatException($"Array requires exactly one type argument, got: {type.OriginalTypeName}");

        var elementType = type.TypeArguments[0];
        var elementWriter = CreateWriterForType(elementType);

        var writerType = typeof(ArrayColumnWriter<>).MakeGenericType(elementWriter.ClrType);
        return (IColumnWriter)Activator.CreateInstance(writerType, elementWriter)!;
    }

    private IColumnWriter CreateMapWriter(ClickHouseType type)
    {
        if (type.TypeArguments.Count != 2)
            throw new FormatException($"Map requires exactly two type arguments, got: {type.OriginalTypeName}");

        var keyType = type.TypeArguments[0];
        var valueType = type.TypeArguments[1];
        var keyWriter = CreateWriterForType(keyType);
        var valueWriter = CreateWriterForType(valueType);

        var writerType = typeof(MapColumnWriter<,>).MakeGenericType(keyWriter.ClrType, valueWriter.ClrType);
        return (IColumnWriter)Activator.CreateInstance(writerType, keyWriter, valueWriter)!;
    }

    private IColumnWriter CreateTupleWriter(ClickHouseType type)
    {
        if (type.TypeArguments.Count == 0)
            throw new FormatException($"Tuple requires at least one type argument, got: {type.OriginalTypeName}");

        var elementWriters = type.TypeArguments
            .Select(CreateWriterForType)
            .ToArray();

        // Pass field names if this is a named tuple
        var fieldNames = type.HasFieldNames ? type.FieldNames.ToArray() : null;

        return new TupleColumnWriter(elementWriters, fieldNames);
    }

    private IColumnWriter CreateLowCardinalityWriter(ClickHouseType type)
    {
        if (type.TypeArguments.Count != 1)
            throw new FormatException($"LowCardinality requires exactly one type argument, got: {type.OriginalTypeName}");

        var innerType = type.TypeArguments[0];
        var innerWriter = CreateWriterForType(innerType);

        var writerType = typeof(LowCardinalityColumnWriter<>).MakeGenericType(innerWriter.ClrType);
        return (IColumnWriter)Activator.CreateInstance(writerType, innerWriter)!;
    }

    private IColumnWriter CreateFixedStringWriter(ClickHouseType type)
    {
        if (type.Parameters.Count != 1)
            throw new FormatException($"FixedString requires exactly one parameter, got: {type.OriginalTypeName}");

        var length = int.Parse(type.Parameters[0]);
        return new FixedStringColumnWriter(length);
    }

    private IColumnWriter CreateDateTimeWriter(ClickHouseType type)
    {
        // DateTime without parameters - use default writer from registry
        if (type.Parameters.Count == 0)
            return new DateTimeColumnWriter();

        // DateTime('timezone') - use timezone-aware writer
        var timezone = type.Parameters[0].Trim('\'');
        return new DateTimeWithTimezoneColumnWriter(timezone);
    }

    private IColumnWriter CreateDateTime64Writer(ClickHouseType type)
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

        return new DateTime64ColumnWriter(precision, timezone);
    }

    private IColumnWriter CreateDecimal32Writer(ClickHouseType type)
    {
        var scale = type.Parameters.Count > 0 ? int.Parse(type.Parameters[0]) : 0;
        return new Decimal32ColumnWriter(scale);
    }

    private IColumnWriter CreateDecimal64Writer(ClickHouseType type)
    {
        var scale = type.Parameters.Count > 0 ? int.Parse(type.Parameters[0]) : 0;
        return new Decimal64ColumnWriter(scale);
    }

    private IColumnWriter CreateDecimal128Writer(ClickHouseType type)
    {
        var scale = type.Parameters.Count > 0 ? int.Parse(type.Parameters[0]) : 0;
        return new Decimal128ColumnWriter(scale);
    }

    private IColumnWriter CreateDecimal256Writer(ClickHouseType type)
    {
        var scale = type.Parameters.Count > 0 ? int.Parse(type.Parameters[0]) : 0;
        return new Decimal256ColumnWriter(scale);
    }

    private IColumnWriter CreateDecimalWriter(ClickHouseType type)
    {
        // Generic Decimal(P, S) - determine size from precision
        if (type.Parameters.Count < 2)
            throw new FormatException($"Decimal requires precision and scale parameters, got: {type.OriginalTypeName}");

        var precision = int.Parse(type.Parameters[0]);
        var scale = int.Parse(type.Parameters[1]);

        // Choose appropriate decimal type based on precision
        return precision switch
        {
            <= 9 => new Decimal32ColumnWriter(scale),
            <= 18 => new Decimal64ColumnWriter(scale),
            <= 38 => new Decimal128ColumnWriter(scale),
            _ => new Decimal256ColumnWriter(scale)
        };
    }
}
