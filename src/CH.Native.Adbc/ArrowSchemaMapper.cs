using System.Linq;
using Apache.Arrow;
using Apache.Arrow.Types;
using CH.Native.Data.Types;

namespace CH.Native.Adbc;

/// <summary>
/// Maps ClickHouse column types (as emitted in a <see cref="CH.Native.Data.TypedBlock"/>) to an
/// Arrow <see cref="Schema"/>. Scalar tier only: numeric, boolean, string/fixed-string, UUID,
/// IP, date/time and decimal types, optionally wrapped in <c>Nullable</c> and/or
/// <c>LowCardinality</c>. Composite types (Array/Map/Tuple) are not yet supported.
/// </summary>
internal static class ArrowSchemaMapper
{
    public static Schema ToSchema(string[] columnNames, string[] columnTypes)
    {
        if (columnNames.Length != columnTypes.Length)
            throw new ArgumentException("Column name and type arrays must have the same length.");

        var fields = new Field[columnNames.Length];
        for (int i = 0; i < columnNames.Length; i++)
        {
            var parsed = ClickHouseTypeParser.Parse(columnTypes[i]);
            var (scalar, nullable) = Unwrap(parsed);
            var arrowType = ToArrowType(scalar, columnTypes[i]);
            fields[i] = new Field(columnNames[i], arrowType, nullable);
        }

        return new Schema(fields, metadata: null);
    }

    /// <summary>
    /// Peels <c>Nullable</c> and <c>LowCardinality</c> wrappers, returning the underlying scalar
    /// type and whether the column can contain nulls.
    /// </summary>
    public static (ClickHouseType Scalar, bool Nullable) Unwrap(ClickHouseType type)
    {
        bool nullable = false;
        while (true)
        {
            switch (type.BaseName)
            {
                case "LowCardinality":
                    type = type.TypeArguments[0];
                    continue;
                case "Nullable":
                    nullable = true;
                    type = type.TypeArguments[0];
                    continue;
                default:
                    return (type, nullable);
            }
        }
    }

    public static IArrowType ToArrowType(ClickHouseType scalar, string originalTypeName)
    {
        // Interval types are named per unit (IntervalSecond, IntervalDay, …); there is no single
        // Arrow interval that captures every ClickHouse unit (Week, Quarter, …), so surface as text.
        if (scalar.BaseName.StartsWith("Interval", StringComparison.Ordinal))
            return StringType.Default;

        return scalar.BaseName switch
        {
            "Int8" or "Enum8" => Int8Type.Default,
            "Int16" or "Enum16" => Int16Type.Default,
            "Int32" => Int32Type.Default,
            "Int64" => Int64Type.Default,
            "UInt8" => UInt8Type.Default,
            "UInt16" => UInt16Type.Default,
            "UInt32" => UInt32Type.Default,
            "UInt64" => UInt64Type.Default,
            // Arrow has no integer wider than 64 bits, and Decimal256 tops out at 76 digits — one
            // short of Int256/UInt256's range — so wide integers are surfaced as exact decimal text.
            "Int128" or "Int256" or "UInt128" or "UInt256" => StringType.Default,
            "Float32" or "BFloat16" => FloatType.Default,
            "Float64" => DoubleType.Default,
            "Bool" => BooleanType.Default,
            "String" => StringType.Default,
            // UUID and IP addresses are surfaced as text in the scalar tier; binary forms
            // (FixedSizeBinary, UInt32 for IPv4) are a planned fidelity follow-up.
            "UUID" or "IPv4" or "IPv6" => StringType.Default,
            "FixedString" => BinaryType.Default,
            "Date" or "Date32" => Date32Type.Default,
            "DateTime" => new TimestampType(TimeUnit.Second, DateTimeTimezone(scalar, precisionParamCount: 0)),
            "DateTime64" => new TimestampType(DateTime64Unit(scalar), DateTimeTimezone(scalar, precisionParamCount: 1)),
            "Time" => new Time32Type(TimeUnit.Second),
            "Time64" => new Time64Type(Time64Unit(scalar)),
            "Decimal" or "Decimal32" or "Decimal64" or "Decimal128" or "Decimal256" => DecimalType(scalar),
            // Always-null type (e.g. SELECT NULL → Nullable(Nothing)).
            "Nothing" => NullType.Default,
            // JSON, Variant and Dynamic have no fixed Arrow shape; surface their canonical text form.
            "JSON" or "Variant" or "Dynamic" => StringType.Default,

            // Composite tier.
            "Array" => new ListType(FieldOf("item", scalar.TypeArguments[0])),
            "Map" => MapTypeFor(scalar),
            "Tuple" => new StructType(TupleFields(scalar.TypeArguments)),
            "Nested" => ListOf(new StructType(TupleFields(scalar.TypeArguments))),

            // Geo aliases — fixed List/List/List nestings over a Point struct (x, y doubles).
            "Point" => PointStruct(),
            "Ring" or "LineString" => ListOf(PointStruct()),
            "Polygon" or "MultiLineString" => ListOf(ListOf(PointStruct())),
            "MultiPolygon" => ListOf(ListOf(ListOf(PointStruct()))),

            _ => throw new NotSupportedException(
                $"ClickHouse type '{originalTypeName}' is not yet supported by the ADBC adapter."),
        };
    }

    /// <summary>Resolves a (possibly Nullable/LowCardinality-wrapped) ClickHouse type to an Arrow field.</summary>
    private static Field FieldOf(string name, ClickHouseType type)
    {
        var (scalar, nullable) = Unwrap(type);
        return new Field(name, ToArrowType(scalar, type.OriginalTypeName), nullable);
    }

    private static IArrowType ArrowTypeOf(ClickHouseType type)
    {
        var (scalar, _) = Unwrap(type);
        return ToArrowType(scalar, type.OriginalTypeName);
    }

    // ClickHouse tuples may be named or positional; Arrow needs field names, so use the 1-based
    // positional names ClickHouse itself assigns to unnamed elements.
    private static Field[] TupleFields(IReadOnlyList<ClickHouseType> args) =>
        args.Select((t, i) => FieldOf((i + 1).ToString(), t)).ToArray();

    private static ListType ListOf(IArrowType inner) => new(new Field("item", inner, nullable: false));

    private static StructType PointStruct() => new(new[]
    {
        new Field("x", DoubleType.Default, nullable: false),
        new Field("y", DoubleType.Default, nullable: false),
    });

    private static MapType MapTypeFor(ClickHouseType scalar)
    {
        // ClickHouse map keys are never nullable; values may be.
        var keyField = new Field("key", ArrowTypeOf(scalar.TypeArguments[0]), nullable: false);
        var valueField = FieldOf("value", scalar.TypeArguments[1]);
        return new MapType(keyField, valueField);
    }

    /// <summary>Arrow Time64 supports only microsecond/nanosecond units; map ClickHouse precision onto them.</summary>
    public static TimeUnit Time64Unit(ClickHouseType scalar)
    {
        int precision = scalar.Parameters.Count > 0 ? int.Parse(scalar.Parameters[0]) : 3;
        return precision <= 6 ? TimeUnit.Microsecond : TimeUnit.Nanosecond;
    }

    private static IArrowType DecimalType(ClickHouseType scalar)
    {
        var (precision, scale) = DecimalPrecisionScale(scalar);
        return precision <= 38
            ? new Decimal128Type(precision, scale)
            : new Decimal256Type(precision, scale);
    }

    public static (int Precision, int Scale) DecimalPrecisionScale(ClickHouseType scalar)
    {
        // Decimal(P, S) carries explicit precision and scale; the width-named forms carry only
        // scale, with precision fixed by the width.
        if (scalar.BaseName == "Decimal")
        {
            return (int.Parse(scalar.Parameters[0]), int.Parse(scalar.Parameters[1]));
        }

        int precision = scalar.BaseName switch
        {
            "Decimal32" => 9,
            "Decimal64" => 18,
            "Decimal128" => 38,
            "Decimal256" => 76,
            _ => throw new NotSupportedException($"Unexpected decimal type '{scalar.BaseName}'."),
        };
        int scale = scalar.Parameters.Count > 0 ? int.Parse(scalar.Parameters[0]) : 0;
        return (precision, scale);
    }

    public static TimeUnit DateTime64Unit(ClickHouseType scalar)
    {
        int precision = scalar.Parameters.Count > 0 ? int.Parse(scalar.Parameters[0]) : 3;
        return precision switch
        {
            0 => TimeUnit.Second,
            <= 3 => TimeUnit.Millisecond,
            <= 6 => TimeUnit.Microsecond,
            _ => TimeUnit.Nanosecond,
        };
    }

    private static string DateTimeTimezone(ClickHouseType scalar, int precisionParamCount)
    {
        // For DateTime the timezone (if any) is the first parameter; for DateTime64 it follows the
        // precision parameter. Parameters arrive quoted (e.g. 'UTC').
        if (scalar.Parameters.Count > precisionParamCount)
        {
            return StripQuotes(scalar.Parameters[precisionParamCount]);
        }

        return "UTC";
    }

    private static string StripQuotes(string value)
    {
        var trimmed = value.Trim();
        if (trimmed.Length >= 2 && trimmed[0] == '\'' && trimmed[^1] == '\'')
            return trimmed[1..^1];
        return trimmed;
    }
}
