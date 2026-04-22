using System.Buffers;
using System.Linq.Expressions;
using System.Net;
using System.Numerics;
using System.Reflection;
using CH.Native.Data;
using CH.Native.Data.Variant;
using CH.Native.Numerics;
using CH.Native.Protocol;

namespace CH.Native.BulkInsert;

/// <summary>
/// Factory for creating typed column extractors that avoid boxing.
/// </summary>
public static class ColumnExtractorFactory
{
    /// <summary>
    /// Creates a column extractor for the specified property and ClickHouse type.
    /// </summary>
    /// <typeparam name="TRow">The source row type.</typeparam>
    /// <param name="property">The property to extract.</param>
    /// <param name="columnName">The ClickHouse column name.</param>
    /// <param name="clickHouseType">The ClickHouse type name from the schema.</param>
    /// <returns>A column extractor that writes directly without boxing.</returns>
    public static IColumnExtractor<TRow> Create<TRow>(
        PropertyInfo property,
        string columnName,
        string clickHouseType)
    {
        var propertyType = property.PropertyType;
        var underlyingType = Nullable.GetUnderlyingType(propertyType);
        var isNullable = underlyingType != null;
        var baseType = underlyingType ?? propertyType;

        // Strip LowCardinality wrapper — client sends raw values with the inner type name,
        // and ClickHouse handles dictionary encoding server-side.
        // Must strip before Nullable check since valid nesting is LowCardinality(Nullable(X)).
        if (clickHouseType.StartsWith("LowCardinality(", StringComparison.Ordinal)
            && clickHouseType.EndsWith(')'))
        {
            clickHouseType = clickHouseType[15..^1];
        }

        // Check if the ClickHouse type is Nullable
        var isClickHouseNullable = clickHouseType.StartsWith("Nullable(", StringComparison.Ordinal);

        // Create the appropriate extractor based on the CLR type
        return baseType switch
        {
            // Integer types
            Type t when t == typeof(sbyte) => CreateNumericExtractor<TRow, sbyte>(property, columnName, clickHouseType, isNullable, isClickHouseNullable,
                static (ref ProtocolWriter w, sbyte v) => w.WriteByte((byte)v)),
            Type t when t == typeof(short) => CreateNumericExtractor<TRow, short>(property, columnName, clickHouseType, isNullable, isClickHouseNullable,
                static (ref ProtocolWriter w, short v) => w.WriteInt16(v)),
            Type t when t == typeof(int) => CreateNumericExtractor<TRow, int>(property, columnName, clickHouseType, isNullable, isClickHouseNullable,
                static (ref ProtocolWriter w, int v) => w.WriteInt32(v)),
            Type t when t == typeof(long) => CreateNumericExtractor<TRow, long>(property, columnName, clickHouseType, isNullable, isClickHouseNullable,
                static (ref ProtocolWriter w, long v) => w.WriteInt64(v)),
            Type t when t == typeof(byte) => CreateNumericExtractor<TRow, byte>(property, columnName, clickHouseType, isNullable, isClickHouseNullable,
                static (ref ProtocolWriter w, byte v) => w.WriteByte(v)),
            Type t when t == typeof(ushort) => CreateNumericExtractor<TRow, ushort>(property, columnName, clickHouseType, isNullable, isClickHouseNullable,
                static (ref ProtocolWriter w, ushort v) => w.WriteUInt16(v)),
            Type t when t == typeof(uint) => CreateNumericExtractor<TRow, uint>(property, columnName, clickHouseType, isNullable, isClickHouseNullable,
                static (ref ProtocolWriter w, uint v) => w.WriteUInt32(v)),
            Type t when t == typeof(ulong) => CreateNumericExtractor<TRow, ulong>(property, columnName, clickHouseType, isNullable, isClickHouseNullable,
                static (ref ProtocolWriter w, ulong v) => w.WriteUInt64(v)),

            // 128-bit integers
            Type t when t == typeof(Int128) => CreateNumericExtractor<TRow, Int128>(property, columnName, clickHouseType, isNullable, isClickHouseNullable,
                static (ref ProtocolWriter w, Int128 v) => w.WriteInt128(v)),
            Type t when t == typeof(UInt128) => CreateNumericExtractor<TRow, UInt128>(property, columnName, clickHouseType, isNullable, isClickHouseNullable,
                static (ref ProtocolWriter w, UInt128 v) => w.WriteUInt128(v)),

            // Floating point
            Type t when t == typeof(float) => CreateNumericExtractor<TRow, float>(property, columnName, clickHouseType, isNullable, isClickHouseNullable,
                static (ref ProtocolWriter w, float v) => w.WriteFloat32(v)),
            Type t when t == typeof(double) => CreateNumericExtractor<TRow, double>(property, columnName, clickHouseType, isNullable, isClickHouseNullable,
                static (ref ProtocolWriter w, double v) => w.WriteFloat64(v)),

            // Boolean
            Type t when t == typeof(bool) => CreateNumericExtractor<TRow, bool>(property, columnName, clickHouseType, isNullable, isClickHouseNullable,
                static (ref ProtocolWriter w, bool v) => w.WriteByte(v ? (byte)1 : (byte)0)),

            // String (reference type, slightly different handling)
            Type t when t == typeof(string) =>
                clickHouseType.Contains("FixedString(")
                    ? CreateFixedStringExtractor<TRow>(property, columnName, clickHouseType, isClickHouseNullable)
                    : CreateStringExtractor<TRow>(property, columnName, clickHouseType, isClickHouseNullable),

            // DateTime types
            Type t when t == typeof(DateTime) => CreateDateTimeExtractor<TRow>(property, columnName, clickHouseType, isNullable, isClickHouseNullable),
            Type t when t == typeof(DateOnly) => CreateDateOnlyExtractor<TRow>(property, columnName, clickHouseType, isNullable, isClickHouseNullable),
            Type t when t == typeof(TimeOnly) => CreateTimeOnlyExtractor<TRow>(property, columnName, clickHouseType, isNullable, isClickHouseNullable),
            Type t when t == typeof(DateTimeOffset) => CreateDateTimeOffsetExtractor<TRow>(property, columnName, clickHouseType, isNullable, isClickHouseNullable),

            // GUID/UUID
            Type t when t == typeof(Guid) => CreateGuidExtractor<TRow>(property, columnName, clickHouseType, isNullable, isClickHouseNullable),

            // Decimal
            Type t when t == typeof(decimal) => CreateDecimalExtractor<TRow>(property, columnName, clickHouseType, isNullable, isClickHouseNullable),

            // ClickHouseDecimal (full-precision Decimal128/256)
            Type t when t == typeof(ClickHouseDecimal) => CreateClickHouseDecimalExtractor<TRow>(property, columnName, clickHouseType, isNullable, isClickHouseNullable),

            // IP addresses
            Type t when t == typeof(IPAddress) => CreateIPAddressExtractor<TRow>(property, columnName, clickHouseType, isClickHouseNullable),

            // Variant — direct extractor that buckets per-arm and writes via the registered
            // inner column writer for each arm. Eliminates the reflection+boxing fallback that
            // otherwise fires for any POCO containing a ClickHouseVariant property.
            Type t when t == typeof(ClickHouseVariant) =>
                CreateVariantExtractor<TRow>(property, columnName, clickHouseType),

            // No direct extractor available for this type
            _ => throw new NotSupportedException(
                $"Direct extraction not supported for CLR type '{propertyType.Name}' " +
                $"(column '{columnName}', ClickHouse type '{clickHouseType}'). " +
                $"The BulkInserter will use the standard column writer path.")
        };
    }

    private delegate void WriteValueDelegate<T>(ref ProtocolWriter writer, T value);

    private static IColumnExtractor<TRow> CreateNumericExtractor<TRow, TValue>(
        PropertyInfo property,
        string columnName,
        string clickHouseType,
        bool isNullable,
        bool isClickHouseNullable,
        WriteValueDelegate<TValue> writeValue)
        where TValue : struct
    {
        if (isNullable)
        {
            var getter = CreateTypedGetter<TRow, TValue?>(property);
            return new NullableValueExtractor<TRow, TValue>(getter, columnName, clickHouseType, writeValue, isClickHouseNullable);
        }
        else
        {
            var getter = CreateTypedGetter<TRow, TValue>(property);
            return new ValueExtractor<TRow, TValue>(getter, columnName, clickHouseType, writeValue, isClickHouseNullable);
        }
    }

    private static IColumnExtractor<TRow> CreateStringExtractor<TRow>(
        PropertyInfo property,
        string columnName,
        string clickHouseType,
        bool isClickHouseNullable)
    {
        var getter = CreateTypedGetter<TRow, string?>(property);
        return new StringExtractor<TRow>(getter, columnName, clickHouseType, isClickHouseNullable);
    }

    private static IColumnExtractor<TRow> CreateFixedStringExtractor<TRow>(
        PropertyInfo property,
        string columnName,
        string clickHouseType,
        bool isClickHouseNullable)
    {
        var length = ExtractFixedStringLength(clickHouseType);
        var getter = CreateTypedGetter<TRow, string?>(property);
        return new FixedStringExtractor<TRow>(getter, columnName, clickHouseType, length, isClickHouseNullable);
    }

    private static int ExtractFixedStringLength(string clickHouseType)
    {
        var idx = clickHouseType.IndexOf("FixedString(", StringComparison.Ordinal);
        if (idx >= 0)
        {
            var start = idx + 12; // Length of "FixedString("
            var end = clickHouseType.IndexOf(')', start);
            if (end > start && int.TryParse(clickHouseType.AsSpan(start, end - start), out var length))
            {
                return length;
            }
        }

        throw new ArgumentException($"Cannot parse FixedString length from type '{clickHouseType}'.");
    }

    private static IColumnExtractor<TRow> CreateDateTimeExtractor<TRow>(
        PropertyInfo property,
        string columnName,
        string clickHouseType,
        bool isNullable,
        bool isClickHouseNullable)
    {
        // DateTime is stored as UInt32 Unix timestamp for DateTime, or Int64 for DateTime64
        var isDateTime64 = clickHouseType.StartsWith("DateTime64", StringComparison.Ordinal) ||
                           (isClickHouseNullable && clickHouseType.Contains("DateTime64"));

        if (isDateTime64)
        {
            // Extract precision from type like "DateTime64(3)" or "Nullable(DateTime64(3))"
            var precision = ExtractDateTime64Precision(clickHouseType);

            if (isNullable)
            {
                var getter = CreateTypedGetter<TRow, DateTime?>(property);
                return new NullableDateTime64Extractor<TRow>(getter, columnName, clickHouseType, precision, isClickHouseNullable);
            }
            else
            {
                var getter = CreateTypedGetter<TRow, DateTime>(property);
                return new DateTime64Extractor<TRow>(getter, columnName, clickHouseType, precision, isClickHouseNullable);
            }
        }
        else
        {
            if (isNullable)
            {
                var getter = CreateTypedGetter<TRow, DateTime?>(property);
                return new NullableDateTimeExtractor<TRow>(getter, columnName, clickHouseType, isClickHouseNullable);
            }
            else
            {
                var getter = CreateTypedGetter<TRow, DateTime>(property);
                return new DateTimeExtractor<TRow>(getter, columnName, clickHouseType, isClickHouseNullable);
            }
        }
    }

    private static int ExtractDateTime64Precision(string clickHouseType)
    {
        // Parse "DateTime64(3)" or "Nullable(DateTime64(3, 'UTC'))" to get precision
        var idx = clickHouseType.IndexOf("DateTime64(", StringComparison.Ordinal);
        if (idx >= 0)
        {
            var start = idx + 11; // Length of "DateTime64("
            var end = clickHouseType.IndexOfAny([',', ')'], start);
            if (end > start && int.TryParse(clickHouseType.AsSpan(start, end - start), out var precision))
            {
                return precision;
            }
        }
        return 3; // Default precision
    }

    private static IColumnExtractor<TRow> CreateDateOnlyExtractor<TRow>(
        PropertyInfo property,
        string columnName,
        string clickHouseType,
        bool isNullable,
        bool isClickHouseNullable)
    {
        // Date is stored as UInt16 (days since 1970-01-01) or Int32 for Date32
        var isDate32 = clickHouseType.Contains("Date32");

        if (isNullable)
        {
            var getter = CreateTypedGetter<TRow, DateOnly?>(property);
            return new NullableDateOnlyExtractor<TRow>(getter, columnName, clickHouseType, isDate32, isClickHouseNullable);
        }
        else
        {
            var getter = CreateTypedGetter<TRow, DateOnly>(property);
            return new DateOnlyExtractor<TRow>(getter, columnName, clickHouseType, isDate32, isClickHouseNullable);
        }
    }

    private static IColumnExtractor<TRow> CreateTimeOnlyExtractor<TRow>(
        PropertyInfo property,
        string columnName,
        string clickHouseType,
        bool isNullable,
        bool isClickHouseNullable)
    {
        // Time is Int32 seconds since midnight; Time64(N) is Int64 sub-seconds with precision N.
        var isTime64 = clickHouseType.StartsWith("Time64", StringComparison.Ordinal) ||
                       (isClickHouseNullable && clickHouseType.Contains("Time64"));

        if (isTime64)
        {
            var precision = ExtractTime64Precision(clickHouseType);

            if (isNullable)
            {
                var getter = CreateTypedGetter<TRow, TimeOnly?>(property);
                return new NullableTime64Extractor<TRow>(getter, columnName, clickHouseType, precision, isClickHouseNullable);
            }
            else
            {
                var getter = CreateTypedGetter<TRow, TimeOnly>(property);
                return new Time64Extractor<TRow>(getter, columnName, clickHouseType, precision, isClickHouseNullable);
            }
        }
        else
        {
            if (isNullable)
            {
                var getter = CreateTypedGetter<TRow, TimeOnly?>(property);
                return new NullableTimeExtractor<TRow>(getter, columnName, clickHouseType, isClickHouseNullable);
            }
            else
            {
                var getter = CreateTypedGetter<TRow, TimeOnly>(property);
                return new TimeExtractor<TRow>(getter, columnName, clickHouseType, isClickHouseNullable);
            }
        }
    }

    private static int ExtractTime64Precision(string clickHouseType)
    {
        var idx = clickHouseType.IndexOf("Time64(", StringComparison.Ordinal);
        if (idx >= 0)
        {
            var start = idx + 7; // Length of "Time64("
            var end = clickHouseType.IndexOfAny([',', ')'], start);
            if (end > start && int.TryParse(clickHouseType.AsSpan(start, end - start), out var precision))
            {
                return precision;
            }
        }
        return 3; // Default precision
    }

    private static IColumnExtractor<TRow> CreateDateTimeOffsetExtractor<TRow>(
        PropertyInfo property,
        string columnName,
        string clickHouseType,
        bool isNullable,
        bool isClickHouseNullable)
    {
        // DateTimeOffset maps to either DateTime (UInt32 seconds) or DateTime64 (Int64
        // scaled) depending on the declared column type. ExtractDateTime64Precision
        // returns a sentinel for the latter; for the former we emit UInt32 seconds.
        var isDateTime64 = clickHouseType.Contains("DateTime64", StringComparison.Ordinal);
        var precision = isDateTime64 ? ExtractDateTime64Precision(clickHouseType) : 0;

        if (isNullable)
        {
            var getter = CreateTypedGetter<TRow, DateTimeOffset?>(property);
            return new NullableDateTimeOffsetExtractor<TRow>(getter, columnName, clickHouseType, precision, isDateTime64, isClickHouseNullable);
        }
        else
        {
            var getter = CreateTypedGetter<TRow, DateTimeOffset>(property);
            return new DateTimeOffsetExtractor<TRow>(getter, columnName, clickHouseType, precision, isDateTime64, isClickHouseNullable);
        }
    }

    private static IColumnExtractor<TRow> CreateGuidExtractor<TRow>(
        PropertyInfo property,
        string columnName,
        string clickHouseType,
        bool isNullable,
        bool isClickHouseNullable)
    {
        if (isNullable)
        {
            var getter = CreateTypedGetter<TRow, Guid?>(property);
            return new NullableGuidExtractor<TRow>(getter, columnName, clickHouseType, isClickHouseNullable);
        }
        else
        {
            var getter = CreateTypedGetter<TRow, Guid>(property);
            return new GuidExtractor<TRow>(getter, columnName, clickHouseType, isClickHouseNullable);
        }
    }

    private static IColumnExtractor<TRow> CreateDecimalExtractor<TRow>(
        PropertyInfo property,
        string columnName,
        string clickHouseType,
        bool isNullable,
        bool isClickHouseNullable)
    {
        // Extract scale from ClickHouse type
        var scale = ExtractDecimalScale(clickHouseType);
        var precision = ExtractDecimalPrecision(clickHouseType);

        if (isNullable)
        {
            var getter = CreateTypedGetter<TRow, decimal?>(property);
            return new NullableDecimalExtractor<TRow>(getter, columnName, clickHouseType, scale, precision, isClickHouseNullable);
        }
        else
        {
            var getter = CreateTypedGetter<TRow, decimal>(property);
            return new DecimalExtractor<TRow>(getter, columnName, clickHouseType, scale, precision, isClickHouseNullable);
        }
    }

    private static int ExtractDecimalScale(string clickHouseType)
    {
        if (clickHouseType.StartsWith("Nullable(", StringComparison.Ordinal) && clickHouseType.EndsWith(')'))
            clickHouseType = clickHouseType[9..^1];

        // Parse "Decimal64(4)" or "Decimal(18, 4)" to get scale
        if (clickHouseType.Contains("Decimal(") || clickHouseType.Contains("Decimal32(") ||
            clickHouseType.Contains("Decimal64(") || clickHouseType.Contains("Decimal128(") ||
            clickHouseType.Contains("Decimal256("))
        {
            var openParen = clickHouseType.LastIndexOf('(');
            var closeParen = clickHouseType.LastIndexOf(')');
            if (openParen >= 0 && closeParen > openParen)
            {
                var content = clickHouseType.Substring(openParen + 1, closeParen - openParen - 1);
                var parts = content.Split(',');
                if (parts.Length == 2 && int.TryParse(parts[1].Trim(), out var scale))
                {
                    return scale;
                }
                else if (parts.Length == 1 && int.TryParse(parts[0].Trim(), out scale))
                {
                    return scale;
                }
            }
        }
        return 0;
    }

    private static int ExtractDecimalPrecision(string clickHouseType)
    {
        if (clickHouseType.StartsWith("Nullable(", StringComparison.Ordinal) && clickHouseType.EndsWith(')'))
            clickHouseType = clickHouseType[9..^1];

        if (clickHouseType.Contains("Decimal32")) return 9;
        if (clickHouseType.Contains("Decimal64")) return 18;
        if (clickHouseType.Contains("Decimal128")) return 38;
        if (clickHouseType.Contains("Decimal256")) return 76;

        // Generic Decimal(P, S)
        if (clickHouseType.Contains("Decimal("))
        {
            var openParen = clickHouseType.IndexOf('(');
            var closeParen = clickHouseType.IndexOf(')');
            if (openParen >= 0 && closeParen > openParen)
            {
                var content = clickHouseType.Substring(openParen + 1, closeParen - openParen - 1);
                var parts = content.Split(',');
                if (parts.Length >= 1 && int.TryParse(parts[0].Trim(), out var precision))
                {
                    return precision;
                }
            }
        }
        return 18; // Default
    }

    private static IColumnExtractor<TRow> CreateClickHouseDecimalExtractor<TRow>(
        PropertyInfo property,
        string columnName,
        string clickHouseType,
        bool isNullable,
        bool isClickHouseNullable)
    {
        var scale = ExtractDecimalScale(clickHouseType);
        var precision = ExtractDecimalPrecision(clickHouseType);

        if (isNullable)
        {
            var getter = CreateTypedGetter<TRow, ClickHouseDecimal?>(property);
            return new NullableClickHouseDecimalExtractor<TRow>(getter, columnName, clickHouseType, scale, precision, isClickHouseNullable);
        }
        else
        {
            var getter = CreateTypedGetter<TRow, ClickHouseDecimal>(property);
            return new ClickHouseDecimalExtractor<TRow>(getter, columnName, clickHouseType, scale, precision, isClickHouseNullable);
        }
    }

    private static IColumnExtractor<TRow> CreateIPAddressExtractor<TRow>(
        PropertyInfo property,
        string columnName,
        string clickHouseType,
        bool isClickHouseNullable)
    {
        var isIPv6 = clickHouseType.Contains("IPv6");
        var getter = CreateTypedGetter<TRow, IPAddress?>(property);
        return new IPAddressExtractor<TRow>(getter, columnName, clickHouseType, isIPv6, isClickHouseNullable);
    }

    private static IColumnExtractor<TRow> CreateVariantExtractor<TRow>(
        PropertyInfo property,
        string columnName,
        string clickHouseType)
    {
        // Resolve per-arm inner writers once at extractor construction time. We need
        // ColumnWriterRegistry.Default because BulkInserter doesn't expose its registry
        // through the column-extractor contract; the registry is process-shared anyway.
        var parsed = Data.Types.ClickHouseTypeParser.Parse(clickHouseType);
        if (!parsed.IsVariant)
            throw new NotSupportedException(
                $"Variant extractor requires a Variant(T1, T2, ...) type; got '{clickHouseType}'.");

        var armWriters = new IColumnWriter[parsed.TypeArguments.Count];
        var writerFactory = new ColumnWriterFactory(ColumnWriterRegistry.Default);
        for (int i = 0; i < parsed.TypeArguments.Count; i++)
            armWriters[i] = writerFactory.CreateWriter(parsed.TypeArguments[i].OriginalTypeName);

        var getter = CreateTypedGetter<TRow, ClickHouseVariant>(property);
        return new VariantExtractor<TRow>(getter, columnName, clickHouseType, armWriters);
    }

    private static Func<TRow, TValue> CreateTypedGetter<TRow, TValue>(PropertyInfo property)
    {
        var parameter = Expression.Parameter(typeof(TRow), "obj");
        var propertyAccess = Expression.Property(parameter, property);

        // Handle type conversion if needed
        Expression body;
        if (property.PropertyType == typeof(TValue))
        {
            body = propertyAccess;
        }
        else
        {
            body = Expression.Convert(propertyAccess, typeof(TValue));
        }

        var lambda = Expression.Lambda<Func<TRow, TValue>>(body, parameter);
        return lambda.Compile();
    }

    private static Func<TRow, object?> CreateBoxingGetter<TRow>(PropertyInfo property)
    {
        var parameter = Expression.Parameter(typeof(TRow), "obj");
        var propertyAccess = Expression.Property(parameter, property);
        var convert = Expression.Convert(propertyAccess, typeof(object));
        var lambda = Expression.Lambda<Func<TRow, object?>>(convert, parameter);
        return lambda.Compile();
    }

    #region Extractor Implementations

    private sealed class ValueExtractor<TRow, TValue> : IColumnExtractor<TRow>
        where TValue : struct
    {
        private readonly Func<TRow, TValue> _getter;
        private readonly WriteValueDelegate<TValue> _writeValue;
        private readonly bool _isClickHouseNullable;

        public string ColumnName { get; }
        public string TypeName { get; }

        public ValueExtractor(
            Func<TRow, TValue> getter,
            string columnName,
            string typeName,
            WriteValueDelegate<TValue> writeValue,
            bool isClickHouseNullable)
        {
            _getter = getter;
            ColumnName = columnName;
            TypeName = typeName;
            _writeValue = writeValue;
            _isClickHouseNullable = isClickHouseNullable;
        }

        public void ExtractAndWrite(ref ProtocolWriter writer, IReadOnlyList<TRow> rows, int rowCount)
        {
            if (_isClickHouseNullable)
            {
                // Write null bitmap (all zeros - no nulls)
                for (int i = 0; i < rowCount; i++)
                {
                    writer.WriteByte(0);
                }
            }

            // Write values directly - no boxing!
            for (int i = 0; i < rowCount; i++)
            {
                var value = _getter(rows[i]);
                _writeValue(ref writer, value);
            }
        }
    }

    private sealed class NullableValueExtractor<TRow, TValue> : IColumnExtractor<TRow>
        where TValue : struct
    {
        private readonly Func<TRow, TValue?> _getter;
        private readonly WriteValueDelegate<TValue> _writeValue;
        private readonly bool _isClickHouseNullable;

        public string ColumnName { get; }
        public string TypeName { get; }

        public NullableValueExtractor(
            Func<TRow, TValue?> getter,
            string columnName,
            string typeName,
            WriteValueDelegate<TValue> writeValue,
            bool isClickHouseNullable)
        {
            _getter = getter;
            ColumnName = columnName;
            TypeName = typeName;
            _writeValue = writeValue;
            _isClickHouseNullable = isClickHouseNullable;
        }

        public void ExtractAndWrite(ref ProtocolWriter writer, IReadOnlyList<TRow> rows, int rowCount)
        {
            if (_isClickHouseNullable)
            {
                // First pass: write null bitmap
                for (int i = 0; i < rowCount; i++)
                {
                    var value = _getter(rows[i]);
                    writer.WriteByte(value.HasValue ? (byte)0 : (byte)1);
                }

                // Second pass: write values (default for nulls)
                for (int i = 0; i < rowCount; i++)
                {
                    var value = _getter(rows[i]);
                    _writeValue(ref writer, value ?? default);
                }
            }
            else
            {
                // Non-nullable column, write values directly
                for (int i = 0; i < rowCount; i++)
                {
                    var value = _getter(rows[i]);
                    _writeValue(ref writer, value ?? default);
                }
            }
        }
    }

    private sealed class StringExtractor<TRow> : IColumnExtractor<TRow>
    {
        private readonly Func<TRow, string?> _getter;
        private readonly bool _isClickHouseNullable;

        public string ColumnName { get; }
        public string TypeName { get; }

        public StringExtractor(Func<TRow, string?> getter, string columnName, string typeName, bool isClickHouseNullable)
        {
            _getter = getter;
            ColumnName = columnName;
            TypeName = typeName;
            _isClickHouseNullable = isClickHouseNullable;
        }

        public void ExtractAndWrite(ref ProtocolWriter writer, IReadOnlyList<TRow> rows, int rowCount)
        {
            if (_isClickHouseNullable)
            {
                // Write null bitmap
                for (int i = 0; i < rowCount; i++)
                {
                    var value = _getter(rows[i]);
                    writer.WriteByte(value == null ? (byte)1 : (byte)0);
                }

                // Write values
                for (int i = 0; i < rowCount; i++)
                {
                    var value = _getter(rows[i]);
                    writer.WriteString(value ?? string.Empty);
                }
            }
            else
            {
                // Write values directly
                for (int i = 0; i < rowCount; i++)
                {
                    var value = _getter(rows[i]);
                    writer.WriteString(value ?? string.Empty);
                }
            }
        }
    }

    private sealed class FixedStringExtractor<TRow> : IColumnExtractor<TRow>
    {
        private readonly Func<TRow, string?> _getter;
        private readonly int _length;
        private readonly bool _isClickHouseNullable;

        public string ColumnName { get; }
        public string TypeName { get; }

        public FixedStringExtractor(Func<TRow, string?> getter, string columnName, string typeName, int length, bool isClickHouseNullable)
        {
            _getter = getter;
            ColumnName = columnName;
            TypeName = typeName;
            _length = length;
            _isClickHouseNullable = isClickHouseNullable;
        }

        public void ExtractAndWrite(ref ProtocolWriter writer, IReadOnlyList<TRow> rows, int rowCount)
        {
            if (_isClickHouseNullable)
            {
                // Write null bitmap
                for (int i = 0; i < rowCount; i++)
                {
                    var value = _getter(rows[i]);
                    writer.WriteByte(value == null ? (byte)1 : (byte)0);
                }

                // Write fixed-length values
                for (int i = 0; i < rowCount; i++)
                {
                    var value = _getter(rows[i]);
                    WriteFixedString(ref writer, value);
                }
            }
            else
            {
                for (int i = 0; i < rowCount; i++)
                {
                    var value = _getter(rows[i]);
                    WriteFixedString(ref writer, value);
                }
            }
        }

        private void WriteFixedString(ref ProtocolWriter writer, string? value)
        {
            Span<byte> buffer = stackalloc byte[_length];
            buffer.Clear();

            if (value != null)
            {
                System.Text.Encoding.UTF8.GetBytes(value.AsSpan(), buffer);
            }

            writer.WriteBytes(buffer);
        }
    }

    private sealed class DateTimeExtractor<TRow> : IColumnExtractor<TRow>
    {
        private readonly Func<TRow, DateTime> _getter;
        private readonly bool _isClickHouseNullable;
        private static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public string ColumnName { get; }
        public string TypeName { get; }

        public DateTimeExtractor(Func<TRow, DateTime> getter, string columnName, string typeName, bool isClickHouseNullable)
        {
            _getter = getter;
            ColumnName = columnName;
            TypeName = typeName;
            _isClickHouseNullable = isClickHouseNullable;
        }

        public void ExtractAndWrite(ref ProtocolWriter writer, IReadOnlyList<TRow> rows, int rowCount)
        {
            if (_isClickHouseNullable)
            {
                // Write null bitmap (all zeros)
                for (int i = 0; i < rowCount; i++)
                    writer.WriteByte(0);
            }

            for (int i = 0; i < rowCount; i++)
            {
                var value = _getter(rows[i]);
                var utcValue = value.Kind == DateTimeKind.Utc ? value : value.ToUniversalTime();
                var unixSeconds = (uint)((utcValue - UnixEpoch).TotalSeconds);
                writer.WriteUInt32(unixSeconds);
            }
        }
    }

    private sealed class NullableDateTimeExtractor<TRow> : IColumnExtractor<TRow>
    {
        private readonly Func<TRow, DateTime?> _getter;
        private readonly bool _isClickHouseNullable;
        private static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public string ColumnName { get; }
        public string TypeName { get; }

        public NullableDateTimeExtractor(Func<TRow, DateTime?> getter, string columnName, string typeName, bool isClickHouseNullable)
        {
            _getter = getter;
            ColumnName = columnName;
            TypeName = typeName;
            _isClickHouseNullable = isClickHouseNullable;
        }

        public void ExtractAndWrite(ref ProtocolWriter writer, IReadOnlyList<TRow> rows, int rowCount)
        {
            if (_isClickHouseNullable)
            {
                for (int i = 0; i < rowCount; i++)
                {
                    var value = _getter(rows[i]);
                    writer.WriteByte(value.HasValue ? (byte)0 : (byte)1);
                }
            }

            for (int i = 0; i < rowCount; i++)
            {
                var value = _getter(rows[i]);
                if (value.HasValue)
                {
                    var utcValue = value.Value.Kind == DateTimeKind.Utc ? value.Value : value.Value.ToUniversalTime();
                    var unixSeconds = (uint)((utcValue - UnixEpoch).TotalSeconds);
                    writer.WriteUInt32(unixSeconds);
                }
                else
                {
                    writer.WriteUInt32(0);
                }
            }
        }
    }

    private sealed class DateTime64Extractor<TRow> : IColumnExtractor<TRow>
    {
        private readonly Func<TRow, DateTime> _getter;
        private readonly int _precision;
        private readonly long _ticksMultiplier;
        private readonly bool _isClickHouseNullable;
        private static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public string ColumnName { get; }
        public string TypeName { get; }

        public DateTime64Extractor(Func<TRow, DateTime> getter, string columnName, string typeName, int precision, bool isClickHouseNullable)
        {
            _getter = getter;
            ColumnName = columnName;
            TypeName = typeName;
            _precision = precision;
            _ticksMultiplier = (long)Math.Pow(10, precision);
            _isClickHouseNullable = isClickHouseNullable;
        }

        public void ExtractAndWrite(ref ProtocolWriter writer, IReadOnlyList<TRow> rows, int rowCount)
        {
            if (_isClickHouseNullable)
            {
                for (int i = 0; i < rowCount; i++)
                    writer.WriteByte(0);
            }

            for (int i = 0; i < rowCount; i++)
            {
                var value = _getter(rows[i]);
                var utcValue = value.Kind == DateTimeKind.Utc ? value : value.ToUniversalTime();
                var totalSeconds = (utcValue - UnixEpoch).TotalSeconds;
                var scaledValue = (long)(totalSeconds * _ticksMultiplier);
                writer.WriteInt64(scaledValue);
            }
        }
    }

    private sealed class NullableDateTime64Extractor<TRow> : IColumnExtractor<TRow>
    {
        private readonly Func<TRow, DateTime?> _getter;
        private readonly int _precision;
        private readonly long _ticksMultiplier;
        private readonly bool _isClickHouseNullable;
        private static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public string ColumnName { get; }
        public string TypeName { get; }

        public NullableDateTime64Extractor(Func<TRow, DateTime?> getter, string columnName, string typeName, int precision, bool isClickHouseNullable)
        {
            _getter = getter;
            ColumnName = columnName;
            TypeName = typeName;
            _precision = precision;
            _ticksMultiplier = (long)Math.Pow(10, precision);
            _isClickHouseNullable = isClickHouseNullable;
        }

        public void ExtractAndWrite(ref ProtocolWriter writer, IReadOnlyList<TRow> rows, int rowCount)
        {
            if (_isClickHouseNullable)
            {
                for (int i = 0; i < rowCount; i++)
                {
                    var value = _getter(rows[i]);
                    writer.WriteByte(value.HasValue ? (byte)0 : (byte)1);
                }
            }

            for (int i = 0; i < rowCount; i++)
            {
                var value = _getter(rows[i]);
                if (value.HasValue)
                {
                    var utcValue = value.Value.Kind == DateTimeKind.Utc ? value.Value : value.Value.ToUniversalTime();
                    var totalSeconds = (utcValue - UnixEpoch).TotalSeconds;
                    var scaledValue = (long)(totalSeconds * _ticksMultiplier);
                    writer.WriteInt64(scaledValue);
                }
                else
                {
                    writer.WriteInt64(0);
                }
            }
        }
    }

    private sealed class DateOnlyExtractor<TRow> : IColumnExtractor<TRow>
    {
        private readonly Func<TRow, DateOnly> _getter;
        private readonly bool _isDate32;
        private readonly bool _isClickHouseNullable;
        private static readonly DateOnly UnixEpoch = new(1970, 1, 1);

        public string ColumnName { get; }
        public string TypeName { get; }

        public DateOnlyExtractor(Func<TRow, DateOnly> getter, string columnName, string typeName, bool isDate32, bool isClickHouseNullable)
        {
            _getter = getter;
            ColumnName = columnName;
            TypeName = typeName;
            _isDate32 = isDate32;
            _isClickHouseNullable = isClickHouseNullable;
        }

        public void ExtractAndWrite(ref ProtocolWriter writer, IReadOnlyList<TRow> rows, int rowCount)
        {
            if (_isClickHouseNullable)
            {
                for (int i = 0; i < rowCount; i++)
                    writer.WriteByte(0);
            }

            for (int i = 0; i < rowCount; i++)
            {
                var value = _getter(rows[i]);
                var days = value.DayNumber - UnixEpoch.DayNumber;
                if (_isDate32)
                {
                    writer.WriteInt32(days);
                }
                else
                {
                    writer.WriteUInt16((ushort)Math.Max(0, Math.Min(ushort.MaxValue, days)));
                }
            }
        }
    }

    private sealed class NullableDateOnlyExtractor<TRow> : IColumnExtractor<TRow>
    {
        private readonly Func<TRow, DateOnly?> _getter;
        private readonly bool _isDate32;
        private readonly bool _isClickHouseNullable;
        private static readonly DateOnly UnixEpoch = new(1970, 1, 1);

        public string ColumnName { get; }
        public string TypeName { get; }

        public NullableDateOnlyExtractor(Func<TRow, DateOnly?> getter, string columnName, string typeName, bool isDate32, bool isClickHouseNullable)
        {
            _getter = getter;
            ColumnName = columnName;
            TypeName = typeName;
            _isDate32 = isDate32;
            _isClickHouseNullable = isClickHouseNullable;
        }

        public void ExtractAndWrite(ref ProtocolWriter writer, IReadOnlyList<TRow> rows, int rowCount)
        {
            if (_isClickHouseNullable)
            {
                for (int i = 0; i < rowCount; i++)
                {
                    var value = _getter(rows[i]);
                    writer.WriteByte(value.HasValue ? (byte)0 : (byte)1);
                }
            }

            for (int i = 0; i < rowCount; i++)
            {
                var value = _getter(rows[i]);
                if (value.HasValue)
                {
                    var days = value.Value.DayNumber - UnixEpoch.DayNumber;
                    if (_isDate32)
                    {
                        writer.WriteInt32(days);
                    }
                    else
                    {
                        writer.WriteUInt16((ushort)Math.Max(0, Math.Min(ushort.MaxValue, days)));
                    }
                }
                else
                {
                    if (_isDate32)
                        writer.WriteInt32(0);
                    else
                        writer.WriteUInt16(0);
                }
            }
        }
    }

    private sealed class TimeExtractor<TRow> : IColumnExtractor<TRow>
    {
        private readonly Func<TRow, TimeOnly> _getter;
        private readonly bool _isClickHouseNullable;

        public string ColumnName { get; }
        public string TypeName { get; }

        public TimeExtractor(Func<TRow, TimeOnly> getter, string columnName, string typeName, bool isClickHouseNullable)
        {
            _getter = getter;
            ColumnName = columnName;
            TypeName = typeName;
            _isClickHouseNullable = isClickHouseNullable;
        }

        public void ExtractAndWrite(ref ProtocolWriter writer, IReadOnlyList<TRow> rows, int rowCount)
        {
            if (_isClickHouseNullable)
            {
                for (int i = 0; i < rowCount; i++)
                    writer.WriteByte(0);
            }

            for (int i = 0; i < rowCount; i++)
            {
                var value = _getter(rows[i]);
                var seconds = (int)(value.Ticks / TimeSpan.TicksPerSecond);
                writer.WriteInt32(seconds);
            }
        }
    }

    private sealed class NullableTimeExtractor<TRow> : IColumnExtractor<TRow>
    {
        private readonly Func<TRow, TimeOnly?> _getter;
        private readonly bool _isClickHouseNullable;

        public string ColumnName { get; }
        public string TypeName { get; }

        public NullableTimeExtractor(Func<TRow, TimeOnly?> getter, string columnName, string typeName, bool isClickHouseNullable)
        {
            _getter = getter;
            ColumnName = columnName;
            TypeName = typeName;
            _isClickHouseNullable = isClickHouseNullable;
        }

        public void ExtractAndWrite(ref ProtocolWriter writer, IReadOnlyList<TRow> rows, int rowCount)
        {
            if (_isClickHouseNullable)
            {
                for (int i = 0; i < rowCount; i++)
                {
                    var value = _getter(rows[i]);
                    writer.WriteByte(value.HasValue ? (byte)0 : (byte)1);
                }
            }

            for (int i = 0; i < rowCount; i++)
            {
                var value = _getter(rows[i]);
                if (value.HasValue)
                {
                    var seconds = (int)(value.Value.Ticks / TimeSpan.TicksPerSecond);
                    writer.WriteInt32(seconds);
                }
                else
                {
                    writer.WriteInt32(0);
                }
            }
        }
    }

    private sealed class Time64Extractor<TRow> : IColumnExtractor<TRow>
    {
        private readonly Func<TRow, TimeOnly> _getter;
        private readonly int _precision;
        private readonly long _ticksPerUnit;
        private readonly long _highPrecisionMultiplier;
        private readonly bool _isClickHouseNullable;

        public string ColumnName { get; }
        public string TypeName { get; }

        public Time64Extractor(Func<TRow, TimeOnly> getter, string columnName, string typeName, int precision, bool isClickHouseNullable)
        {
            _getter = getter;
            ColumnName = columnName;
            TypeName = typeName;
            _precision = precision;
            var unitsPerSecond = (long)Math.Pow(10, precision);
            _ticksPerUnit = precision <= 7 ? TimeSpan.TicksPerSecond / unitsPerSecond : 0;
            _highPrecisionMultiplier = precision > 7 ? (long)Math.Pow(10, precision - 7) : 0;
            _isClickHouseNullable = isClickHouseNullable;
        }

        public void ExtractAndWrite(ref ProtocolWriter writer, IReadOnlyList<TRow> rows, int rowCount)
        {
            if (_isClickHouseNullable)
            {
                for (int i = 0; i < rowCount; i++)
                    writer.WriteByte(0);
            }

            for (int i = 0; i < rowCount; i++)
            {
                writer.WriteInt64(EncodeTimeOnly(_getter(rows[i])));
            }
        }

        private long EncodeTimeOnly(TimeOnly value)
        {
            return _precision > 7
                ? value.Ticks * _highPrecisionMultiplier
                : value.Ticks / _ticksPerUnit;
        }
    }

    private sealed class NullableTime64Extractor<TRow> : IColumnExtractor<TRow>
    {
        private readonly Func<TRow, TimeOnly?> _getter;
        private readonly int _precision;
        private readonly long _ticksPerUnit;
        private readonly long _highPrecisionMultiplier;
        private readonly bool _isClickHouseNullable;

        public string ColumnName { get; }
        public string TypeName { get; }

        public NullableTime64Extractor(Func<TRow, TimeOnly?> getter, string columnName, string typeName, int precision, bool isClickHouseNullable)
        {
            _getter = getter;
            ColumnName = columnName;
            TypeName = typeName;
            _precision = precision;
            var unitsPerSecond = (long)Math.Pow(10, precision);
            _ticksPerUnit = precision <= 7 ? TimeSpan.TicksPerSecond / unitsPerSecond : 0;
            _highPrecisionMultiplier = precision > 7 ? (long)Math.Pow(10, precision - 7) : 0;
            _isClickHouseNullable = isClickHouseNullable;
        }

        public void ExtractAndWrite(ref ProtocolWriter writer, IReadOnlyList<TRow> rows, int rowCount)
        {
            if (_isClickHouseNullable)
            {
                for (int i = 0; i < rowCount; i++)
                {
                    var value = _getter(rows[i]);
                    writer.WriteByte(value.HasValue ? (byte)0 : (byte)1);
                }
            }

            for (int i = 0; i < rowCount; i++)
            {
                var value = _getter(rows[i]);
                writer.WriteInt64(value.HasValue ? Encode(value.Value) : 0);
            }
        }

        private long Encode(TimeOnly value)
        {
            return _precision > 7
                ? value.Ticks * _highPrecisionMultiplier
                : value.Ticks / _ticksPerUnit;
        }
    }

    private sealed class DateTimeOffsetExtractor<TRow> : IColumnExtractor<TRow>
    {
        private readonly Func<TRow, DateTimeOffset> _getter;
        private readonly long _ticksMultiplier;
        private readonly bool _isDateTime64;
        private readonly bool _isClickHouseNullable;
        private static readonly DateTimeOffset UnixEpoch = new(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

        public string ColumnName { get; }
        public string TypeName { get; }

        public DateTimeOffsetExtractor(Func<TRow, DateTimeOffset> getter, string columnName, string typeName, int precision, bool isDateTime64, bool isClickHouseNullable)
        {
            _getter = getter;
            ColumnName = columnName;
            TypeName = typeName;
            _ticksMultiplier = (long)Math.Pow(10, precision);
            _isDateTime64 = isDateTime64;
            _isClickHouseNullable = isClickHouseNullable;
        }

        public void ExtractAndWrite(ref ProtocolWriter writer, IReadOnlyList<TRow> rows, int rowCount)
        {
            if (_isClickHouseNullable)
            {
                for (int i = 0; i < rowCount; i++)
                    writer.WriteByte(0);
            }

            for (int i = 0; i < rowCount; i++)
            {
                var value = _getter(rows[i]);
                var totalSeconds = (value.UtcDateTime - UnixEpoch.UtcDateTime).TotalSeconds;
                if (_isDateTime64)
                {
                    writer.WriteInt64((long)(totalSeconds * _ticksMultiplier));
                }
                else
                {
                    writer.WriteUInt32((uint)Math.Max(0, Math.Min(totalSeconds, uint.MaxValue)));
                }
            }
        }
    }

    private sealed class NullableDateTimeOffsetExtractor<TRow> : IColumnExtractor<TRow>
    {
        private readonly Func<TRow, DateTimeOffset?> _getter;
        private readonly long _ticksMultiplier;
        private readonly bool _isDateTime64;
        private readonly bool _isClickHouseNullable;
        private static readonly DateTimeOffset UnixEpoch = new(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

        public string ColumnName { get; }
        public string TypeName { get; }

        public NullableDateTimeOffsetExtractor(Func<TRow, DateTimeOffset?> getter, string columnName, string typeName, int precision, bool isDateTime64, bool isClickHouseNullable)
        {
            _getter = getter;
            ColumnName = columnName;
            TypeName = typeName;
            _ticksMultiplier = (long)Math.Pow(10, precision);
            _isDateTime64 = isDateTime64;
            _isClickHouseNullable = isClickHouseNullable;
        }

        public void ExtractAndWrite(ref ProtocolWriter writer, IReadOnlyList<TRow> rows, int rowCount)
        {
            if (_isClickHouseNullable)
            {
                for (int i = 0; i < rowCount; i++)
                {
                    var value = _getter(rows[i]);
                    writer.WriteByte(value.HasValue ? (byte)0 : (byte)1);
                }
            }

            for (int i = 0; i < rowCount; i++)
            {
                var value = _getter(rows[i]);
                if (_isDateTime64)
                {
                    if (value.HasValue)
                    {
                        var totalSeconds = (value.Value.UtcDateTime - UnixEpoch.UtcDateTime).TotalSeconds;
                        writer.WriteInt64((long)(totalSeconds * _ticksMultiplier));
                    }
                    else
                    {
                        writer.WriteInt64(0);
                    }
                }
                else
                {
                    if (value.HasValue)
                    {
                        var totalSeconds = (value.Value.UtcDateTime - UnixEpoch.UtcDateTime).TotalSeconds;
                        writer.WriteUInt32((uint)Math.Max(0, Math.Min(totalSeconds, uint.MaxValue)));
                    }
                    else
                    {
                        writer.WriteUInt32(0);
                    }
                }
            }
        }
    }

    private sealed class GuidExtractor<TRow> : IColumnExtractor<TRow>
    {
        private readonly Func<TRow, Guid> _getter;
        private readonly bool _isClickHouseNullable;

        public string ColumnName { get; }
        public string TypeName { get; }

        public GuidExtractor(Func<TRow, Guid> getter, string columnName, string typeName, bool isClickHouseNullable)
        {
            _getter = getter;
            ColumnName = columnName;
            TypeName = typeName;
            _isClickHouseNullable = isClickHouseNullable;
        }

        public void ExtractAndWrite(ref ProtocolWriter writer, IReadOnlyList<TRow> rows, int rowCount)
        {
            if (_isClickHouseNullable)
            {
                for (int i = 0; i < rowCount; i++)
                    writer.WriteByte(0);
            }

            // Matches UuidColumnWriter/UuidColumnReader: ClickHouse wire format is each
            // 8-byte half reversed, with the first half's three LE fields byte-swapped.
            Span<byte> buffer = stackalloc byte[16];
            for (int i = 0; i < rowCount; i++)
            {
                var value = _getter(rows[i]);
                value.TryWriteBytes(buffer);
                writer.WriteByte(buffer[6]); writer.WriteByte(buffer[7]);
                writer.WriteByte(buffer[4]); writer.WriteByte(buffer[5]);
                writer.WriteByte(buffer[0]); writer.WriteByte(buffer[1]);
                writer.WriteByte(buffer[2]); writer.WriteByte(buffer[3]);
                writer.WriteByte(buffer[15]); writer.WriteByte(buffer[14]);
                writer.WriteByte(buffer[13]); writer.WriteByte(buffer[12]);
                writer.WriteByte(buffer[11]); writer.WriteByte(buffer[10]);
                writer.WriteByte(buffer[9]); writer.WriteByte(buffer[8]);
            }
        }
    }

    private sealed class NullableGuidExtractor<TRow> : IColumnExtractor<TRow>
    {
        private readonly Func<TRow, Guid?> _getter;
        private readonly bool _isClickHouseNullable;

        public string ColumnName { get; }
        public string TypeName { get; }

        public NullableGuidExtractor(Func<TRow, Guid?> getter, string columnName, string typeName, bool isClickHouseNullable)
        {
            _getter = getter;
            ColumnName = columnName;
            TypeName = typeName;
            _isClickHouseNullable = isClickHouseNullable;
        }

        public void ExtractAndWrite(ref ProtocolWriter writer, IReadOnlyList<TRow> rows, int rowCount)
        {
            if (_isClickHouseNullable)
            {
                for (int i = 0; i < rowCount; i++)
                {
                    var value = _getter(rows[i]);
                    writer.WriteByte(value.HasValue ? (byte)0 : (byte)1);
                }
            }

            Span<byte> buffer = stackalloc byte[16];
            for (int i = 0; i < rowCount; i++)
            {
                var value = _getter(rows[i]);
                var guid = value ?? Guid.Empty;
                guid.TryWriteBytes(buffer);
                writer.WriteByte(buffer[6]); writer.WriteByte(buffer[7]);
                writer.WriteByte(buffer[4]); writer.WriteByte(buffer[5]);
                writer.WriteByte(buffer[0]); writer.WriteByte(buffer[1]);
                writer.WriteByte(buffer[2]); writer.WriteByte(buffer[3]);
                writer.WriteByte(buffer[15]); writer.WriteByte(buffer[14]);
                writer.WriteByte(buffer[13]); writer.WriteByte(buffer[12]);
                writer.WriteByte(buffer[11]); writer.WriteByte(buffer[10]);
                writer.WriteByte(buffer[9]); writer.WriteByte(buffer[8]);
            }
        }
    }

    private sealed class DecimalExtractor<TRow> : IColumnExtractor<TRow>
    {
        private readonly Func<TRow, decimal> _getter;
        private readonly int _scale;
        private readonly int _precision;
        private readonly bool _isClickHouseNullable;

        public string ColumnName { get; }
        public string TypeName { get; }

        public DecimalExtractor(Func<TRow, decimal> getter, string columnName, string typeName, int scale, int precision, bool isClickHouseNullable)
        {
            _getter = getter;
            ColumnName = columnName;
            TypeName = typeName;
            _scale = scale;
            _precision = precision;
            _isClickHouseNullable = isClickHouseNullable;
        }

        public void ExtractAndWrite(ref ProtocolWriter writer, IReadOnlyList<TRow> rows, int rowCount)
        {
            if (_isClickHouseNullable)
            {
                for (int i = 0; i < rowCount; i++)
                    writer.WriteByte(0);
            }

            var multiplier = (decimal)Math.Pow(10, _scale);

            for (int i = 0; i < rowCount; i++)
            {
                var value = _getter(rows[i]);
                var scaled = value * multiplier;

                if (_precision <= 9)
                {
                    writer.WriteInt32((int)scaled);
                }
                else if (_precision <= 18)
                {
                    writer.WriteInt64((long)scaled);
                }
                else if (_precision <= 38)
                {
                    // Use allocation-free decimal to Int128 conversion
                    writer.WriteDecimalAsInt128(scaled);
                }
                else
                {
                    // Use allocation-free decimal to Int256 conversion
                    writer.WriteDecimalAsInt256(scaled);
                }
            }
        }
    }

    private sealed class NullableDecimalExtractor<TRow> : IColumnExtractor<TRow>
    {
        private readonly Func<TRow, decimal?> _getter;
        private readonly int _scale;
        private readonly int _precision;
        private readonly bool _isClickHouseNullable;

        public string ColumnName { get; }
        public string TypeName { get; }

        public NullableDecimalExtractor(Func<TRow, decimal?> getter, string columnName, string typeName, int scale, int precision, bool isClickHouseNullable)
        {
            _getter = getter;
            ColumnName = columnName;
            TypeName = typeName;
            _scale = scale;
            _precision = precision;
            _isClickHouseNullable = isClickHouseNullable;
        }

        public void ExtractAndWrite(ref ProtocolWriter writer, IReadOnlyList<TRow> rows, int rowCount)
        {
            if (_isClickHouseNullable)
            {
                for (int i = 0; i < rowCount; i++)
                {
                    var value = _getter(rows[i]);
                    writer.WriteByte(value.HasValue ? (byte)0 : (byte)1);
                }
            }

            var multiplier = (decimal)Math.Pow(10, _scale);

            for (int i = 0; i < rowCount; i++)
            {
                var value = _getter(rows[i]);
                var scaled = (value ?? 0) * multiplier;

                if (_precision <= 9)
                {
                    writer.WriteInt32((int)scaled);
                }
                else if (_precision <= 18)
                {
                    writer.WriteInt64((long)scaled);
                }
                else if (_precision <= 38)
                {
                    // Use allocation-free decimal to Int128 conversion
                    writer.WriteDecimalAsInt128(scaled);
                }
                else
                {
                    // Use allocation-free decimal to Int256 conversion
                    writer.WriteDecimalAsInt256(scaled);
                }
            }
        }
    }

    private sealed class ClickHouseDecimalExtractor<TRow> : IColumnExtractor<TRow>
    {
        private readonly Func<TRow, ClickHouseDecimal> _getter;
        private readonly int _scale;
        private readonly int _precision;
        private readonly bool _isClickHouseNullable;

        public string ColumnName { get; }
        public string TypeName { get; }

        public ClickHouseDecimalExtractor(Func<TRow, ClickHouseDecimal> getter, string columnName, string typeName, int scale, int precision, bool isClickHouseNullable)
        {
            _getter = getter;
            ColumnName = columnName;
            TypeName = typeName;
            _scale = scale;
            _precision = precision;
            _isClickHouseNullable = isClickHouseNullable;
        }

        public void ExtractAndWrite(ref ProtocolWriter writer, IReadOnlyList<TRow> rows, int rowCount)
        {
            if (_isClickHouseNullable)
            {
                for (int i = 0; i < rowCount; i++)
                    writer.WriteByte(0);
            }

            for (int i = 0; i < rowCount; i++)
            {
                var value = _getter(rows[i]);
                var scaled = RescaleMantissa(value, _scale);

                if (_precision <= 38)
                {
                    writer.WriteInt128((Int128)scaled);
                }
                else
                {
                    writer.WriteInt256(scaled);
                }
            }
        }

        private static BigInteger RescaleMantissa(ClickHouseDecimal value, int targetScale)
        {
            var mantissa = value.Mantissa;
            if (value.Scale == targetScale) return mantissa;
            if (value.Scale < targetScale)
                return mantissa * BigInteger.Pow(10, targetScale - value.Scale);
            return BigInteger.Divide(mantissa, BigInteger.Pow(10, value.Scale - targetScale));
        }
    }

    private sealed class NullableClickHouseDecimalExtractor<TRow> : IColumnExtractor<TRow>
    {
        private readonly Func<TRow, ClickHouseDecimal?> _getter;
        private readonly int _scale;
        private readonly int _precision;
        private readonly bool _isClickHouseNullable;

        public string ColumnName { get; }
        public string TypeName { get; }

        public NullableClickHouseDecimalExtractor(Func<TRow, ClickHouseDecimal?> getter, string columnName, string typeName, int scale, int precision, bool isClickHouseNullable)
        {
            _getter = getter;
            ColumnName = columnName;
            TypeName = typeName;
            _scale = scale;
            _precision = precision;
            _isClickHouseNullable = isClickHouseNullable;
        }

        public void ExtractAndWrite(ref ProtocolWriter writer, IReadOnlyList<TRow> rows, int rowCount)
        {
            if (_isClickHouseNullable)
            {
                for (int i = 0; i < rowCount; i++)
                {
                    var value = _getter(rows[i]);
                    writer.WriteByte(value.HasValue ? (byte)0 : (byte)1);
                }
            }

            for (int i = 0; i < rowCount; i++)
            {
                var value = _getter(rows[i]) ?? ClickHouseDecimal.Zero;
                var scaled = RescaleMantissa(value, _scale);

                if (_precision <= 38)
                {
                    writer.WriteInt128((Int128)scaled);
                }
                else
                {
                    writer.WriteInt256(scaled);
                }
            }
        }

        private static BigInteger RescaleMantissa(ClickHouseDecimal value, int targetScale)
        {
            var mantissa = value.Mantissa;
            if (value.Scale == targetScale) return mantissa;
            if (value.Scale < targetScale)
                return mantissa * BigInteger.Pow(10, targetScale - value.Scale);
            return BigInteger.Divide(mantissa, BigInteger.Pow(10, value.Scale - targetScale));
        }
    }

    private sealed class IPAddressExtractor<TRow> : IColumnExtractor<TRow>
    {
        private readonly Func<TRow, IPAddress?> _getter;
        private readonly bool _isIPv6;
        private readonly bool _isClickHouseNullable;

        public string ColumnName { get; }
        public string TypeName { get; }

        public IPAddressExtractor(Func<TRow, IPAddress?> getter, string columnName, string typeName, bool isIPv6, bool isClickHouseNullable)
        {
            _getter = getter;
            ColumnName = columnName;
            TypeName = typeName;
            _isIPv6 = isIPv6;
            _isClickHouseNullable = isClickHouseNullable;
        }

        public void ExtractAndWrite(ref ProtocolWriter writer, IReadOnlyList<TRow> rows, int rowCount)
        {
            if (_isClickHouseNullable)
            {
                for (int i = 0; i < rowCount; i++)
                {
                    var value = _getter(rows[i]);
                    writer.WriteByte(value == null ? (byte)1 : (byte)0);
                }
            }

            // Use stackalloc to avoid heap allocation
            Span<byte> buffer = stackalloc byte[16];
            for (int i = 0; i < rowCount; i++)
            {
                var value = _getter(rows[i]);
                if (_isIPv6)
                {
                    if (value != null)
                    {
                        value.TryWriteBytes(buffer, out _);
                    }
                    else
                    {
                        buffer.Clear();
                    }
                    writer.WriteBytes(buffer);
                }
                else
                {
                    if (value != null)
                    {
                        value.TryWriteBytes(buffer, out _);
                        buffer[0..4].Reverse();
                        writer.WriteBytes(buffer[0..4]);
                    }
                    else
                    {
                        writer.WriteUInt32(0);
                    }
                }
            }
        }
    }

    #endregion

    /// <summary>
    /// Extractor for Variant(T1, T2, ...) columns. Buckets rows per-arm and delegates each
    /// arm's packed write to the registered inner column writer. Avoids the boxed-column
    /// fallback path so POCOs containing <see cref="ClickHouseVariant"/> properties can be
    /// bulk-inserted without reflection-boxing every value.
    /// </summary>
    private sealed class VariantExtractor<TRow> : IColumnExtractor<TRow>
    {
        private const ulong DiscriminatorVersion0 = 0;

        private readonly Func<TRow, ClickHouseVariant> _getter;
        private readonly IColumnWriter[] _armWriters;

        public string ColumnName { get; }
        public string TypeName { get; }

        public VariantExtractor(
            Func<TRow, ClickHouseVariant> getter,
            string columnName,
            string typeName,
            IColumnWriter[] armWriters)
        {
            _getter = getter;
            ColumnName = columnName;
            TypeName = typeName;
            _armWriters = armWriters;
        }

        public void ExtractAndWrite(ref ProtocolWriter writer, IReadOnlyList<TRow> rows, int rowCount)
        {
            writer.WriteUInt64(DiscriminatorVersion0);

            if (rowCount == 0)
                return;

            var armCount = _armWriters.Length;

            // First pass: collect discriminator bytes into a pooled buffer, tallying arm counts.
            var counts = armCount <= 32 ? stackalloc int[armCount] : new int[armCount];
            var discPooled = ArrayPool<byte>.Shared.Rent(rowCount);
            var discSpan = discPooled.AsSpan(0, rowCount);
            for (int i = 0; i < rowCount; i++)
            {
                var v = _getter(rows[i]);
                var disc = v.Discriminator;
                discSpan[i] = disc;
                if (disc == ClickHouseVariant.NullDiscriminator) continue;
                if (disc >= armCount)
                    throw new ArgumentOutOfRangeException(
                        nameof(rows),
                        $"Row {i} has Variant discriminator {disc} but column declares only {armCount} arms.");
                counts[disc]++;
            }
            writer.WriteBytes(discSpan);

            // Second pass: per arm, materialise a bucket of exact length and delegate.
            // The buckets can't be pooled because IColumnWriter iterates values.Length —
            // they are the only irreducible allocation on this hot path.
            var buckets = ArrayPool<object?[]>.Shared.Rent(armCount);
            try
            {
                for (int arm = 0; arm < armCount; arm++)
                    buckets[arm] = counts[arm] == 0 ? Array.Empty<object?>() : new object?[counts[arm]];

                var cursors = armCount <= 32 ? stackalloc int[armCount] : new int[armCount];
                for (int i = 0; i < rowCount; i++)
                {
                    var disc = discSpan[i];
                    if (disc == ClickHouseVariant.NullDiscriminator) continue;
                    buckets[disc][cursors[disc]++] = _getter(rows[i]).Value;
                }

                for (int arm = 0; arm < armCount; arm++)
                    _armWriters[arm].WriteColumn(ref writer, buckets[arm]);
            }
            finally
            {
                ArrayPool<object?[]>.Shared.Return(buckets, clearArray: true);
                ArrayPool<byte>.Shared.Return(discPooled);
            }
        }
    }
}
