using System.Collections;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using CH.Native.Data;
using CH.Native.Data.Geo;
using CH.Native.Data.Types;
using CH.Native.Numerics;

namespace CH.Native.Adbc;

/// <summary>
/// Converts a ClickHouse <see cref="TypedBlock"/> into an Arrow <see cref="RecordBatch"/>.
/// Values are copied into Arrow-owned buffers so the batch outlives the (pooled) source block.
/// </summary>
/// <remarks>
/// The scalar tier uses the boxing <see cref="ITypedColumn.GetValue"/> accessor for clarity and to
/// handle nullable and non-nullable storage uniformly; span-based fast paths for hot primitive types
/// are a planned performance follow-up. Null handling is driven by <see cref="ITypedColumn.IsNull"/>.
/// </remarks>
internal static class BlockRecordBatchConverter
{
    public static RecordBatch ToRecordBatch(TypedBlock block, Schema schema)
    {
        int rowCount = block.RowCount;
        var arrays = new IArrowArray[block.ColumnCount];
        for (int c = 0; c < block.ColumnCount; c++)
        {
            var parsed = ClickHouseTypeParser.Parse(block.ColumnTypes[c]);
            // Build against the schema field's Arrow type so the array's DataType (timezone, unit,
            // precision, scale) is identical to the schema's — never reconstruct it independently.
            arrays[c] = BuildArray(block.Columns[c], parsed, schema.GetFieldByIndex(c).DataType, rowCount);
        }

        return new RecordBatch(schema, arrays, rowCount);
    }

    /// <summary>
    /// Recursively builds an Arrow array from a column of ClickHouse values. <paramref name="type"/>
    /// is the full (possibly Nullable/LowCardinality-wrapped) ClickHouse type; nested composite levels
    /// re-enter here through a <see cref="ListColumn"/> view over the flattened child values.
    /// </summary>
    private static IArrowArray BuildArray(ITypedColumn col, ClickHouseType type, IArrowType arrowType, int n)
    {
        var (scalar, _) = ArrowSchemaMapper.Unwrap(type);

        // Interval is named per unit (IntervalSecond, IntervalDay, …); render its (count, unit) as text.
        if (scalar.BaseName.StartsWith("Interval", StringComparison.Ordinal))
            return BuildString(col, n, static v => v.ToString()!);

        return scalar.BaseName switch
        {
            "Int8" or "Enum8" => BuildInt8(col, n),
            "Int16" or "Enum16" => BuildInt16(col, n),
            "Int32" => BuildInt32(col, n),
            "Int64" => BuildInt64(col, n),
            "UInt8" => BuildUInt8(col, n),
            "UInt16" => BuildUInt16(col, n),
            "UInt32" => BuildUInt32(col, n),
            "UInt64" => BuildUInt64(col, n),
            // Wide integers (Int128/256, UInt128/256) surface as exact decimal text — see ArrowSchemaMapper.
            "Int128" or "Int256" or "UInt128" or "UInt256" => BuildString(col, n, FormatInvariant),
            "Float32" or "BFloat16" => BuildFloat(col, n),
            "Float64" => BuildDouble(col, n),
            "Bool" => BuildBool(col, n),
            "String" => BuildStringColumn(col, n),
            "UUID" or "IPv4" or "IPv6" => BuildString(col, n, static v => v.ToString()!),
            "FixedString" => BuildBinary(col, n),
            "Date" or "Date32" => BuildDate32(col, n),
            "DateTime" or "DateTime64" => BuildTimestamp(col, n, (TimestampType)arrowType),
            "Time" => BuildTime32(col, n, (Time32Type)arrowType),
            "Time64" => BuildTime64(col, n, (Time64Type)arrowType),
            "Decimal" or "Decimal32" or "Decimal64" or "Decimal128" or "Decimal256" => BuildDecimal(col, arrowType, n),
            "Nothing" => new NullArray(n),
            "JSON" => BuildString(col, n, static v => ((JsonDocument)v).RootElement.GetRawText()),
            "Variant" or "Dynamic" => BuildString(col, n, static v => v.ToString()!),

            // Composite tier — recurse through the flattened child values.
            "Array" => BuildListArray(col, (ListType)arrowType, n,
                (childCol, childArrow, m) => BuildArray(childCol, scalar.TypeArguments[0], childArrow, m)),
            "Tuple" => BuildStructArray(col, scalar.TypeArguments, (StructType)arrowType, n),
            "Map" => BuildMapArray(col, scalar, (MapType)arrowType, n),
            "Nested" => BuildNestedArray(col, scalar.TypeArguments, (ListType)arrowType, n),

            // Geo aliases — fixed nestings of List/List/List over a Point struct (x, y doubles).
            "Point" => BuildPointArray(col, (StructType)arrowType, n),
            "Ring" or "LineString" => BuildListArray(col, (ListType)arrowType, n,
                (cc, ca, m) => BuildPointArray(cc, (StructType)ca, m)),
            "Polygon" or "MultiLineString" => BuildListArray(col, (ListType)arrowType, n,
                (cc, ca, m) => BuildListArray(cc, (ListType)ca, m,
                    (c2, a2, m2) => BuildPointArray(c2, (StructType)a2, m2))),
            "MultiPolygon" => BuildListArray(col, (ListType)arrowType, n,
                (cc, ca, m) => BuildListArray(cc, (ListType)ca, m,
                    (c2, a2, m2) => BuildListArray(c2, (ListType)a2, m2,
                        (c3, a3, m3) => BuildPointArray(c3, (StructType)a3, m3)))),

            _ => throw new NotSupportedException(
                $"ClickHouse type '{scalar.OriginalTypeName}' is not yet supported by the ADBC adapter."),
        };
    }

    private static string FormatInvariant(object v) =>
        v is IFormattable f ? f.ToString(null, CultureInfo.InvariantCulture) : v.ToString()!;

    private static IArrowArray BuildInt8(ITypedColumn col, int n) =>
        BuildPrimitive<sbyte, Int8Array, Int8Array.Builder>(new Int8Array.Builder(), col, n);

    private static IArrowArray BuildInt16(ITypedColumn col, int n) =>
        BuildPrimitive<short, Int16Array, Int16Array.Builder>(new Int16Array.Builder(), col, n);

    private static IArrowArray BuildInt32(ITypedColumn col, int n) =>
        BuildPrimitive<int, Int32Array, Int32Array.Builder>(new Int32Array.Builder(), col, n);

    private static IArrowArray BuildInt64(ITypedColumn col, int n) =>
        BuildPrimitive<long, Int64Array, Int64Array.Builder>(new Int64Array.Builder(), col, n);

    private static IArrowArray BuildUInt8(ITypedColumn col, int n) =>
        BuildPrimitive<byte, UInt8Array, UInt8Array.Builder>(new UInt8Array.Builder(), col, n);

    private static IArrowArray BuildUInt16(ITypedColumn col, int n) =>
        BuildPrimitive<ushort, UInt16Array, UInt16Array.Builder>(new UInt16Array.Builder(), col, n);

    private static IArrowArray BuildUInt32(ITypedColumn col, int n) =>
        BuildPrimitive<uint, UInt32Array, UInt32Array.Builder>(new UInt32Array.Builder(), col, n);

    private static IArrowArray BuildUInt64(ITypedColumn col, int n) =>
        BuildPrimitive<ulong, UInt64Array, UInt64Array.Builder>(new UInt64Array.Builder(), col, n);

    private static IArrowArray BuildFloat(ITypedColumn col, int n) =>
        BuildPrimitive<float, FloatArray, FloatArray.Builder>(new FloatArray.Builder(), col, n);

    private static IArrowArray BuildDouble(ITypedColumn col, int n) =>
        BuildPrimitive<double, DoubleArray, DoubleArray.Builder>(new DoubleArray.Builder(), col, n);

    /// <summary>
    /// Builds a fixed-width primitive Arrow array. Fast path: bulk-copy the column's backing span
    /// (<c>TypedColumn&lt;T&gt;</c>) or iterate a <c>TypedColumn&lt;T?&gt;</c> span — neither boxes.
    /// Any other <see cref="ITypedColumn"/> implementation falls back to the boxing
    /// <see cref="ITypedColumn.GetValue"/> accessor. All three branches are value-identical.
    /// </summary>
    private static IArrowArray BuildPrimitive<T, TArray, TBuilder>(TBuilder builder, ITypedColumn col, int n)
        where T : struct
        where TArray : IArrowArray
        where TBuilder : PrimitiveArrayBuilder<T, TArray, TBuilder>
    {
        builder.Reserve(n);
        switch (col)
        {
            case TypedColumn<T> nonNull:
                builder.Append(nonNull.Values);
                break;
            case TypedColumn<T?> nullable:
                foreach (var value in nullable.Values)
                {
                    if (value.HasValue) builder.Append(value.Value);
                    else builder.AppendNull();
                }
                break;
            default:
                for (int i = 0; i < n; i++)
                {
                    if (col.IsNull(i)) builder.AppendNull();
                    else builder.Append((T)col.GetValue(i)!);
                }
                break;
        }
        return builder.Build();
    }

    private static IArrowArray BuildBool(ITypedColumn col, int n)
    {
        // BooleanArray is bit-packed (not a PrimitiveArrayBuilder<bool>), but the span branches still
        // avoid per-cell boxing relative to the GetValue fallback.
        var b = new BooleanArray.Builder();
        switch (col)
        {
            case TypedColumn<bool> nonNull:
                foreach (var value in nonNull.Values) b.Append(value);
                break;
            case TypedColumn<bool?> nullable:
                foreach (var value in nullable.Values)
                {
                    if (value.HasValue) b.Append(value.Value);
                    else b.AppendNull();
                }
                break;
            default:
                for (int i = 0; i < n; i++) { if (col.IsNull(i)) b.AppendNull(); else b.Append((bool)col.GetValue(i)!); }
                break;
        }
        return b.Build();
    }

    // Dedicated builder for the String column. A ClickHouse String (and Nullable(String)) reads back as
    // TypedColumn<string> (nulls are null refs), so the span path needs no boxing and no conversion;
    // the other text-projected types keep the convert-delegate overload below.
    private static IArrowArray BuildStringColumn(ITypedColumn col, int n)
    {
        var b = new StringArray.Builder();
        b.Reserve(n);
        if (col is TypedColumn<string> typed)
        {
            foreach (var s in typed.Values) { if (s is null) b.AppendNull(); else b.Append(s); }
        }
        else
        {
            for (int i = 0; i < n; i++) { if (col.IsNull(i)) b.AppendNull(); else b.Append((string)col.GetValue(i)!); }
        }
        return b.Build();
    }

    private static IArrowArray BuildString(ITypedColumn col, int n, Func<object, string> convert)
    {
        var b = new StringArray.Builder();
        for (int i = 0; i < n; i++) { if (col.IsNull(i)) b.AppendNull(); else b.Append(convert(col.GetValue(i)!)); }
        return b.Build();
    }

    private static IArrowArray BuildBinary(ITypedColumn col, int n)
    {
        var b = new BinaryArray.Builder();
        for (int i = 0; i < n; i++)
        {
            if (col.IsNull(i)) b.AppendNull();
            else b.Append((ReadOnlySpan<byte>)(byte[])col.GetValue(i)!);
        }
        return b.Build();
    }

    private static IArrowArray BuildDate32(ITypedColumn col, int n)
    {
        var b = new Date32Array.Builder();
        b.Reserve(n);
        // Same conversion as the boxing path (DateOnly → midnight-UTC DateTime), sourced from the
        // typed span where possible so no DateOnly is boxed.
        switch (col)
        {
            case TypedColumn<DateOnly> nonNull:
                foreach (var v in nonNull.Values) b.Append(Date32Of(v));
                break;
            case TypedColumn<DateOnly?> nullable:
                foreach (var v in nullable.Values) { if (v.HasValue) b.Append(Date32Of(v.Value)); else b.AppendNull(); }
                break;
            default:
                for (int i = 0; i < n; i++) { if (col.IsNull(i)) b.AppendNull(); else b.Append(Date32Of((DateOnly)col.GetValue(i)!)); }
                break;
        }
        return b.Build();
    }

    private static DateTime Date32Of(DateOnly d) => d.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc);

    private static IArrowArray BuildTimestamp(ITypedColumn col, int n, TimestampType type)
    {
        var b = new TimestampArray.Builder(type);
        b.Reserve(n);
        // A timezone-aware ClickHouse DateTime/DateTime64 column reads as DateTimeOffset (offset carried
        // on the value); a naive one reads as DateTime (interpreted as UTC). Take the typed-span path for
        // either storage; fall back to the boxing value-switch for anything else.
        switch (col)
        {
            case TypedColumn<DateTimeOffset> nonNull:
                foreach (var v in nonNull.Values) b.Append(v);
                break;
            case TypedColumn<DateTimeOffset?> nullable:
                foreach (var v in nullable.Values) { if (v.HasValue) b.Append(v.Value); else b.AppendNull(); }
                break;
            case TypedColumn<DateTime> nonNull:
                foreach (var v in nonNull.Values) b.Append(InstantOf(v));
                break;
            case TypedColumn<DateTime?> nullable:
                foreach (var v in nullable.Values) { if (v.HasValue) b.Append(InstantOf(v.Value)); else b.AppendNull(); }
                break;
            default:
                for (int i = 0; i < n; i++)
                {
                    if (col.IsNull(i)) { b.AppendNull(); continue; }
                    var value = col.GetValue(i)!;
                    b.Append(value switch
                    {
                        DateTimeOffset dto => dto,
                        DateTime dt => InstantOf(dt),
                        _ => throw new NotSupportedException($"Unexpected timestamp element type '{value.GetType()}'."),
                    });
                }
                break;
        }
        return b.Build();
    }

    private static DateTimeOffset InstantOf(DateTime dt) => new(DateTime.SpecifyKind(dt, DateTimeKind.Utc));

    // ClickHouse Time/Time64 read back as TimeOnly (100-ns ticks since midnight). Arrow Time32 carries
    // seconds (or millis); Arrow Time64 carries micros (or nanos). Convert ticks into the column's unit.
    private static IArrowArray BuildTime32(ITypedColumn col, int n, Time32Type type)
    {
        long ticksPerUnit = type.Unit == TimeUnit.Second ? TimeSpan.TicksPerSecond : TimeSpan.TicksPerMillisecond;
        var b = new Time32Array.Builder(type);
        for (int i = 0; i < n; i++)
        {
            if (col.IsNull(i)) b.AppendNull();
            else b.Append((int)(((TimeOnly)col.GetValue(i)!).Ticks / ticksPerUnit));
        }
        return b.Build();
    }

    private static IArrowArray BuildTime64(ITypedColumn col, int n, Time64Type type)
    {
        var b = new Time64Array.Builder(type);
        for (int i = 0; i < n; i++)
        {
            if (col.IsNull(i)) { b.AppendNull(); continue; }
            long ticks = ((TimeOnly)col.GetValue(i)!).Ticks;
            // 1 tick = 100 ns ⇒ micros = ticks / 10, nanos = ticks * 100.
            b.Append(type.Unit == TimeUnit.Microsecond ? ticks / 10 : ticks * 100);
        }
        return b.Build();
    }

    private static IArrowArray BuildDecimal(ITypedColumn col, IArrowType arrowType, int n)
    {
        if (arrowType is Decimal128Type d128)
        {
            var b = new Decimal128Array.Builder(d128);
            FillDecimal(col, n, s => b.Append(s), () => b.AppendNull());
            return b.Build();
        }

        var b256 = new Decimal256Array.Builder((Decimal256Type)arrowType);
        FillDecimal(col, n, s => b256.Append(s), () => b256.AppendNull());
        return b256.Build();
    }

    // Narrow decimals (Decimal32/64) read back as System.Decimal, wide ones (Decimal128/256) as
    // ClickHouseDecimal. Render each to the same invariant string the boxing path produces, sourced
    // from the typed span where possible so the value struct is not boxed.
    private static void FillDecimal(ITypedColumn col, int n, Action<string> append, Action appendNull)
    {
        switch (col)
        {
            case TypedColumn<decimal> nonNull:
                foreach (var v in nonNull.Values) append(v.ToString(CultureInfo.InvariantCulture));
                break;
            case TypedColumn<decimal?> nullable:
                foreach (var v in nullable.Values) { if (v.HasValue) append(v.Value.ToString(CultureInfo.InvariantCulture)); else appendNull(); }
                break;
            case TypedColumn<ClickHouseDecimal> nonNull:
                foreach (var v in nonNull.Values) append(v.ToString());
                break;
            case TypedColumn<ClickHouseDecimal?> nullable:
                foreach (var v in nullable.Values) { if (v.HasValue) append(v.Value.ToString()); else appendNull(); }
                break;
            default:
                for (int i = 0; i < n; i++) { if (col.IsNull(i)) appendNull(); else append(DecimalString(col.GetValue(i)!)); }
                break;
        }
    }

    // A ClickHouse Decimal column reads back as ClickHouseDecimal (full 38/76-digit precision) for
    // wide types but as System.Decimal for narrow ones that fit. Render both as an invariant string
    // and let the Arrow builder parse it at the column's scale — this avoids precision loss.
    private static string DecimalString(object value) => value switch
    {
        ClickHouseDecimal chd => chd.ToString(),
        decimal dec => dec.ToString(CultureInfo.InvariantCulture),
        _ => throw new NotSupportedException($"Unexpected decimal element type '{value.GetType()}'."),
    };

    // ---- Composite builders -------------------------------------------------------------------

    /// <summary>
    /// Builds an Arrow <see cref="ListArray"/>. Each non-null row value is enumerated into a single
    /// flattened child buffer; <paramref name="buildChild"/> turns that buffer (as a column view) plus
    /// the list's element Arrow type into the child array, so it can recurse for nested lists.
    /// </summary>
    private static IArrowArray BuildListArray(
        ITypedColumn col, ListType listType, int n,
        Func<ITypedColumn, IArrowType, int, IArrowArray> buildChild)
    {
        var offsets = new ArrowBuffer.Builder<int>(n + 1);
        var validity = new ArrowBuffer.BitmapBuilder(n);
        var childValues = new List<object?>();
        int running = 0, nullCount = 0;

        offsets.Append(0);
        for (int i = 0; i < n; i++)
        {
            if (col.IsNull(i))
            {
                validity.Append(false);
                nullCount++;
            }
            else
            {
                validity.Append(true);
                foreach (var element in (IEnumerable)col.GetValue(i)!)
                {
                    childValues.Add(element);
                    running++;
                }
            }
            offsets.Append(running);
        }

        var child = buildChild(new ListColumn(childValues), listType.ValueDataType, childValues.Count);
        return new ListArray(listType, n, offsets.Build(), child, validity.Build(), nullCount);
    }

    /// <summary>Builds an Arrow <see cref="StructArray"/> from a ClickHouse Tuple (row value is <c>object[]</c>).</summary>
    private static IArrowArray BuildStructArray(
        ITypedColumn col, IReadOnlyList<ClickHouseType> fieldTypes, StructType structType, int n)
    {
        int fieldCount = fieldTypes.Count;
        var fieldValues = new List<object?>[fieldCount];
        for (int f = 0; f < fieldCount; f++) fieldValues[f] = new List<object?>(n);

        var validity = new ArrowBuffer.BitmapBuilder(n);
        int nullCount = 0;
        for (int i = 0; i < n; i++)
        {
            if (col.IsNull(i))
            {
                validity.Append(false);
                nullCount++;
                for (int f = 0; f < fieldCount; f++) fieldValues[f].Add(null);
            }
            else
            {
                validity.Append(true);
                // A ClickHouse Tuple reads back as a System.Tuple/ValueTuple (both implement ITuple).
                var elements = (ITuple)col.GetValue(i)!;
                for (int f = 0; f < fieldCount; f++) fieldValues[f].Add(elements[f]);
            }
        }

        var children = new IArrowArray[fieldCount];
        for (int f = 0; f < fieldCount; f++)
            children[f] = BuildArray(new ListColumn(fieldValues[f]), fieldTypes[f], structType.Fields[f].DataType, n);

        return new StructArray(structType, n, children, validity.Build(), nullCount);
    }

    /// <summary>Builds an Arrow <see cref="MapArray"/> from a ClickHouse Map (row value is <c>IDictionary</c>).</summary>
    private static IArrowArray BuildMapArray(ITypedColumn col, ClickHouseType scalar, MapType mapType, int n)
    {
        var entryStruct = new StructType(new[] { mapType.KeyField, mapType.ValueField });
        var offsets = new ArrowBuffer.Builder<int>(n + 1);
        var validity = new ArrowBuffer.BitmapBuilder(n);
        var keys = new List<object?>();
        var values = new List<object?>();
        int running = 0, nullCount = 0;

        offsets.Append(0);
        for (int i = 0; i < n; i++)
        {
            if (col.IsNull(i))
            {
                validity.Append(false);
                nullCount++;
            }
            else
            {
                validity.Append(true);
                foreach (DictionaryEntry entry in (IDictionary)col.GetValue(i)!)
                {
                    keys.Add(entry.Key);
                    values.Add(entry.Value);
                    running++;
                }
            }
            offsets.Append(running);
        }

        var keyArray = BuildArray(new ListColumn(keys), scalar.TypeArguments[0], entryStruct.Fields[0].DataType, keys.Count);
        var valueArray = BuildArray(new ListColumn(values), scalar.TypeArguments[1], entryStruct.Fields[1].DataType, values.Count);
        var entries = new StructArray(entryStruct, keys.Count, new[] { keyArray, valueArray }, ArrowBuffer.Empty, 0);

        return new MapArray(mapType, n, offsets.Build(), entries, validity.Build(), nullCount);
    }

    /// <summary>
    /// Builds a ClickHouse Nested column as Arrow <c>List&lt;Struct&gt;</c>. Each row value is
    /// <c>object[]</c> of parallel field arrays (all the same length); the struct is the per-element
    /// row across those arrays.
    /// </summary>
    private static IArrowArray BuildNestedArray(
        ITypedColumn col, IReadOnlyList<ClickHouseType> fieldTypes, ListType listType, int n)
    {
        var entryStruct = (StructType)listType.ValueDataType;
        int fieldCount = fieldTypes.Count;
        var fieldValues = new List<object?>[fieldCount];
        for (int f = 0; f < fieldCount; f++) fieldValues[f] = new List<object?>();

        var offsets = new ArrowBuffer.Builder<int>(n + 1);
        var validity = new ArrowBuffer.BitmapBuilder(n);
        int running = 0, nullCount = 0;

        offsets.Append(0);
        for (int i = 0; i < n; i++)
        {
            if (col.IsNull(i))
            {
                validity.Append(false);
                nullCount++;
            }
            else
            {
                validity.Append(true);
                var fields = (object[])col.GetValue(i)!;
                int length = ((System.Array)fields[0]).Length;
                for (int j = 0; j < length; j++)
                    for (int f = 0; f < fieldCount; f++)
                        fieldValues[f].Add(((System.Array)fields[f]).GetValue(j));
                running += length;
            }
            offsets.Append(running);
        }

        var children = new IArrowArray[fieldCount];
        for (int f = 0; f < fieldCount; f++)
            children[f] = BuildArray(new ListColumn(fieldValues[f]), fieldTypes[f], entryStruct.Fields[f].DataType, fieldValues[f].Count);

        var entries = new StructArray(entryStruct, running, children, ArrowBuffer.Empty, 0);
        return new ListArray(listType, n, offsets.Build(), entries, validity.Build(), nullCount);
    }

    /// <summary>Builds a geo Point (<c>Struct&lt;x, y&gt;</c>) array; row value is a <see cref="Point"/>.</summary>
    private static IArrowArray BuildPointArray(ITypedColumn col, StructType structType, int n)
    {
        var xs = new List<object?>(n);
        var ys = new List<object?>(n);
        var validity = new ArrowBuffer.BitmapBuilder(n);
        int nullCount = 0;
        for (int i = 0; i < n; i++)
        {
            if (col.IsNull(i))
            {
                validity.Append(false);
                nullCount++;
                xs.Add(null);
                ys.Add(null);
            }
            else
            {
                validity.Append(true);
                var p = (Point)col.GetValue(i)!;
                xs.Add(p.X);
                ys.Add(p.Y);
            }
        }

        var children = new IArrowArray[]
        {
            BuildDouble(new ListColumn(xs), n),
            BuildDouble(new ListColumn(ys), n),
        };
        return new StructArray(structType, n, children, validity.Build(), nullCount);
    }

    /// <summary>
    /// Adapts a flattened list of boxed values to <see cref="ITypedColumn"/> so the scalar leaf
    /// builders can be reused for nested composite children without a parallel set of overloads.
    /// </summary>
    private sealed class ListColumn : ITypedColumn
    {
        private readonly IReadOnlyList<object?> _values;
        public ListColumn(IReadOnlyList<object?> values) => _values = values;
        public Type ElementType => typeof(object);
        public int Count => _values.Count;
        public object? GetValue(int index) => _values[index];
        public bool IsNull(int index) => _values[index] is null;
        public void Dispose() { }
    }
}
