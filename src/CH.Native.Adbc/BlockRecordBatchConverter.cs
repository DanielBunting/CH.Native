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
            "String" => BuildString(col, n, static v => (string)v),
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

    private static IArrowArray BuildInt8(ITypedColumn col, int n)
    {
        var b = new Int8Array.Builder();
        for (int i = 0; i < n; i++) { if (col.IsNull(i)) b.AppendNull(); else b.Append((sbyte)col.GetValue(i)!); }
        return b.Build();
    }

    private static IArrowArray BuildInt16(ITypedColumn col, int n)
    {
        var b = new Int16Array.Builder();
        for (int i = 0; i < n; i++) { if (col.IsNull(i)) b.AppendNull(); else b.Append((short)col.GetValue(i)!); }
        return b.Build();
    }

    private static IArrowArray BuildInt32(ITypedColumn col, int n)
    {
        var b = new Int32Array.Builder();
        for (int i = 0; i < n; i++) { if (col.IsNull(i)) b.AppendNull(); else b.Append((int)col.GetValue(i)!); }
        return b.Build();
    }

    private static IArrowArray BuildInt64(ITypedColumn col, int n)
    {
        var b = new Int64Array.Builder();
        for (int i = 0; i < n; i++) { if (col.IsNull(i)) b.AppendNull(); else b.Append((long)col.GetValue(i)!); }
        return b.Build();
    }

    private static IArrowArray BuildUInt8(ITypedColumn col, int n)
    {
        var b = new UInt8Array.Builder();
        for (int i = 0; i < n; i++) { if (col.IsNull(i)) b.AppendNull(); else b.Append((byte)col.GetValue(i)!); }
        return b.Build();
    }

    private static IArrowArray BuildUInt16(ITypedColumn col, int n)
    {
        var b = new UInt16Array.Builder();
        for (int i = 0; i < n; i++) { if (col.IsNull(i)) b.AppendNull(); else b.Append((ushort)col.GetValue(i)!); }
        return b.Build();
    }

    private static IArrowArray BuildUInt32(ITypedColumn col, int n)
    {
        var b = new UInt32Array.Builder();
        for (int i = 0; i < n; i++) { if (col.IsNull(i)) b.AppendNull(); else b.Append((uint)col.GetValue(i)!); }
        return b.Build();
    }

    private static IArrowArray BuildUInt64(ITypedColumn col, int n)
    {
        var b = new UInt64Array.Builder();
        for (int i = 0; i < n; i++) { if (col.IsNull(i)) b.AppendNull(); else b.Append((ulong)col.GetValue(i)!); }
        return b.Build();
    }

    private static IArrowArray BuildFloat(ITypedColumn col, int n)
    {
        var b = new FloatArray.Builder();
        for (int i = 0; i < n; i++) { if (col.IsNull(i)) b.AppendNull(); else b.Append((float)col.GetValue(i)!); }
        return b.Build();
    }

    private static IArrowArray BuildDouble(ITypedColumn col, int n)
    {
        var b = new DoubleArray.Builder();
        for (int i = 0; i < n; i++) { if (col.IsNull(i)) b.AppendNull(); else b.Append((double)col.GetValue(i)!); }
        return b.Build();
    }

    private static IArrowArray BuildBool(ITypedColumn col, int n)
    {
        var b = new BooleanArray.Builder();
        for (int i = 0; i < n; i++) { if (col.IsNull(i)) b.AppendNull(); else b.Append((bool)col.GetValue(i)!); }
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
        for (int i = 0; i < n; i++)
        {
            if (col.IsNull(i)) b.AppendNull();
            else b.Append(((DateOnly)col.GetValue(i)!).ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc));
        }
        return b.Build();
    }

    private static IArrowArray BuildTimestamp(ITypedColumn col, int n, TimestampType type)
    {
        var b = new TimestampArray.Builder(type);
        for (int i = 0; i < n; i++)
        {
            if (col.IsNull(i)) { b.AppendNull(); continue; }

            // A timezone-aware ClickHouse DateTime/DateTime64 column reads as DateTimeOffset (the
            // offset is carried on the value); a naive one reads as DateTime (interpreted as UTC).
            var value = col.GetValue(i)!;
            var instant = value switch
            {
                DateTimeOffset dto => dto,
                DateTime dt => new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc)),
                _ => throw new NotSupportedException(
                    $"Unexpected timestamp element type '{value.GetType()}'."),
            };
            b.Append(instant);
        }
        return b.Build();
    }

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
            for (int i = 0; i < n; i++)
            {
                if (col.IsNull(i)) b.AppendNull();
                else b.Append(DecimalString(col.GetValue(i)!));
            }
            return b.Build();
        }

        var b256 = new Decimal256Array.Builder((Decimal256Type)arrowType);
        for (int i = 0; i < n; i++)
        {
            if (col.IsNull(i)) b256.AppendNull();
            else b256.Append(DecimalString(col.GetValue(i)!));
        }
        return b256.Build();
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
