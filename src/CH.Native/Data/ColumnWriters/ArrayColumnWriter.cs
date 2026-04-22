using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for Array(T) values.
/// </summary>
/// <remarks>
/// Wire format:
/// 1. Offsets array: UInt64 cumulative counts for each row
/// 2. All element values concatenated
/// </remarks>
/// <typeparam name="T">The element type.</typeparam>
public sealed class ArrayColumnWriter<T> : IColumnWriter<T[]>
{
    private readonly IColumnWriter<T> _elementWriter;

    /// <summary>
    /// Creates an Array writer that uses the specified element writer.
    /// </summary>
    /// <param name="elementWriter">The writer for element values.</param>
    public ArrayColumnWriter(IColumnWriter<T> elementWriter)
    {
        _elementWriter = elementWriter ?? throw new ArgumentNullException(nameof(elementWriter));
    }

    /// <summary>
    /// Creates an Array writer from a non-generic IColumnWriter.
    /// </summary>
    /// <param name="elementWriter">The writer for element values.</param>
    public ArrayColumnWriter(IColumnWriter elementWriter)
    {
        if (elementWriter is IColumnWriter<T> typedWriter)
        {
            _elementWriter = typedWriter;
        }
        else
        {
            throw new ArgumentException(
                $"Element writer must implement IColumnWriter<{typeof(T).Name}>.",
                nameof(elementWriter));
        }
    }

    /// <inheritdoc />
    public string TypeName => $"Array({_elementWriter.TypeName})";

    /// <inheritdoc />
    public Type ClrType => typeof(T[]);

    /// <inheritdoc />
    // Recursively emit the element writer's prefix. Array has no prefix bytes of its
    // own — its offsets are per-row data. But if the element is LowCardinality (or
    // wraps one), its KeysSerializationVersion must precede our offsets.
    public void WritePrefix(ref ProtocolWriter writer) => _elementWriter.WritePrefix(ref writer);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, T[][] values)
    {
        // Step 1: Write cumulative offsets (UInt64 per row)
        ulong offset = 0;
        int totalElements = 0;
        for (int i = 0; i < values.Length; i++)
        {
            var len = values[i]?.Length ?? 0;
            offset += (ulong)len;
            totalElements += len;
            writer.WriteUInt64(offset);
        }

        // Step 2: Flatten all elements and write as a column
        var allElements = new T[totalElements];
        int pos = 0;
        for (int i = 0; i < values.Length; i++)
        {
            var array = values[i];
            if (array != null && array.Length > 0)
            {
                Array.Copy(array, 0, allElements, pos, array.Length);
                pos += array.Length;
            }
        }
        _elementWriter.WriteColumn(ref writer, allElements);
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, T[] value)
    {
        var length = value?.Length ?? 0;
        writer.WriteUInt64((ulong)length);

        if (value != null)
        {
            for (int i = 0; i < value.Length; i++)
            {
                _elementWriter.WriteValue(ref writer, value[i]);
            }
        }
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        // Step 1: Write cumulative offsets. Accept any IList so that e.g. string[]
        // can pass through to FixedStringColumnWriter (ClrType=byte[] but its
        // non-generic WriteValue also accepts strings).
        ulong offset = 0;
        int totalElements = 0;
        bool allMatchT = true;
        for (int i = 0; i < values.Length; i++)
        {
            int len;
            if (values[i] is T[] typedArray)
            {
                len = typedArray.Length;
            }
            else if (values[i] is System.Collections.IList list)
            {
                len = list.Count;
                allMatchT = false;
            }
            else
            {
                len = 0;
            }
            offset += (ulong)len;
            totalElements += len;
            writer.WriteUInt64(offset);
        }

        // Step 2a (typed fast path): every row already matches T[] — flatten and
        // delegate to the typed WriteColumn so inner writers that emit per-column
        // headers (Nullable null bitmap, LowCardinality flags/dict) see one call.
        if (allMatchT)
        {
            var allElements = new T[totalElements];
            int pos = 0;
            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] is T[] array && array.Length > 0)
                {
                    Array.Copy(array, 0, allElements, pos, array.Length);
                    pos += array.Length;
                }
            }
            _elementWriter.WriteColumn(ref writer, allElements);
            return;
        }

        // Step 2b (CLR-mismatch path): flatten into object?[] and delegate to the
        // element writer's non-generic WriteColumn, which applies its own coercion
        // (FixedString/string, Decimal128/decimal, etc.). This still keeps the one-
        // call contract — crucial for LowCardinality (per-element WriteValue would
        // emit one LC header per element, corrupting the stream).
        var flattened = new object?[totalElements];
        int fp = 0;
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is System.Collections.IList list)
            {
                for (int j = 0; j < list.Count; j++)
                {
                    flattened[fp++] = CoerceForInnerWriter(list[j]);
                }
            }
        }
        IColumnWriter elementWriterNonGeneric = _elementWriter;
        elementWriterNonGeneric.WriteColumn(ref writer, flattened);
    }

    // Pre-coerce for cases where the inner writer's non-generic WriteColumn won't
    // coerce itself. Currently only FixedString-shaped writers (ClrType = byte[])
    // when wrapped in another writer (e.g. LowCardinality) don't chain through the
    // inner element's non-generic WriteValue, so a string POCO value would get cast
    // to default(byte[]) = null. Re-encode to UTF-8 up-front here.
    private static object? CoerceForInnerWriter(object? value)
    {
        if (typeof(T) == typeof(byte[]) && value is string s)
        {
            return System.Text.Encoding.UTF8.GetBytes(s);
        }
        return value;
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        switch (value)
        {
            case T[] typedArray:
                WriteValue(ref writer, typedArray);
                break;
            case System.Collections.IList list:
                writer.WriteUInt64((ulong)list.Count);
                IColumnWriter elementWriterNonGeneric = _elementWriter;
                for (int j = 0; j < list.Count; j++)
                    elementWriterNonGeneric.WriteValue(ref writer, list[j]);
                break;
            default:
                WriteValue(ref writer, Array.Empty<T>());
                break;
        }
    }
}
