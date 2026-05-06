using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for Map(K, V) values.
/// </summary>
/// <remarks>
/// Wire format: Same as Array(Tuple(K, V))
/// 1. Offsets array (UInt64 cumulative counts)
/// 2. All keys concatenated
/// 3. All values concatenated
/// </remarks>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
internal sealed class MapColumnWriter<TKey, TValue> : IColumnWriter<Dictionary<TKey, TValue>>
    where TKey : notnull
{
    private readonly IColumnWriter<TKey> _keyWriter;
    private readonly IColumnWriter<TValue> _valueWriter;

    /// <summary>
    /// Creates a Map writer with the specified key and value writers.
    /// </summary>
    /// <param name="keyWriter">Writer for map keys.</param>
    /// <param name="valueWriter">Writer for map values.</param>
    public MapColumnWriter(IColumnWriter<TKey> keyWriter, IColumnWriter<TValue> valueWriter)
    {
        _keyWriter = keyWriter ?? throw new ArgumentNullException(nameof(keyWriter));
        _valueWriter = valueWriter ?? throw new ArgumentNullException(nameof(valueWriter));
    }

    /// <summary>
    /// Creates a Map writer from non-generic IColumnWriters.
    /// </summary>
    /// <param name="keyWriter">Writer for map keys.</param>
    /// <param name="valueWriter">Writer for map values.</param>
    public MapColumnWriter(IColumnWriter keyWriter, IColumnWriter valueWriter)
    {
        if (keyWriter is IColumnWriter<TKey> typedKeyWriter)
        {
            _keyWriter = typedKeyWriter;
        }
        else
        {
            throw new ArgumentException(
                $"Key writer must implement IColumnWriter<{typeof(TKey).Name}>.",
                nameof(keyWriter));
        }

        if (valueWriter is IColumnWriter<TValue> typedValueWriter)
        {
            _valueWriter = typedValueWriter;
        }
        else
        {
            throw new ArgumentException(
                $"Value writer must implement IColumnWriter<{typeof(TValue).Name}>.",
                nameof(valueWriter));
        }
    }

    /// <inheritdoc />
    public string TypeName => $"Map({_keyWriter.TypeName}, {_valueWriter.TypeName})";

    /// <inheritdoc />
    public Type ClrType => typeof(Dictionary<TKey, TValue>);

    /// <inheritdoc />
    // Emit both inner writers' prefixes (keys first, then values). Map has no prefix
    // of its own — offsets are per-row data.
    public void WritePrefix(ref ProtocolWriter writer)
    {
        _keyWriter.WritePrefix(ref writer);
        _valueWriter.WritePrefix(ref writer);
    }

    /// <inheritdoc />
    // Nullable(Map(...)) wrapper substitutes this empty Dictionary under a
    // null-bitmap byte. Map's wire format requires offset+kv-bytes for every
    // row regardless of bitmap; an empty Dictionary contributes (0, no kv).
    public Dictionary<TKey, TValue> NullPlaceholder => new();

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, Dictionary<TKey, TValue>[] values)
    {
        // Step 1: Write cumulative offsets (UInt64 per row). Reject null rows
        // — Map(K, V) is non-nullable; Nullable(Map(K, V)) wraps with
        // NullableRefColumnWriter which substitutes an empty Dictionary before
        // delegating here.
        ulong offset = 0;
        int totalEntries = 0;
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is null)
                throw NullAt(i);
            var count = values[i].Count;
            offset += (ulong)count;
            totalEntries += count;
            writer.WriteUInt64(offset);
        }

        // Step 2: Flatten all keys and write as a column
        var allKeys = new TKey[totalEntries];
        int pos = 0;
        for (int i = 0; i < values.Length; i++)
        {
            foreach (var key in values[i].Keys)
                allKeys[pos++] = key;
        }
        _keyWriter.WriteColumn(ref writer, allKeys);

        // Step 3: Flatten all values and write as a column
        var allValues = new TValue[totalEntries];
        pos = 0;
        for (int i = 0; i < values.Length; i++)
        {
            foreach (var val in values[i].Values)
                allValues[pos++] = val;
        }
        _valueWriter.WriteColumn(ref writer, allValues);
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, Dictionary<TKey, TValue> value)
    {
        if (value is null)
            throw NullAt(rowIndex: -1);

        writer.WriteUInt64((ulong)value.Count);

        foreach (var key in value.Keys)
        {
            _keyWriter.WriteValue(ref writer, key);
        }

        foreach (var val in value.Values)
        {
            _valueWriter.WriteValue(ref writer, val);
        }
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        // Step 1: Write offsets. Accept any IDictionary so e.g. Dictionary<string,string>
        // can pass through to FixedStringColumnWriter (ClrType=byte[] but accepts strings
        // via its non-generic WriteValue). Reject null rows — Map(K, V) is non-nullable;
        // Nullable(Map(K, V)) wraps with NullableRefColumnWriter which substitutes an
        // empty Dictionary first.
        ulong offset = 0;
        int totalEntries = 0;
        bool allMatch = true;
        for (int i = 0; i < values.Length; i++)
        {
            int count;
            if (values[i] is null)
            {
                throw NullAt(i);
            }
            else if (values[i] is Dictionary<TKey, TValue> typedDict)
            {
                count = typedDict.Count;
            }
            else if (values[i] is System.Collections.IDictionary d)
            {
                count = d.Count;
                allMatch = false;
            }
            else
            {
                throw new InvalidOperationException(
                    $"MapColumnWriter<{typeof(TKey).Name}, {typeof(TValue).Name}> received unsupported value type " +
                    $"{values[i]!.GetType().Name} at row {i}. Expected Dictionary<{typeof(TKey).Name}, {typeof(TValue).Name}> or IDictionary.");
            }
            offset += (ulong)count;
            totalEntries += count;
            writer.WriteUInt64(offset);
        }

        // Fast/correct path: when every row is the exact typed Dictionary, use the
        // bulk WriteColumn on the inner writers (preserves per-column headers such as
        // NullableColumnWriter's null bitmap).
        if (allMatch)
        {
            var allKeys = new TKey[totalEntries];
            var allValues = new TValue[totalEntries];
            int pos = 0;
            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] is Dictionary<TKey, TValue> dict)
                {
                    foreach (var kvp in dict)
                    {
                        allKeys[pos] = kvp.Key;
                        allValues[pos] = kvp.Value;
                        pos++;
                    }
                }
            }
            _keyWriter.WriteColumn(ref writer, allKeys);
            _valueWriter.WriteColumn(ref writer, allValues);
            return;
        }

        // Compat path: CLR types don't exactly match — iterate entries and write
        // each key/value via the non-generic per-value writer. Only safe when the
        // inner writers don't emit per-column state prefixes (true for String /
        // FixedString / numerics; not true for Nullable / LowCardinality at inner).
        IColumnWriter keyWriterNg = _keyWriter;
        IColumnWriter valueWriterNg = _valueWriter;
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is System.Collections.IDictionary dict)
            {
                foreach (System.Collections.DictionaryEntry entry in dict)
                    keyWriterNg.WriteValue(ref writer, entry.Key);
            }
        }
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is System.Collections.IDictionary dict)
            {
                foreach (System.Collections.DictionaryEntry entry in dict)
                    valueWriterNg.WriteValue(ref writer, entry.Value);
            }
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        switch (value)
        {
            case null:
                throw NullAt(rowIndex: -1);
            case Dictionary<TKey, TValue> dict:
                WriteValue(ref writer, dict);
                break;
            default:
                throw new InvalidOperationException(
                    $"MapColumnWriter<{typeof(TKey).Name}, {typeof(TValue).Name}> received unsupported value type " +
                    $"{value.GetType().Name}. Expected Dictionary<{typeof(TKey).Name}, {typeof(TValue).Name}>.");
        }
    }

    private static InvalidOperationException NullAt(int rowIndex)
    {
        var where = rowIndex >= 0 ? $" at row {rowIndex}" : string.Empty;
        return new InvalidOperationException(
            $"MapColumnWriter<{typeof(TKey).Name}, {typeof(TValue).Name}> received null{where}. The Map column type " +
            $"is non-nullable; declare the column as Nullable(Map({typeof(TKey).Name}, {typeof(TValue).Name})) and wrap " +
            $"this writer with NullableRefColumnWriter, or ensure source values are non-null.");
    }
}
