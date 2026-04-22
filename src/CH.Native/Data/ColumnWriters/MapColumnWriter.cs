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
public sealed class MapColumnWriter<TKey, TValue> : IColumnWriter<Dictionary<TKey, TValue>>
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
    public void WriteColumn(ref ProtocolWriter writer, Dictionary<TKey, TValue>[] values)
    {
        // Step 1: Write cumulative offsets (UInt64 per row)
        ulong offset = 0;
        int totalEntries = 0;
        for (int i = 0; i < values.Length; i++)
        {
            var count = values[i]?.Count ?? 0;
            offset += (ulong)count;
            totalEntries += count;
            writer.WriteUInt64(offset);
        }

        // Step 2: Flatten all keys and write as a column
        var allKeys = new TKey[totalEntries];
        int pos = 0;
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is { } dict)
                foreach (var key in dict.Keys)
                    allKeys[pos++] = key;
        }
        _keyWriter.WriteColumn(ref writer, allKeys);

        // Step 3: Flatten all values and write as a column
        var allValues = new TValue[totalEntries];
        pos = 0;
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is { } dict)
                foreach (var val in dict.Values)
                    allValues[pos++] = val;
        }
        _valueWriter.WriteColumn(ref writer, allValues);
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, Dictionary<TKey, TValue> value)
    {
        var count = value?.Count ?? 0;
        writer.WriteUInt64((ulong)count);

        if (value != null && count > 0)
        {
            foreach (var key in value.Keys)
            {
                _keyWriter.WriteValue(ref writer, key);
            }

            foreach (var val in value.Values)
            {
                _valueWriter.WriteValue(ref writer, val);
            }
        }
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        // Step 1: Write offsets. Accept any IDictionary so e.g. Dictionary<string,string>
        // can pass through to FixedStringColumnWriter (ClrType=byte[] but accepts strings
        // via its non-generic WriteValue).
        ulong offset = 0;
        int totalEntries = 0;
        bool allMatch = true;
        for (int i = 0; i < values.Length; i++)
        {
            int count;
            if (values[i] is Dictionary<TKey, TValue> typedDict)
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
                count = 0;
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
        if (value is Dictionary<TKey, TValue> dict)
        {
            WriteValue(ref writer, dict);
        }
        else
        {
            WriteValue(ref writer, new Dictionary<TKey, TValue>());
        }
    }
}
