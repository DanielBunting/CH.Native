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
        // First pass: categorise per row and write offsets. Most callers are pure
        // Dictionary<TKey, TValue> — track that as a fast path that avoids per-row
        // buffer allocation. Entries-shape variants are dispatched first so the
        // IDictionary fallback only catches non-generic dictionaries (e.g. F# Map<,>)
        // that don't also implement IEnumerable<KeyValuePair<,>>.
        Dictionary<TKey, TValue>?[]? dicts = null;
        IReadOnlyList<KeyValuePair<TKey, TValue>>?[]? entries = null;
        System.Collections.IDictionary?[]? legacyDicts = null;
        bool allDictionary = true;
        bool hasLegacyDict = false;
        ulong offset = 0;
        int totalEntries = 0;
        for (int i = 0; i < values.Length; i++)
        {
            var raw = values[i];
            int count;
            switch (raw)
            {
                case null:
                    throw NullAt(i);
                case Dictionary<TKey, TValue> typedDict:
                    dicts ??= new Dictionary<TKey, TValue>?[values.Length];
                    dicts[i] = typedDict;
                    count = typedDict.Count;
                    break;
                case ClickHouseMap<TKey, TValue> cmap:
                    entries ??= new IReadOnlyList<KeyValuePair<TKey, TValue>>?[values.Length];
                    entries[i] = cmap;
                    count = cmap.Count;
                    allDictionary = false;
                    break;
                case KeyValuePair<TKey, TValue>[] arr:
                    entries ??= new IReadOnlyList<KeyValuePair<TKey, TValue>>?[values.Length];
                    entries[i] = arr;
                    count = arr.Length;
                    allDictionary = false;
                    break;
                case IReadOnlyList<KeyValuePair<TKey, TValue>> rolist:
                    entries ??= new IReadOnlyList<KeyValuePair<TKey, TValue>>?[values.Length];
                    entries[i] = rolist;
                    count = rolist.Count;
                    allDictionary = false;
                    break;
                case IList<KeyValuePair<TKey, TValue>> ilist:
                {
                    // Defensive copy: a custom IList<KVP> may not also implement IReadOnlyList<KVP>.
                    var buf = new KeyValuePair<TKey, TValue>[ilist.Count];
                    for (int j = 0; j < buf.Length; j++) buf[j] = ilist[j];
                    entries ??= new IReadOnlyList<KeyValuePair<TKey, TValue>>?[values.Length];
                    entries[i] = buf;
                    count = buf.Length;
                    allDictionary = false;
                    break;
                }
                case IEnumerable<KeyValuePair<TKey, TValue>> kvpEnumerable:
                {
                    var buf = new List<KeyValuePair<TKey, TValue>>();
                    foreach (var kvp in kvpEnumerable) buf.Add(kvp);
                    entries ??= new IReadOnlyList<KeyValuePair<TKey, TValue>>?[values.Length];
                    entries[i] = buf;
                    count = buf.Count;
                    allDictionary = false;
                    break;
                }
                case System.Collections.IDictionary d:
                    legacyDicts ??= new System.Collections.IDictionary?[values.Length];
                    legacyDicts[i] = d;
                    hasLegacyDict = true;
                    count = d.Count;
                    allDictionary = false;
                    break;
                default:
                    throw new InvalidOperationException(
                        $"{MapTypeStr} received unsupported value type {raw.GetType().Name} at row {i}. " +
                        "Expected Dictionary, ClickHouseMap, KeyValuePair[], IReadOnlyList/IList/IEnumerable<KeyValuePair>, or IDictionary.");
            }
            offset += (ulong)count;
            totalEntries += count;
            writer.WriteUInt64(offset);
        }

        // Pure-Dictionary fast path: bulk-write keys then values directly off the
        // typed Dictionary — no per-row buffer allocation. Matches the original
        // pre-entries-shape behaviour.
        if (allDictionary)
        {
            var allKeys = new TKey[totalEntries];
            var allValues = new TValue[totalEntries];
            int pos = 0;
            for (int i = 0; i < values.Length; i++)
            {
                foreach (var kvp in dicts![i]!)
                {
                    allKeys[pos] = kvp.Key;
                    allValues[pos] = kvp.Value;
                    pos++;
                }
            }
            _keyWriter.WriteColumn(ref writer, allKeys);
            _valueWriter.WriteColumn(ref writer, allValues);
            return;
        }

        // Mixed-but-no-legacy path: every row is either Dictionary or a known
        // entries-shape, so flatten once and use the bulk inner writers. Preserves
        // per-column headers such as NullableColumnWriter's null bitmap.
        if (!hasLegacyDict)
        {
            var allKeys = new TKey[totalEntries];
            var allValues = new TValue[totalEntries];
            int pos = 0;
            for (int i = 0; i < values.Length; i++)
            {
                if (dicts is not null && dicts[i] is { } d)
                {
                    foreach (var kvp in d)
                    {
                        allKeys[pos] = kvp.Key;
                        allValues[pos] = kvp.Value;
                        pos++;
                    }
                }
                else
                {
                    var rowEntries = entries![i]!;
                    for (int j = 0; j < rowEntries.Count; j++)
                    {
                        var kvp = rowEntries[j];
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

        // Compat path: at least one row was a non-generic IDictionary. Iterate via
        // the non-generic per-value writer. Only safe when the inner writers don't
        // emit per-column state prefixes (true for String / FixedString / numerics;
        // not true for Nullable / LowCardinality at inner).
        IColumnWriter keyWriterNg = _keyWriter;
        IColumnWriter valueWriterNg = _valueWriter;
        for (int i = 0; i < values.Length; i++)
        {
            if (dicts is not null && dicts[i] is { } d)
            {
                foreach (var kvp in d)
                    keyWriterNg.WriteValue(ref writer, kvp.Key);
            }
            else if (entries is not null && entries[i] is { } rowEntries)
            {
                for (int j = 0; j < rowEntries.Count; j++)
                    keyWriterNg.WriteValue(ref writer, rowEntries[j].Key);
            }
            else if (legacyDicts![i] is { } dict)
            {
                foreach (System.Collections.DictionaryEntry entry in dict)
                    keyWriterNg.WriteValue(ref writer, entry.Key);
            }
        }
        for (int i = 0; i < values.Length; i++)
        {
            if (dicts is not null && dicts[i] is { } d)
            {
                foreach (var kvp in d)
                    valueWriterNg.WriteValue(ref writer, kvp.Value);
            }
            else if (entries is not null && entries[i] is { } rowEntries)
            {
                for (int j = 0; j < rowEntries.Count; j++)
                    valueWriterNg.WriteValue(ref writer, rowEntries[j].Value);
            }
            else if (legacyDicts![i] is { } dict)
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
            case ClickHouseMap<TKey, TValue> cmap:
                WriteEntriesValue(ref writer, cmap);
                break;
            case KeyValuePair<TKey, TValue>[] arr:
                WriteEntriesValue(ref writer, arr);
                break;
            case IReadOnlyList<KeyValuePair<TKey, TValue>> rolist:
                WriteEntriesValue(ref writer, rolist);
                break;
            case IEnumerable<KeyValuePair<TKey, TValue>> kvpEnumerable:
            {
                var buf = new List<KeyValuePair<TKey, TValue>>();
                foreach (var kvp in kvpEnumerable) buf.Add(kvp);
                WriteEntriesValue(ref writer, buf);
                break;
            }
            default:
                throw new InvalidOperationException(
                    $"{MapTypeStr} received unsupported value type {value.GetType().Name}. " +
                    "Expected Dictionary, ClickHouseMap, KeyValuePair[], or IReadOnlyList/IList/IEnumerable<KeyValuePair>.");
        }
    }

    private void WriteEntriesValue(ref ProtocolWriter writer, IReadOnlyList<KeyValuePair<TKey, TValue>> entries)
    {
        writer.WriteUInt64((ulong)entries.Count);
        for (int i = 0; i < entries.Count; i++)
        {
            _keyWriter.WriteValue(ref writer, entries[i].Key);
        }
        for (int i = 0; i < entries.Count; i++)
        {
            _valueWriter.WriteValue(ref writer, entries[i].Value);
        }
    }

    private static string MapTypeStr => $"MapColumnWriter<{typeof(TKey).Name}, {typeof(TValue).Name}>";

    private static InvalidOperationException NullAt(int rowIndex)
    {
        var where = rowIndex >= 0 ? $" at row {rowIndex}" : string.Empty;
        return new InvalidOperationException(
            $"MapColumnWriter<{typeof(TKey).Name}, {typeof(TValue).Name}> received null{where}. The Map column type " +
            $"is non-nullable; declare the column as Nullable(Map({typeof(TKey).Name}, {typeof(TValue).Name})) and wrap " +
            $"this writer with NullableRefColumnWriter, or ensure source values are non-null.");
    }
}
