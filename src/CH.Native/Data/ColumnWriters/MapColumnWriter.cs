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
        for (int i = 0; i < values.Length; i++)
        {
            offset += (ulong)(values[i]?.Count ?? 0);
            writer.WriteUInt64(offset);
        }

        // Step 2: Write all keys
        for (int i = 0; i < values.Length; i++)
        {
            var dict = values[i];
            if (dict != null)
            {
                foreach (var key in dict.Keys)
                {
                    _keyWriter.WriteValue(ref writer, key);
                }
            }
        }

        // Step 3: Write all values
        for (int i = 0; i < values.Length; i++)
        {
            var dict = values[i];
            if (dict != null)
            {
                foreach (var value in dict.Values)
                {
                    _valueWriter.WriteValue(ref writer, value);
                }
            }
        }
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
        // Step 1: Write offsets
        ulong offset = 0;
        for (int i = 0; i < values.Length; i++)
        {
            var dict = values[i] as Dictionary<TKey, TValue>;
            offset += (ulong)(dict?.Count ?? 0);
            writer.WriteUInt64(offset);
        }

        // Step 2: Write all keys
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is Dictionary<TKey, TValue> dict)
            {
                foreach (var key in dict.Keys)
                {
                    _keyWriter.WriteValue(ref writer, key);
                }
            }
        }

        // Step 3: Write all values
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is Dictionary<TKey, TValue> dict)
            {
                foreach (var value in dict.Values)
                {
                    _valueWriter.WriteValue(ref writer, value);
                }
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
