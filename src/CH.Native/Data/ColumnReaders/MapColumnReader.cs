using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for Map(K, V) values.
/// </summary>
/// <remarks>
/// Wire format: Same as Array(Tuple(K, V))
/// 1. Offsets array (VarInt cumulative counts)
/// 2. All keys concatenated
/// 3. All values concatenated
/// </remarks>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
internal sealed class MapColumnReader<TKey, TValue> : IColumnReader<Dictionary<TKey, TValue>>
    where TKey : notnull
{
    private readonly IColumnReader<TKey> _keyReader;
    private readonly IColumnReader<TValue> _valueReader;

    /// <summary>
    /// Creates a Map reader with the specified key and value readers.
    /// </summary>
    /// <param name="keyReader">Reader for map keys.</param>
    /// <param name="valueReader">Reader for map values.</param>
    public MapColumnReader(IColumnReader<TKey> keyReader, IColumnReader<TValue> valueReader)
    {
        _keyReader = keyReader ?? throw new ArgumentNullException(nameof(keyReader));
        _valueReader = valueReader ?? throw new ArgumentNullException(nameof(valueReader));
    }

    /// <summary>
    /// Creates a Map reader from non-generic IColumnReaders.
    /// </summary>
    /// <param name="keyReader">Reader for map keys.</param>
    /// <param name="valueReader">Reader for map values.</param>
    public MapColumnReader(IColumnReader keyReader, IColumnReader valueReader)
    {
        if (keyReader is IColumnReader<TKey> typedKeyReader)
        {
            _keyReader = typedKeyReader;
        }
        else
        {
            throw new ArgumentException(
                $"Key reader must implement IColumnReader<{typeof(TKey).Name}>.",
                nameof(keyReader));
        }

        if (valueReader is IColumnReader<TValue> typedValueReader)
        {
            _valueReader = typedValueReader;
        }
        else
        {
            throw new ArgumentException(
                $"Value reader must implement IColumnReader<{typeof(TValue).Name}>.",
                nameof(valueReader));
        }
    }

    /// <inheritdoc />
    public string TypeName => $"Map({_keyReader.TypeName}, {_valueReader.TypeName})";

    /// <inheritdoc />
    public Type ClrType => typeof(Dictionary<TKey, TValue>);

    /// <inheritdoc />
    public void ReadPrefix(ref ProtocolReader reader)
    {
        _keyReader.ReadPrefix(ref reader);
        _valueReader.ReadPrefix(ref reader);
    }

    /// <inheritdoc />
    public Dictionary<TKey, TValue> ReadValue(ref ProtocolReader reader)
    {
        // Single value uses UInt64 offset
        var count = reader.ReadUInt64AsInt32("Map entry count");
        var dict = new Dictionary<TKey, TValue>(count);

        if (count > 0)
        {
            using var keys = _keyReader.ReadTypedColumn(ref reader, count);
            using var values = _valueReader.ReadTypedColumn(ref reader, count);

            // Map keys are already deduplicated by the server in well-formed
            // blocks. If a corrupt / hostile stream emits a duplicate key, the
            // assignment below is last-wins by Dictionary semantics — silently
            // dropping the earlier value. We accept that rather than throwing,
            // matching server-side semantics, but document it here so a future
            // change to "throw on duplicate" is a deliberate choice.
            for (int i = 0; i < count; i++)
            {
                dict[keys[i]] = values[i];
            }
        }

        return dict;
    }

    /// <inheritdoc />
    public TypedColumn<Dictionary<TKey, TValue>> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        if (rowCount == 0)
            return new TypedColumn<Dictionary<TKey, TValue>>(Array.Empty<Dictionary<TKey, TValue>>());

        // Step 1: Read offsets (cumulative counts) - UInt64 per row
        var offsets = new ulong[rowCount];
        for (int i = 0; i < rowCount; i++)
        {
            offsets[i] = reader.ReadUInt64();
        }

        // Step 2: Calculate total entries
        var totalEntries = rowCount > 0 ? ProtocolGuards.ToInt32(offsets[rowCount - 1], "Map total entries") : 0;

        // Step 3: Read all keys then all values (columnar layout within the map)
        var result = new Dictionary<TKey, TValue>[rowCount];

        if (totalEntries > 0)
        {
            using var allKeys = _keyReader.ReadTypedColumn(ref reader, totalEntries);
            using var allValues = _valueReader.ReadTypedColumn(ref reader, totalEntries);

            // Step 4: Split into dictionaries per row
            var start = 0;
            for (int i = 0; i < rowCount; i++)
            {
                var end = ProtocolGuards.ToInt32(offsets[i], "Map row offset");
                var count = end - start;
                result[i] = new Dictionary<TKey, TValue>(count);

                for (int j = start; j < end; j++)
                {
                    result[i][allKeys[j]] = allValues[j];
                }
                start = end;
            }
        }
        else
        {
            for (int i = 0; i < rowCount; i++)
            {
                result[i] = new Dictionary<TKey, TValue>();
            }
        }

        return new TypedColumn<Dictionary<TKey, TValue>>(result);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
