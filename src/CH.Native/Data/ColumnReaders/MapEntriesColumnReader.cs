using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Lossless reader for <c>Map(K, V)</c> values. Produces
/// <see cref="ClickHouseMap{TKey, TValue}"/> instances preserving the entries'
/// wire order and any duplicate keys, in contrast to
/// <see cref="MapColumnReader{TKey, TValue}"/> which collapses to
/// <see cref="Dictionary{TKey, TValue}"/> with last-wins semantics.
/// </summary>
/// <remarks>
/// Wire format is identical to <see cref="MapColumnReader{TKey, TValue}"/>:
/// per-row UInt64 cumulative offsets, then a flat keys column, then a flat
/// values column.
/// </remarks>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
internal sealed class MapEntriesColumnReader<TKey, TValue> : IColumnReader<ClickHouseMap<TKey, TValue>>
    where TKey : notnull
{
    private readonly IColumnReader<TKey> _keyReader;
    private readonly IColumnReader<TValue> _valueReader;

    public MapEntriesColumnReader(IColumnReader<TKey> keyReader, IColumnReader<TValue> valueReader)
    {
        _keyReader = keyReader ?? throw new ArgumentNullException(nameof(keyReader));
        _valueReader = valueReader ?? throw new ArgumentNullException(nameof(valueReader));
    }

    public MapEntriesColumnReader(IColumnReader keyReader, IColumnReader valueReader)
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
    public Type ClrType => typeof(ClickHouseMap<TKey, TValue>);

    /// <inheritdoc />
    public void ReadPrefix(ref ProtocolReader reader)
    {
        _keyReader.ReadPrefix(ref reader);
        _valueReader.ReadPrefix(ref reader);
    }

    /// <inheritdoc />
    public ClickHouseMap<TKey, TValue> ReadValue(ref ProtocolReader reader)
    {
        var count = reader.ReadUInt64AsInt32("Map entry count");

        if (count == 0)
            return new ClickHouseMap<TKey, TValue>(Array.Empty<KeyValuePair<TKey, TValue>>());

        using var keys = _keyReader.ReadTypedColumn(ref reader, count);
        using var values = _valueReader.ReadTypedColumn(ref reader, count);

        var entries = new KeyValuePair<TKey, TValue>[count];
        for (int i = 0; i < count; i++)
        {
            entries[i] = new KeyValuePair<TKey, TValue>(keys[i], values[i]);
        }
        return new ClickHouseMap<TKey, TValue>(entries);
    }

    /// <inheritdoc />
    public TypedColumn<ClickHouseMap<TKey, TValue>> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        if (rowCount == 0)
            return new TypedColumn<ClickHouseMap<TKey, TValue>>(Array.Empty<ClickHouseMap<TKey, TValue>>());

        var offsets = new ulong[rowCount];
        for (int i = 0; i < rowCount; i++)
        {
            offsets[i] = reader.ReadUInt64();
        }

        var totalEntries = ProtocolGuards.ToInt32(offsets[rowCount - 1], "Map total entries");

        var result = new ClickHouseMap<TKey, TValue>[rowCount];

        if (totalEntries > 0)
        {
            using var allKeys = _keyReader.ReadTypedColumn(ref reader, totalEntries);
            using var allValues = _valueReader.ReadTypedColumn(ref reader, totalEntries);

            var start = 0;
            for (int i = 0; i < rowCount; i++)
            {
                var end = ProtocolGuards.ToInt32(offsets[i], "Map row offset");
                var count = end - start;
                if (count < 0)
                    throw new InvalidOperationException(
                        $"Map row {i} has negative entry count derived from non-monotonic offsets ({start} → {end}).");

                if (count == 0)
                {
                    result[i] = new ClickHouseMap<TKey, TValue>(Array.Empty<KeyValuePair<TKey, TValue>>());
                }
                else
                {
                    var entries = new KeyValuePair<TKey, TValue>[count];
                    for (int j = 0; j < count; j++)
                    {
                        entries[j] = new KeyValuePair<TKey, TValue>(allKeys[start + j], allValues[start + j]);
                    }
                    result[i] = new ClickHouseMap<TKey, TValue>(entries);
                }

                start = end;
            }
        }
        else
        {
            for (int i = 0; i < rowCount; i++)
            {
                result[i] = new ClickHouseMap<TKey, TValue>(Array.Empty<KeyValuePair<TKey, TValue>>());
            }
        }

        return new TypedColumn<ClickHouseMap<TKey, TValue>>(result);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
