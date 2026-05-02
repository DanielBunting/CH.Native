using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for LowCardinality(T) values.
/// </summary>
/// <remarks>
/// LowCardinality uses dictionary encoding to efficiently store columns with few unique values.
///
/// Wire format:
/// 1. Version (UInt64) - serialization version/state
/// 2. Index type and flags (UInt64)
/// 3. Dictionary size (UInt64)
/// 4. Dictionary values (using inner reader for the base type)
/// 5. Index count (UInt64)
/// 6. Indices (using index type from flags)
///
/// For Nullable inner types, ClickHouse strips the Nullable wrapper from the dictionary.
/// Dictionary entry at index 0 represents null. The inner reader reads the base type directly.
///
/// The CLR type returned is the same as the inner type (dictionary encoding is transparent).
/// </remarks>
/// <typeparam name="T">The underlying type.</typeparam>
public sealed class LowCardinalityColumnReader<T> : IColumnReader<T>
{
    private readonly IColumnReader<T> _innerReader;
    private readonly bool _isNullable;
    private readonly string _typeName;

    // Index type constants from ClickHouse
    private const int IndexTypeUInt8 = 0;
    private const int IndexTypeUInt16 = 1;
    private const int IndexTypeUInt32 = 2;
    private const int IndexTypeUInt64 = 3;

    // Serialization flags
    private const ulong HasAdditionalKeysBit = 1UL << 9;
    private const ulong NeedGlobalDictionaryBit = 1UL << 10;
    private const ulong NeedUpdateDictionary = 1UL << 11;

    /// <summary>
    /// Creates a LowCardinality reader that wraps the specified inner reader.
    /// </summary>
    /// <param name="innerReader">The reader for the underlying type (base type, not Nullable).</param>
    /// <param name="isNullable">Whether the original type was LowCardinality(Nullable(T)).</param>
    public LowCardinalityColumnReader(IColumnReader<T> innerReader, bool isNullable = false)
    {
        _innerReader = innerReader ?? throw new ArgumentNullException(nameof(innerReader));
        _isNullable = isNullable;
        _typeName = isNullable
            ? $"LowCardinality(Nullable({_innerReader.TypeName}))"
            : $"LowCardinality({_innerReader.TypeName})";
    }

    /// <summary>
    /// Creates a LowCardinality reader from a non-generic IColumnReader.
    /// </summary>
    /// <param name="innerReader">The reader for the underlying type.</param>
    /// <param name="isNullable">Whether the original type was LowCardinality(Nullable(T)).</param>
    public LowCardinalityColumnReader(IColumnReader innerReader, bool isNullable = false)
    {
        if (innerReader is IColumnReader<T> typedReader)
        {
            _innerReader = typedReader;
        }
        else
        {
            throw new ArgumentException(
                $"Inner reader must implement IColumnReader<{typeof(T).Name}>.",
                nameof(innerReader));
        }
        _isNullable = isNullable;
        _typeName = isNullable
            ? $"LowCardinality(Nullable({_innerReader.TypeName}))"
            : $"LowCardinality({_innerReader.TypeName})";
    }

    /// <inheritdoc />
    public string TypeName => _typeName;

    /// <inheritdoc />
    public Type ClrType => typeof(T);

    /// <inheritdoc />
    public T ReadValue(ref ProtocolReader reader)
    {
        // Single value reading for LowCardinality is unusual in practice
        // but we'll support it by reading a 1-element column
        using var values = ReadTypedColumn(ref reader, 1);
        return values[0];
    }

    /// <inheritdoc />
    // Mirrors LowCardinalityColumnWriter.WritePrefix — the server sends the
    // KeysSerializationVersion UInt64 at the column-level state-prefix phase,
    // before any outer composite's structural bytes.
    public void ReadPrefix(ref ProtocolReader reader)
    {
        var version = reader.ReadUInt64();
        if (version != KeysSerializationVersion)
        {
            throw new InvalidDataException(
                $"Unsupported LowCardinality KeysSerializationVersion: {version} (expected {KeysSerializationVersion}).");
        }
    }

    private const ulong KeysSerializationVersion = 1;

    /// <inheritdoc />
    public TypedColumn<T> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        if (rowCount == 0)
            return new TypedColumn<T>(Array.Empty<T>());

        // Read index type and flags (version was consumed via ReadPrefix)
        var flags = reader.ReadUInt64();
        var indexType = (int)(flags & 0xFF);

        // Read dictionary size
        var dictSize = reader.ReadUInt64();

        // Read dictionary values - keep the column alive during index resolution
        using var dictColumn = dictSize > 0
            ? _innerReader.ReadTypedColumn(ref reader, ProtocolGuards.ToInt32(dictSize, "LowCardinality dictionary size"))
            : null;

        // Read number of indices (should match rowCount)
        var indexCount = reader.ReadUInt64();

        // Use pooled array for the result
        var pool = ArrayPool<T>.Shared;
        var result = pool.Rent(rowCount);

        // Read indices and resolve to actual values directly from dictColumn
        for (int i = 0; i < rowCount; i++)
        {
            var index = indexType switch
            {
                IndexTypeUInt8 => reader.ReadByte(),
                IndexTypeUInt16 => reader.ReadUInt16(),
                IndexTypeUInt32 => reader.ReadUInt32(),
                IndexTypeUInt64 => reader.ReadUInt64(),
                _ => throw new NotSupportedException($"Unknown LowCardinality index type: {indexType}")
            };

            // For Nullable LowCardinality, index 0 represents null
            if (_isNullable && index == 0)
            {
                result[i] = default!;
                continue;
            }

            var dictCount = dictColumn?.Count ?? 0;
            if (index >= (ulong)dictCount)
            {
                throw new InvalidDataException(
                    $"LowCardinality index {index} at row {i} is out of range (dictionary size = {dictCount}).");
            }

            // Bounds check above guarantees the cast fits int.
            result[i] = dictColumn![(int)index];
        }

        return new TypedColumn<T>(result, rowCount, pool);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        // Return optimized DictionaryEncodedColumn that preserves dictionary encoding
        return ReadDictionaryEncodedColumn(ref reader, rowCount);
    }

    /// <summary>
    /// Reads LowCardinality column data into a DictionaryEncodedColumn for optimal memory usage.
    /// This preserves dictionary encoding instead of expanding to full arrays.
    /// </summary>
    /// <param name="reader">The protocol reader.</param>
    /// <param name="rowCount">The number of rows.</param>
    /// <returns>A dictionary-encoded column with lazy per-row access.</returns>
    public DictionaryEncodedColumn<T> ReadDictionaryEncodedColumn(ref ProtocolReader reader, int rowCount)
    {
        if (rowCount == 0)
            return new DictionaryEncodedColumn<T>(Array.Empty<T>(), Array.Empty<int>(), 0, null);

        // Read index type and flags (version was consumed via ReadPrefix)
        var flags = reader.ReadUInt64();
        var indexType = (int)(flags & 0xFF);

        // Read dictionary size
        var dictSize = reader.ReadUInt64();

        // Read dictionary values (small, worth keeping as regular array)
        T[] dictionary;
        if (dictSize > 0)
        {
            var dictSizeInt = ProtocolGuards.ToInt32(dictSize, "LowCardinality dictionary size");
            using var dictColumn = _innerReader.ReadTypedColumn(ref reader, dictSizeInt);
            dictionary = new T[dictSizeInt];
            for (int i = 0; i < dictSizeInt; i++)
            {
                dictionary[i] = dictColumn[i];
            }
        }
        else
        {
            dictionary = Array.Empty<T>();
        }

        // Read number of indices (should match rowCount)
        var indexCount = reader.ReadUInt64();

        // Read indices into pooled array
        var indicesPool = ArrayPool<int>.Shared;
        var indices = indicesPool.Rent(rowCount);

        for (int i = 0; i < rowCount; i++)
        {
            indices[i] = indexType switch
            {
                IndexTypeUInt8 => reader.ReadByte(),
                IndexTypeUInt16 => reader.ReadUInt16(),
                IndexTypeUInt32 => reader.ReadUInt32AsInt32("LowCardinality index"),
                IndexTypeUInt64 => reader.ReadUInt64AsInt32("LowCardinality index"),
                _ => throw new NotSupportedException($"Unknown LowCardinality index type: {indexType}")
            };
        }

        return new DictionaryEncodedColumn<T>(dictionary, indices, rowCount, indicesPool, _isNullable);
    }
}
