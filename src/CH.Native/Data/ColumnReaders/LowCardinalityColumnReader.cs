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
/// 4. Dictionary values (using inner reader)
/// 5. Index count (UInt64)
/// 6. Indices (using index type from flags)
///
/// The CLR type returned is the same as the inner type (dictionary encoding is transparent).
/// </remarks>
/// <typeparam name="T">The underlying type.</typeparam>
public sealed class LowCardinalityColumnReader<T> : IColumnReader<T>
{
    private readonly IColumnReader<T> _innerReader;

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
    /// <param name="innerReader">The reader for the underlying type.</param>
    public LowCardinalityColumnReader(IColumnReader<T> innerReader)
    {
        _innerReader = innerReader ?? throw new ArgumentNullException(nameof(innerReader));
    }

    /// <summary>
    /// Creates a LowCardinality reader from a non-generic IColumnReader.
    /// </summary>
    /// <param name="innerReader">The reader for the underlying type.</param>
    public LowCardinalityColumnReader(IColumnReader innerReader)
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
    }

    /// <inheritdoc />
    public string TypeName => $"LowCardinality({_innerReader.TypeName})";

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
    public TypedColumn<T> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        if (rowCount == 0)
            return new TypedColumn<T>(Array.Empty<T>());

        // Read serialization state/version
        var version = reader.ReadUInt64();

        // Read index type and flags
        var flags = reader.ReadUInt64();
        var indexType = (int)(flags & 0xFF);

        // Read dictionary size
        var dictSize = reader.ReadUInt64();

        // Read dictionary values - keep the column alive during index resolution
        using var dictColumn = dictSize > 0
            ? _innerReader.ReadTypedColumn(ref reader, (int)dictSize)
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

            // Resolve dictionary lookup directly from the inner column
            if (dictColumn != null && index < (ulong)dictColumn.Count)
            {
                result[i] = dictColumn[(int)index];
            }
            else
            {
                // Index out of range or empty dictionary - this shouldn't happen with valid data
                result[i] = default!;
            }
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

        // Read serialization state/version
        var version = reader.ReadUInt64();

        // Read index type and flags
        var flags = reader.ReadUInt64();
        var indexType = (int)(flags & 0xFF);

        // Read dictionary size
        var dictSize = reader.ReadUInt64();

        // Read dictionary values (small, worth keeping as regular array)
        T[] dictionary;
        if (dictSize > 0)
        {
            using var dictColumn = _innerReader.ReadTypedColumn(ref reader, (int)dictSize);
            dictionary = new T[(int)dictSize];
            for (int i = 0; i < (int)dictSize; i++)
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
                IndexTypeUInt32 => (int)reader.ReadUInt32(),
                IndexTypeUInt64 => (int)reader.ReadUInt64(),
                _ => throw new NotSupportedException($"Unknown LowCardinality index type: {indexType}")
            };
        }

        return new DictionaryEncodedColumn<T>(dictionary, indices, rowCount, indicesPool);
    }
}
