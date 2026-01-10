using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for Array(T) values.
/// </summary>
/// <remarks>
/// Wire format:
/// 1. Offsets array: VarInt cumulative counts for each row
/// 2. All element values concatenated
/// </remarks>
/// <typeparam name="T">The element type.</typeparam>
public sealed class ArrayColumnReader<T> : IColumnReader<T[]>
{
    private readonly IColumnReader<T> _elementReader;

    /// <summary>
    /// Creates an Array reader that uses the specified element reader.
    /// </summary>
    /// <param name="elementReader">The reader for element values.</param>
    public ArrayColumnReader(IColumnReader<T> elementReader)
    {
        _elementReader = elementReader ?? throw new ArgumentNullException(nameof(elementReader));
    }

    /// <summary>
    /// Creates an Array reader from a non-generic IColumnReader.
    /// </summary>
    /// <param name="elementReader">The reader for element values.</param>
    public ArrayColumnReader(IColumnReader elementReader)
    {
        if (elementReader is IColumnReader<T> typedReader)
        {
            _elementReader = typedReader;
        }
        else
        {
            throw new ArgumentException(
                $"Element reader must implement IColumnReader<{typeof(T).Name}>.",
                nameof(elementReader));
        }
    }

    /// <inheritdoc />
    public string TypeName => $"Array({_elementReader.TypeName})";

    /// <inheritdoc />
    public Type ClrType => typeof(T[]);

    /// <inheritdoc />
    public T[] ReadValue(ref ProtocolReader reader)
    {
        // Single value reading uses UInt64 offset format too
        var offset = reader.ReadUInt64();
        if (offset == 0)
            return Array.Empty<T>();

        using var elements = _elementReader.ReadTypedColumn(ref reader, (int)offset);
        var result = new T[(int)offset];
        for (int i = 0; i < (int)offset; i++)
        {
            result[i] = elements[i];
        }
        return result;
    }

    /// <inheritdoc />
    public TypedColumn<T[]> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        if (rowCount == 0)
            return new TypedColumn<T[]>(Array.Empty<T[]>());

        // Step 1: Read offsets (cumulative counts) into pooled array - UInt64 per row
        var offsetsPool = ArrayPool<int>.Shared;
        var offsets = offsetsPool.Rent(rowCount);

        try
        {
            for (int i = 0; i < rowCount; i++)
            {
                offsets[i] = (int)reader.ReadUInt64();
            }

            // Step 2: Calculate total elements and read them all
            var totalElements = rowCount > 0 ? offsets[rowCount - 1] : 0;

            // Step 3: Use pooled array for result
            var resultPool = ArrayPool<T[]>.Shared;
            var result = resultPool.Rent(rowCount);

            if (totalElements > 0)
            {
                using var allElements = _elementReader.ReadTypedColumn(ref reader, totalElements);
                var elementsSpan = allElements.Values;

                var start = 0;
                for (int i = 0; i < rowCount; i++)
                {
                    var end = offsets[i];
                    var length = end - start;

                    if (length == 0)
                    {
                        result[i] = Array.Empty<T>();
                    }
                    else
                    {
                        result[i] = new T[length];
                        elementsSpan.Slice(start, length).CopyTo(result[i]);
                    }
                    start = end;
                }
            }
            else
            {
                for (int i = 0; i < rowCount; i++)
                {
                    result[i] = Array.Empty<T>();
                }
            }

            return new TypedColumn<T[]>(result, rowCount, resultPool);
        }
        finally
        {
            offsetsPool.Return(offsets);
        }
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        // Return optimized FlattenedArrayColumn that avoids per-row allocation
        return ReadFlattenedColumn(ref reader, rowCount);
    }

    /// <summary>
    /// Reads array column data into a FlattenedArrayColumn for optimal memory usage.
    /// This avoids allocating a separate T[] for each row.
    /// </summary>
    /// <param name="reader">The protocol reader.</param>
    /// <param name="rowCount">The number of rows.</param>
    /// <returns>A flattened array column with lazy per-row access.</returns>
    public FlattenedArrayColumn<T> ReadFlattenedColumn(ref ProtocolReader reader, int rowCount)
    {
        if (rowCount == 0)
            return new FlattenedArrayColumn<T>(
                Array.Empty<T>(), 0,
                Array.Empty<int>(), 0,
                null, null);

        // Step 1: Read offsets (cumulative counts) into pooled array
        var offsetsPool = ArrayPool<int>.Shared;
        var offsets = offsetsPool.Rent(rowCount);

        for (int i = 0; i < rowCount; i++)
        {
            offsets[i] = (int)reader.ReadUInt64();
        }

        var totalElements = rowCount > 0 ? offsets[rowCount - 1] : 0;

        if (totalElements == 0)
        {
            // All arrays are empty
            return new FlattenedArrayColumn<T>(
                Array.Empty<T>(), 0,
                offsets, rowCount,
                null, offsetsPool);
        }

        // Step 2: Read all elements at once
        using var innerColumn = _elementReader.ReadTypedColumn(ref reader, totalElements);

        // Step 3: Copy elements to our pooled array
        var elementsPool = ArrayPool<T>.Shared;
        var elements = elementsPool.Rent(totalElements);
        innerColumn.Values.CopyTo(elements);

        return new FlattenedArrayColumn<T>(
            elements, totalElements,
            offsets, rowCount,
            elementsPool, offsetsPool);
    }
}
