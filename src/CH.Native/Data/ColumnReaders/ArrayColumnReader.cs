using System.Buffers;
using CH.Native.Exceptions;
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
internal sealed class ArrayColumnReader<T> : IColumnReader<T[]>
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
    public void ReadPrefix(ref ProtocolReader reader) => _elementReader.ReadPrefix(ref reader);

    /// <inheritdoc />
    public T[] ReadValue(ref ProtocolReader reader)
    {
        // Single value reading uses UInt64 offset format too
        var offset = reader.ReadUInt64AsInt32("Array offset");
        if (offset == 0)
            return Array.Empty<T>();

        using var elements = _elementReader.ReadTypedColumn(ref reader, offset);
        var result = new T[offset];
        for (int i = 0; i < offset; i++)
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
            int previous = 0;
            for (int i = 0; i < rowCount; i++)
            {
                var offset = reader.ReadUInt64AsInt32("Array offset");
                if (offset < previous)
                {
                    throw new ClickHouseProtocolException(
                        $"Array offset at row {i} ({offset}) is less than previous cumulative offset ({previous}); offsets must be monotonically non-decreasing.");
                }
                offsets[i] = offset;
                previous = offset;
            }

            // Step 2: Calculate total elements and read them all
            var totalElements = rowCount > 0 ? offsets[rowCount - 1] : 0;

            // Step 3: Use pooled array for result
            var resultPool = ArrayPool<T[]>.Shared;
            var result = resultPool.Rent(rowCount);

            try
            {
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
            catch
            {
                // Element-reader threw on malformed inner data — return the
                // result array to the pool before propagating, otherwise the
                // pool slowly fills with abandoned arrays under repeated
                // hostile/corrupt input.
                resultPool.Return(result, clearArray: true);
                throw;
            }
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

        // Step 1: Read offsets (cumulative counts) into pooled array.
        // Wire reads (ReadUInt64AsInt32) can throw on end-of-stream or guard
        // violations; we own the rented buffer until ownership transfers to
        // the FlattenedArrayColumn at the bottom.
        var offsetsPool = ArrayPool<int>.Shared;
        var offsets = offsetsPool.Rent(rowCount);

        try
        {
            int previousFlat = 0;
            for (int i = 0; i < rowCount; i++)
            {
                var offset = reader.ReadUInt64AsInt32("Array offset");
                if (offset < previousFlat)
                {
                    throw new ClickHouseProtocolException(
                        $"Array offset at row {i} ({offset}) is less than previous cumulative offset ({previousFlat}); offsets must be monotonically non-decreasing.");
                }
                offsets[i] = offset;
                previousFlat = offset;
            }

            var totalElements = rowCount > 0 ? offsets[rowCount - 1] : 0;

            if (totalElements == 0)
            {
                // All arrays are empty — ownership of `offsets` transfers to
                // the FlattenedArrayColumn (it'll return them on Dispose).
                var col = new FlattenedArrayColumn<T>(
                    Array.Empty<T>(), 0,
                    offsets, rowCount,
                    null, offsetsPool);
                offsets = null!; // ownership transferred; suppress finally-return.
                return col;
            }

            // Step 2: Read all elements at once.
            using var innerColumn = _elementReader.ReadTypedColumn(ref reader, totalElements);

            // Step 3: Copy elements to our pooled array.
            var elementsPool = ArrayPool<T>.Shared;
            var elements = elementsPool.Rent(totalElements);
            try
            {
                innerColumn.Values.CopyTo(elements);
            }
            catch
            {
                // CopyTo on aligned spans rarely throws, but if it does,
                // return the elements buffer — the offsets buffer is
                // handled by the outer finally.
                elementsPool.Return(elements, clearArray: true);
                throw;
            }

            // Ownership of both pooled buffers now transfers to FlattenedArrayColumn.
            var transferred = new FlattenedArrayColumn<T>(
                elements, totalElements,
                offsets, rowCount,
                elementsPool, offsetsPool);
            offsets = null!; // ownership transferred; suppress finally-return.
            return transferred;
        }
        finally
        {
            // Returns the offsets buffer if ownership wasn't transferred (i.e. an
            // exception escaped before we returned a FlattenedArrayColumn that
            // holds the buffer). The two ownership-transfer points above null
            // out `offsets` so this finally is a no-op on the success path.
            if (offsets is not null)
                offsetsPool.Return(offsets, clearArray: true);
        }
    }
}
