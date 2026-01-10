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

        // Step 1: Read offsets (cumulative counts) - UInt64 per row
        var offsets = new ulong[rowCount];
        for (int i = 0; i < rowCount; i++)
        {
            offsets[i] = reader.ReadUInt64();
        }

        // Step 2: Calculate total elements and read them all
        var totalElements = rowCount > 0 ? (int)offsets[rowCount - 1] : 0;

        // Step 3: Split into arrays per row
        var result = new T[rowCount][];

        if (totalElements > 0)
        {
            using var allElements = _elementReader.ReadTypedColumn(ref reader, totalElements);

            var start = 0;
            for (int i = 0; i < rowCount; i++)
            {
                var end = (int)offsets[i];
                var length = end - start;
                result[i] = new T[length];
                for (int j = 0; j < length; j++)
                {
                    result[i][j] = allElements[start + j];
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

        return new TypedColumn<T[]>(result);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
