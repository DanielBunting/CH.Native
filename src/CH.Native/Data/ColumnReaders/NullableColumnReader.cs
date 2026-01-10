using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for Nullable(T) where T is a value type.
/// </summary>
/// <remarks>
/// Wire format:
/// 1. Null bitmap: 1 byte per row (0 = not null, 1 = null)
/// 2. All values (including slots for null rows, which have undefined content)
/// </remarks>
/// <typeparam name="T">The underlying value type.</typeparam>
public sealed class NullableColumnReader<T> : IColumnReader<T?>
    where T : struct
{
    private readonly IColumnReader<T> _innerReader;

    /// <summary>
    /// Creates a Nullable reader that wraps the specified inner reader.
    /// </summary>
    /// <param name="innerReader">The reader for the underlying type.</param>
    public NullableColumnReader(IColumnReader<T> innerReader)
    {
        _innerReader = innerReader ?? throw new ArgumentNullException(nameof(innerReader));
    }

    /// <summary>
    /// Creates a Nullable reader from a non-generic IColumnReader.
    /// </summary>
    /// <param name="innerReader">The reader for the underlying type.</param>
    public NullableColumnReader(IColumnReader innerReader)
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
    public string TypeName => $"Nullable({_innerReader.TypeName})";

    /// <inheritdoc />
    public Type ClrType => typeof(T?);

    /// <inheritdoc />
    public T? ReadValue(ref ProtocolReader reader)
    {
        var isNull = reader.ReadByte() != 0;
        var value = _innerReader.ReadValue(ref reader);
        return isNull ? null : value;
    }

    /// <inheritdoc />
    public TypedColumn<T?> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        // Step 1: Read null bitmap (1 byte per row)
        var nullBitmap = new bool[rowCount];
        for (int i = 0; i < rowCount; i++)
        {
            nullBitmap[i] = reader.ReadByte() != 0;
        }

        // Step 2: Read ALL values (including slots for null rows)
        using var innerValues = _innerReader.ReadTypedColumn(ref reader, rowCount);

        // Step 3: Apply null mask
        var result = new T?[rowCount];
        for (int i = 0; i < rowCount; i++)
        {
            result[i] = nullBitmap[i] ? null : innerValues[i];
        }

        return new TypedColumn<T?>(result);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}

/// <summary>
/// Column reader for Nullable(T) where T is a reference type.
/// </summary>
/// <remarks>
/// Wire format is the same as value type Nullable:
/// 1. Null bitmap: 1 byte per row (0 = not null, 1 = null)
/// 2. All values (including slots for null rows)
/// </remarks>
/// <typeparam name="T">The underlying reference type.</typeparam>
public sealed class NullableRefColumnReader<T> : IColumnReader<T?>
    where T : class
{
    private readonly IColumnReader<T> _innerReader;

    /// <summary>
    /// Creates a Nullable reader that wraps the specified inner reader.
    /// </summary>
    /// <param name="innerReader">The reader for the underlying type.</param>
    public NullableRefColumnReader(IColumnReader<T> innerReader)
    {
        _innerReader = innerReader ?? throw new ArgumentNullException(nameof(innerReader));
    }

    /// <summary>
    /// Creates a Nullable reader from a non-generic IColumnReader.
    /// </summary>
    /// <param name="innerReader">The reader for the underlying type.</param>
    public NullableRefColumnReader(IColumnReader innerReader)
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
    public string TypeName => $"Nullable({_innerReader.TypeName})";

    /// <inheritdoc />
    public Type ClrType => typeof(T);

    /// <inheritdoc />
    public T? ReadValue(ref ProtocolReader reader)
    {
        var isNull = reader.ReadByte() != 0;
        var value = _innerReader.ReadValue(ref reader);
        return isNull ? null : value;
    }

    /// <inheritdoc />
    public TypedColumn<T?> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        // Step 1: Read null bitmap (1 byte per row)
        var nullBitmap = new bool[rowCount];
        for (int i = 0; i < rowCount; i++)
        {
            nullBitmap[i] = reader.ReadByte() != 0;
        }

        // Step 2: Read ALL values (including slots for null rows)
        using var innerValues = _innerReader.ReadTypedColumn(ref reader, rowCount);

        // Step 3: Apply null mask
        var result = new T?[rowCount];
        for (int i = 0; i < rowCount; i++)
        {
            result[i] = nullBitmap[i] ? null : innerValues[i];
        }

        return new TypedColumn<T?>(result);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
