using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for Array(T) values.
/// </summary>
/// <remarks>
/// Wire format:
/// 1. Offsets array: UInt64 cumulative counts for each row
/// 2. All element values concatenated
/// </remarks>
/// <typeparam name="T">The element type.</typeparam>
public sealed class ArrayColumnWriter<T> : IColumnWriter<T[]>
{
    private readonly IColumnWriter<T> _elementWriter;

    /// <summary>
    /// Creates an Array writer that uses the specified element writer.
    /// </summary>
    /// <param name="elementWriter">The writer for element values.</param>
    public ArrayColumnWriter(IColumnWriter<T> elementWriter)
    {
        _elementWriter = elementWriter ?? throw new ArgumentNullException(nameof(elementWriter));
    }

    /// <summary>
    /// Creates an Array writer from a non-generic IColumnWriter.
    /// </summary>
    /// <param name="elementWriter">The writer for element values.</param>
    public ArrayColumnWriter(IColumnWriter elementWriter)
    {
        if (elementWriter is IColumnWriter<T> typedWriter)
        {
            _elementWriter = typedWriter;
        }
        else
        {
            throw new ArgumentException(
                $"Element writer must implement IColumnWriter<{typeof(T).Name}>.",
                nameof(elementWriter));
        }
    }

    /// <inheritdoc />
    public string TypeName => $"Array({_elementWriter.TypeName})";

    /// <inheritdoc />
    public Type ClrType => typeof(T[]);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, T[][] values)
    {
        // Step 1: Write cumulative offsets (UInt64 per row)
        ulong offset = 0;
        for (int i = 0; i < values.Length; i++)
        {
            offset += (ulong)(values[i]?.Length ?? 0);
            writer.WriteUInt64(offset);
        }

        // Step 2: Write all elements concatenated
        for (int i = 0; i < values.Length; i++)
        {
            var array = values[i];
            if (array != null)
            {
                for (int j = 0; j < array.Length; j++)
                {
                    _elementWriter.WriteValue(ref writer, array[j]);
                }
            }
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, T[] value)
    {
        var length = value?.Length ?? 0;
        writer.WriteUInt64((ulong)length);

        if (value != null)
        {
            for (int i = 0; i < value.Length; i++)
            {
                _elementWriter.WriteValue(ref writer, value[i]);
            }
        }
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        // Step 1: Write cumulative offsets
        ulong offset = 0;
        for (int i = 0; i < values.Length; i++)
        {
            var array = values[i] as T[];
            offset += (ulong)(array?.Length ?? 0);
            writer.WriteUInt64(offset);
        }

        // Step 2: Write all elements
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is T[] array)
            {
                for (int j = 0; j < array.Length; j++)
                {
                    _elementWriter.WriteValue(ref writer, array[j]);
                }
            }
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        if (value is T[] array)
        {
            WriteValue(ref writer, array);
        }
        else
        {
            WriteValue(ref writer, Array.Empty<T>());
        }
    }
}
