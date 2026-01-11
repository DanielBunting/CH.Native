using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for Nullable(T) where T is a value type.
/// </summary>
/// <remarks>
/// Wire format:
/// 1. Null bitmap: 1 byte per row (0 = not null, 1 = null)
/// 2. All values (default values written for null slots)
/// </remarks>
/// <typeparam name="T">The underlying value type.</typeparam>
public sealed class NullableColumnWriter<T> : IColumnWriter<T?>
    where T : struct
{
    private readonly IColumnWriter<T> _innerWriter;

    /// <summary>
    /// Creates a Nullable writer that wraps the specified inner writer.
    /// </summary>
    /// <param name="innerWriter">The writer for the underlying type.</param>
    public NullableColumnWriter(IColumnWriter<T> innerWriter)
    {
        _innerWriter = innerWriter ?? throw new ArgumentNullException(nameof(innerWriter));
    }

    /// <summary>
    /// Creates a Nullable writer from a non-generic IColumnWriter.
    /// </summary>
    /// <param name="innerWriter">The writer for the underlying type.</param>
    public NullableColumnWriter(IColumnWriter innerWriter)
    {
        if (innerWriter is IColumnWriter<T> typedWriter)
        {
            _innerWriter = typedWriter;
        }
        else
        {
            throw new ArgumentException(
                $"Inner writer must implement IColumnWriter<{typeof(T).Name}>.",
                nameof(innerWriter));
        }
    }

    /// <inheritdoc />
    public string TypeName => $"Nullable({_innerWriter.TypeName})";

    /// <inheritdoc />
    public Type ClrType => typeof(T?);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, T?[] values)
    {
        // Step 1: Write null bitmap (1 byte per row)
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteByte(values[i].HasValue ? (byte)0 : (byte)1);
        }

        // Step 2: Write all values (default for nulls)
        for (int i = 0; i < values.Length; i++)
        {
            _innerWriter.WriteValue(ref writer, values[i] ?? default);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, T? value)
    {
        writer.WriteByte(value.HasValue ? (byte)0 : (byte)1);
        _innerWriter.WriteValue(ref writer, value ?? default);
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        // Step 1: Write null bitmap
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteByte(values[i] is null ? (byte)1 : (byte)0);
        }

        // Step 2: Write all values
        for (int i = 0; i < values.Length; i++)
        {
            _innerWriter.WriteValue(ref writer, values[i] is T v ? v : default);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        writer.WriteByte(value is null ? (byte)1 : (byte)0);
        _innerWriter.WriteValue(ref writer, value is T v ? v : default);
    }
}

/// <summary>
/// Column writer for Nullable(T) where T is a reference type.
/// </summary>
/// <remarks>
/// Wire format is the same as value type Nullable:
/// 1. Null bitmap: 1 byte per row (0 = not null, 1 = null)
/// 2. All values (default values written for null slots)
/// </remarks>
/// <typeparam name="T">The underlying reference type.</typeparam>
public sealed class NullableRefColumnWriter<T> : IColumnWriter<T?>
    where T : class
{
    private readonly IColumnWriter<T> _innerWriter;

    /// <summary>
    /// Creates a Nullable writer that wraps the specified inner writer.
    /// </summary>
    /// <param name="innerWriter">The writer for the underlying type.</param>
    public NullableRefColumnWriter(IColumnWriter<T> innerWriter)
    {
        _innerWriter = innerWriter ?? throw new ArgumentNullException(nameof(innerWriter));
    }

    /// <summary>
    /// Creates a Nullable writer from a non-generic IColumnWriter.
    /// </summary>
    /// <param name="innerWriter">The writer for the underlying type.</param>
    public NullableRefColumnWriter(IColumnWriter innerWriter)
    {
        if (innerWriter is IColumnWriter<T> typedWriter)
        {
            _innerWriter = typedWriter;
        }
        else
        {
            throw new ArgumentException(
                $"Inner writer must implement IColumnWriter<{typeof(T).Name}>.",
                nameof(innerWriter));
        }
    }

    /// <inheritdoc />
    public string TypeName => $"Nullable({_innerWriter.TypeName})";

    /// <inheritdoc />
    public Type ClrType => typeof(T);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, T?[] values)
    {
        // Step 1: Write null bitmap (1 byte per row)
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteByte(values[i] is null ? (byte)1 : (byte)0);
        }

        // Step 2: Write all values (default/empty for nulls)
        for (int i = 0; i < values.Length; i++)
        {
            _innerWriter.WriteValue(ref writer, values[i]!);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, T? value)
    {
        writer.WriteByte(value is null ? (byte)1 : (byte)0);
        _innerWriter.WriteValue(ref writer, value!);
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        // Step 1: Write null bitmap
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteByte(values[i] is null ? (byte)1 : (byte)0);
        }

        // Step 2: Write all values
        for (int i = 0; i < values.Length; i++)
        {
            _innerWriter.WriteValue(ref writer, (values[i] as T)!);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        writer.WriteByte(value is null ? (byte)1 : (byte)0);
        _innerWriter.WriteValue(ref writer, (value as T)!);
    }
}
