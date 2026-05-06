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
internal sealed class NullableColumnWriter<T> : IColumnWriter<T?>
    where T : struct
{
    private readonly IColumnWriter<T> _innerWriter;
    private readonly T _nullPlaceholder;

    /// <summary>
    /// Creates a Nullable writer that wraps the specified inner writer.
    /// </summary>
    /// <param name="innerWriter">The writer for the underlying type.</param>
    public NullableColumnWriter(IColumnWriter<T> innerWriter)
    {
        _innerWriter = innerWriter ?? throw new ArgumentNullException(nameof(innerWriter));
        _nullPlaceholder = ResolveNullPlaceholder(_innerWriter);
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
        _nullPlaceholder = ResolveNullPlaceholder(_innerWriter);
    }

    // Most value-type writers leave the interface default in place (which throws);
    // fall back to default(T) for those. Range-checked writers (DateTime,
    // DateTimeWithTimezone) override NullPlaceholder to return a benign in-range
    // value (Unix epoch) so the per-row write of a placeholder under a "null"
    // bitmap byte doesn't trip their range guard.
    private static T ResolveNullPlaceholder(IColumnWriter<T> innerWriter)
    {
        try { return innerWriter.NullPlaceholder; }
        catch (NotSupportedException) { return default; }
    }

    /// <inheritdoc />
    public string TypeName => $"Nullable({_innerWriter.TypeName})";

    /// <inheritdoc />
    public Type ClrType => typeof(T?);

    /// <inheritdoc />
    // Nullable itself has no state prefix (its null bitmap is per-row data), but an
    // inner LC still needs its KeysSerializationVersion emitted.
    public void WritePrefix(ref ProtocolWriter writer) => _innerWriter.WritePrefix(ref writer);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, T?[] values)
    {
        // Step 1: Write null bitmap (1 byte per row)
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteByte(values[i].HasValue ? (byte)0 : (byte)1);
        }

        // Step 2: Write all values, substituting the inner writer's declared
        // placeholder for null slots (default(T) by default; UnixEpoch for
        // range-checked DateTime writers).
        for (int i = 0; i < values.Length; i++)
        {
            _innerWriter.WriteValue(ref writer, values[i] ?? _nullPlaceholder);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, T? value)
    {
        writer.WriteByte(value.HasValue ? (byte)0 : (byte)1);
        _innerWriter.WriteValue(ref writer, value ?? _nullPlaceholder);
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
            _innerWriter.WriteValue(ref writer, values[i] is T v ? v : _nullPlaceholder);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        writer.WriteByte(value is null ? (byte)1 : (byte)0);
        _innerWriter.WriteValue(ref writer, value is T v ? v : _nullPlaceholder);
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
internal sealed class NullableRefColumnWriter<T> : IColumnWriter<T?>
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
    public void WritePrefix(ref ProtocolWriter writer) => _innerWriter.WritePrefix(ref writer);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, T?[] values)
    {
        // Step 1: Write null bitmap (1 byte per row)
        bool anyNull = false;
        for (int i = 0; i < values.Length; i++)
        {
            var isNull = values[i] is null;
            anyNull |= isNull;
            writer.WriteByte(isNull ? (byte)1 : (byte)0);
        }

        // Step 2: Delegate to the inner column writer so composite inner types
        // (Array, Map, ...) emit their columnar offset blocks rather than per-row
        // framing. The inner is now strict on null (the boxed-path fix), so we
        // must substitute its declared NullPlaceholder for any null slot before
        // delegating. The no-null fast path skips the substitution allocation.
        if (!anyNull)
        {
            _innerWriter.WriteColumn(ref writer, values!);
            return;
        }

        var placeholder = _innerWriter.NullPlaceholder;
        var substituted = new T[values.Length];
        for (int i = 0; i < values.Length; i++)
            substituted[i] = values[i] ?? placeholder;
        _innerWriter.WriteColumn(ref writer, substituted);
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, T? value)
    {
        var isNull = value is null;
        writer.WriteByte(isNull ? (byte)1 : (byte)0);
        _innerWriter.WriteValue(ref writer, isNull ? _innerWriter.NullPlaceholder : value!);
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        // Step 1: Write null bitmap
        bool anyNull = false;
        for (int i = 0; i < values.Length; i++)
        {
            var isNull = values[i] is null;
            anyNull |= isNull;
            writer.WriteByte(isNull ? (byte)1 : (byte)0);
        }

        // Step 2: Delegate to the inner writer's non-generic WriteColumn so
        // composite inner types emit their proper columnar layout. Substitute
        // the inner's NullPlaceholder (boxed) for any null slot — the inner is
        // now strict on null in its non-generic path too.
        if (!anyNull)
        {
            ((IColumnWriter)_innerWriter).WriteColumn(ref writer, values);
            return;
        }

        object placeholder = _innerWriter.NullPlaceholder!;
        var substituted = new object?[values.Length];
        for (int i = 0; i < values.Length; i++)
            substituted[i] = values[i] ?? placeholder;
        ((IColumnWriter)_innerWriter).WriteColumn(ref writer, substituted);
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        var isNull = value is null;
        writer.WriteByte(isNull ? (byte)1 : (byte)0);
        if (isNull)
            ((IColumnWriter)_innerWriter).WriteValue(ref writer, _innerWriter.NullPlaceholder);
        else
            _innerWriter.WriteValue(ref writer, (value as T)!);
    }
}
