using System.Text;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for FixedString(N) values.
/// FixedString stores exactly N bytes per value, padded with null bytes if needed.
/// </summary>
/// <remarks>
/// This writer is strict: a null value into any of its write paths throws
/// <see cref="InvalidOperationException"/>. For Nullable(FixedString) columns
/// the caller must wrap with <see cref="NullableRefColumnWriter{T}"/>, which
/// substitutes <see cref="NullPlaceholder"/> (an empty <c>byte[]</c> that
/// <see cref="WriteValue"/> pads to N zero bytes) for null slots.
/// </remarks>
internal sealed class FixedStringColumnWriter : IColumnWriter<byte[]>
{
    private readonly int _length;

    /// <summary>
    /// Creates a FixedString writer for the specified length.
    /// </summary>
    /// <param name="length">The fixed string length in bytes.</param>
    public FixedStringColumnWriter(int length)
    {
        if (length <= 0)
            throw new ArgumentOutOfRangeException(nameof(length), "FixedString length must be positive.");

        _length = length;
    }

    /// <inheritdoc />
    public string TypeName => $"FixedString({_length})";

    /// <inheritdoc />
    public Type ClrType => typeof(byte[]);

    /// <summary>
    /// Gets the fixed string length in bytes.
    /// </summary>
    public int Length => _length;

    /// <inheritdoc />
    public byte[] NullPlaceholder => Array.Empty<byte>();

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, byte[][] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is null)
                throw NullAt(i);
            WriteValue(ref writer, values[i]);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, byte[] value)
    {
        if (value is null)
            throw NullAt(rowIndex: -1);

        // Pre-fix oversized input was silently truncated to the column's
        // declared length, leaving the caller unaware that data was lost.
        // Reject loudly — the fix is to declare a wider FixedString column
        // or to truncate explicitly at the call site.
        if (value.Length > _length)
        {
            throw new ArgumentException(
                $"FixedString({_length}) cannot accept a {value.Length}-byte value; truncate at the " +
                $"call site or declare a wider column.",
                nameof(value));
        }

        // Write the value bytes, then zero-pad to the declared length.
        for (int i = 0; i < value.Length; i++)
        {
            writer.WriteByte(value[i]);
        }
        for (int i = value.Length; i < _length; i++)
        {
            writer.WriteByte(0);
        }
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            var value = values[i];
            if (value is null)
                throw NullAt(i);
            if (value is byte[] bytes)
            {
                WriteValue(ref writer, bytes);
            }
            else if (value is string str)
            {
                WriteValue(ref writer, Encoding.UTF8.GetBytes(str));
            }
            else
            {
                throw new InvalidOperationException(
                    $"FixedStringColumnWriter received unsupported value type {value.GetType().Name} " +
                    $"at row {i}. Expected byte[] or string.");
            }
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        if (value is null)
            throw NullAt(rowIndex: -1);
        if (value is byte[] bytes)
        {
            WriteValue(ref writer, bytes);
        }
        else if (value is string str)
        {
            WriteValue(ref writer, Encoding.UTF8.GetBytes(str));
        }
        else
        {
            throw new InvalidOperationException(
                $"FixedStringColumnWriter received unsupported value type {value.GetType().Name}. " +
                $"Expected byte[] or string.");
        }
    }

    private InvalidOperationException NullAt(int rowIndex)
    {
        var where = rowIndex >= 0 ? $" at row {rowIndex}" : string.Empty;
        return new InvalidOperationException(
            $"FixedStringColumnWriter({_length}) received null{where}. The FixedString " +
            $"column type is non-nullable; declare the column as Nullable(FixedString({_length})) " +
            $"and wrap this writer with NullableRefColumnWriter, or ensure source values are non-null.");
    }
}
