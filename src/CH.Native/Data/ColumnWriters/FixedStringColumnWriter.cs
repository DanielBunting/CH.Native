using System.Text;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for FixedString(N) values.
/// FixedString stores exactly N bytes per value, padded with null bytes if needed.
/// </summary>
public sealed class FixedStringColumnWriter : IColumnWriter<byte[]>
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
    public void WriteColumn(ref ProtocolWriter writer, byte[][] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            WriteValue(ref writer, values[i]);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, byte[] value)
    {
        // Write the value bytes, truncating or padding as needed
        var bytesToWrite = Math.Min(value.Length, _length);
        for (int i = 0; i < bytesToWrite; i++)
        {
            writer.WriteByte(value[i]);
        }

        // Pad with null bytes if needed
        for (int i = bytesToWrite; i < _length; i++)
        {
            writer.WriteByte(0);
        }
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            var value = values[i];
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
                WriteValue(ref writer, Array.Empty<byte>());
            }
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
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
            WriteValue(ref writer, Array.Empty<byte>());
        }
    }
}
