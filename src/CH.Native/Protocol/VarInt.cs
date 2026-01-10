namespace CH.Native.Protocol;

/// <summary>
/// LEB128 (Little Endian Base 128) variable-length integer encoding.
/// Each byte uses 7 bits for data, with the high bit indicating continuation.
/// </summary>
public static class VarInt
{
    /// <summary>
    /// Maximum number of bytes needed to encode a 64-bit integer.
    /// </summary>
    public const int MaxLength = 10;

    /// <summary>
    /// Writes a variable-length encoded unsigned integer to the buffer.
    /// </summary>
    /// <param name="buffer">The buffer to write to.</param>
    /// <param name="value">The value to encode.</param>
    /// <returns>The number of bytes written.</returns>
    public static int Write(Span<byte> buffer, ulong value)
    {
        int i = 0;
        while (value >= 0x80)
        {
            buffer[i++] = (byte)(value | 0x80);
            value >>= 7;
        }
        buffer[i++] = (byte)value;
        return i;
    }

    /// <summary>
    /// Reads a variable-length encoded unsigned integer from the buffer.
    /// </summary>
    /// <param name="buffer">The buffer to read from.</param>
    /// <param name="bytesRead">The number of bytes consumed.</param>
    /// <returns>The decoded value.</returns>
    public static ulong Read(ReadOnlySpan<byte> buffer, out int bytesRead)
    {
        ulong result = 0;
        int shift = 0;
        bytesRead = 0;

        byte b;
        do
        {
            b = buffer[bytesRead++];
            result |= (ulong)(b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);

        return result;
    }
}
