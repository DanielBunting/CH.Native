using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace CH.Native.Protocol;

/// <summary>
/// High-performance binary reader for the ClickHouse native protocol.
/// Wraps a <see cref="SequenceReader{T}"/> for efficient parsing of fragmented network buffers.
/// </summary>
public ref struct ProtocolReader
{
    private SequenceReader<byte> _reader;

    /// <summary>
    /// Creates a new ProtocolReader for the specified sequence.
    /// </summary>
    /// <param name="sequence">The byte sequence to read from.</param>
    public ProtocolReader(ReadOnlySequence<byte> sequence)
    {
        _reader = new SequenceReader<byte>(sequence);
    }

    /// <summary>
    /// Gets the number of bytes consumed so far.
    /// </summary>
    public long Consumed => _reader.Consumed;

    /// <summary>
    /// Gets the number of bytes remaining.
    /// </summary>
    public long Remaining => _reader.Remaining;

    /// <summary>
    /// Reads a variable-length encoded unsigned integer.
    /// </summary>
    public ulong ReadVarInt()
    {
        ulong result = 0;
        int shift = 0;

        byte b;
        do
        {
            if (!_reader.TryRead(out b))
                throw new InvalidOperationException("Unexpected end of data while reading VarInt.");

            result |= (ulong)(b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);

        return result;
    }

    /// <summary>
    /// Reads a single byte.
    /// </summary>
    public byte ReadByte()
    {
        if (!_reader.TryRead(out byte value))
            throw new InvalidOperationException("Unexpected end of data while reading byte.");
        return value;
    }

    /// <summary>
    /// Reads a 16-bit signed integer in little-endian format.
    /// </summary>
    public short ReadInt16()
    {
        Span<byte> buffer = stackalloc byte[sizeof(short)];
        if (!_reader.TryCopyTo(buffer))
            throw new InvalidOperationException("Unexpected end of data while reading Int16.");
        _reader.Advance(sizeof(short));
        return BinaryPrimitives.ReadInt16LittleEndian(buffer);
    }

    /// <summary>
    /// Reads a 16-bit unsigned integer in little-endian format.
    /// </summary>
    public ushort ReadUInt16()
    {
        Span<byte> buffer = stackalloc byte[sizeof(ushort)];
        if (!_reader.TryCopyTo(buffer))
            throw new InvalidOperationException("Unexpected end of data while reading UInt16.");
        _reader.Advance(sizeof(ushort));
        return BinaryPrimitives.ReadUInt16LittleEndian(buffer);
    }

    /// <summary>
    /// Reads a 32-bit signed integer in little-endian format.
    /// </summary>
    public int ReadInt32()
    {
        Span<byte> buffer = stackalloc byte[sizeof(int)];
        if (!_reader.TryCopyTo(buffer))
            throw new InvalidOperationException("Unexpected end of data while reading Int32.");
        _reader.Advance(sizeof(int));
        return BinaryPrimitives.ReadInt32LittleEndian(buffer);
    }

    /// <summary>
    /// Reads a 32-bit unsigned integer in little-endian format.
    /// </summary>
    public uint ReadUInt32()
    {
        Span<byte> buffer = stackalloc byte[sizeof(uint)];
        if (!_reader.TryCopyTo(buffer))
            throw new InvalidOperationException("Unexpected end of data while reading UInt32.");
        _reader.Advance(sizeof(uint));
        return BinaryPrimitives.ReadUInt32LittleEndian(buffer);
    }

    /// <summary>
    /// Reads a 64-bit signed integer in little-endian format.
    /// </summary>
    public long ReadInt64()
    {
        Span<byte> buffer = stackalloc byte[sizeof(long)];
        if (!_reader.TryCopyTo(buffer))
            throw new InvalidOperationException("Unexpected end of data while reading Int64.");
        _reader.Advance(sizeof(long));
        return BinaryPrimitives.ReadInt64LittleEndian(buffer);
    }

    /// <summary>
    /// Reads a 64-bit unsigned integer in little-endian format.
    /// </summary>
    public ulong ReadUInt64()
    {
        Span<byte> buffer = stackalloc byte[sizeof(ulong)];
        if (!_reader.TryCopyTo(buffer))
            throw new InvalidOperationException("Unexpected end of data while reading UInt64.");
        _reader.Advance(sizeof(ulong));
        return BinaryPrimitives.ReadUInt64LittleEndian(buffer);
    }

    /// <summary>
    /// Reads a 128-bit signed integer in little-endian format.
    /// </summary>
    public Int128 ReadInt128()
    {
        Span<byte> buffer = stackalloc byte[16];
        if (!_reader.TryCopyTo(buffer))
            throw new InvalidOperationException("Unexpected end of data while reading Int128.");
        _reader.Advance(16);

        // Read as two 64-bit values (little-endian)
        var lower = BinaryPrimitives.ReadUInt64LittleEndian(buffer);
        var upper = BinaryPrimitives.ReadInt64LittleEndian(buffer[8..]);
        return new Int128((ulong)upper, lower);
    }

    /// <summary>
    /// Reads a 128-bit unsigned integer in little-endian format.
    /// </summary>
    public UInt128 ReadUInt128()
    {
        Span<byte> buffer = stackalloc byte[16];
        if (!_reader.TryCopyTo(buffer))
            throw new InvalidOperationException("Unexpected end of data while reading UInt128.");
        _reader.Advance(16);

        // Read as two 64-bit values (little-endian)
        var lower = BinaryPrimitives.ReadUInt64LittleEndian(buffer);
        var upper = BinaryPrimitives.ReadUInt64LittleEndian(buffer[8..]);
        return new UInt128(upper, lower);
    }

    /// <summary>
    /// Reads a 256-bit signed integer in little-endian format.
    /// </summary>
    public System.Numerics.BigInteger ReadInt256()
    {
        var bytes = ReadBytes(32);
        return new System.Numerics.BigInteger(bytes.Span, isUnsigned: false, isBigEndian: false);
    }

    /// <summary>
    /// Reads a 256-bit unsigned integer in little-endian format.
    /// </summary>
    public System.Numerics.BigInteger ReadUInt256()
    {
        var bytes = ReadBytes(32);
        return new System.Numerics.BigInteger(bytes.Span, isUnsigned: true, isBigEndian: false);
    }

    /// <summary>
    /// Reads a string with VarInt length prefix and UTF-8 encoding.
    /// </summary>
    public string ReadString()
    {
        var length = (int)ReadVarInt();
        if (length == 0)
            return string.Empty;

        var bytes = ReadBytes(length);
        return Encoding.UTF8.GetString(bytes.Span);
    }

    /// <summary>
    /// Reads the specified number of bytes.
    /// </summary>
    public ReadOnlyMemory<byte> ReadBytes(int count)
    {
        if (count == 0)
            return ReadOnlyMemory<byte>.Empty;

        if (_reader.Remaining < count)
            throw new InvalidOperationException($"Unexpected end of data while reading {count} bytes, only {_reader.Remaining} available.");

        var slice = _reader.UnreadSequence.Slice(0, count);
        _reader.Advance(count);

        // If the slice is contiguous, return it directly
        if (slice.IsSingleSegment)
            return slice.First;

        // Otherwise, copy to a new array
        var array = new byte[count];
        slice.CopyTo(array);
        return array;
    }

    /// <summary>
    /// Peeks at the byte at the specified offset without advancing the reader.
    /// </summary>
    /// <param name="offset">The zero-based offset from the current position.</param>
    /// <returns>The byte at the specified offset.</returns>
    /// <exception cref="InvalidOperationException">Thrown if not enough data is available.</exception>
    public byte PeekByte(int offset)
    {
        if (_reader.Remaining <= offset)
            throw new InvalidOperationException($"Cannot peek at offset {offset}, only {_reader.Remaining} bytes remaining.");

        var slice = _reader.UnreadSequence.Slice(offset, 1);
        return slice.First.Span[0];
    }
}
