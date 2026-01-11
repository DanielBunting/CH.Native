using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace CH.Native.Protocol;

/// <summary>
/// A disposable wrapper for byte data that may be pooled.
/// When the data is contiguous in the source buffer, no allocation occurs.
/// When fragmented, an array is rented from ArrayPool and returned on dispose.
/// </summary>
public readonly struct PooledBytes : IDisposable
{
    /// <summary>
    /// An empty PooledBytes instance.
    /// </summary>
    public static readonly PooledBytes Empty = new(ReadOnlyMemory<byte>.Empty, null, null);

    /// <summary>
    /// The byte data.
    /// </summary>
    public readonly ReadOnlyMemory<byte> Memory;

    private readonly ArrayPool<byte>? _pool;
    private readonly byte[]? _array;

    /// <summary>
    /// Creates a PooledBytes from the specified memory, optionally with pool info for disposal.
    /// </summary>
    public PooledBytes(ReadOnlyMemory<byte> memory, ArrayPool<byte>? pool, byte[]? array)
    {
        Memory = memory;
        _pool = pool;
        _array = array;
    }

    /// <summary>
    /// Gets the byte data as a span.
    /// </summary>
    public ReadOnlySpan<byte> Span => Memory.Span;

    /// <summary>
    /// Gets the length of the data.
    /// </summary>
    public int Length => Memory.Length;

    /// <summary>
    /// Returns the pooled array if one was used.
    /// </summary>
    public void Dispose()
    {
        if (_pool != null && _array != null)
        {
            _pool.Return(_array);
        }
    }
}

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
        using var bytes = ReadPooledBytes(32);
        return new System.Numerics.BigInteger(bytes.Span, isUnsigned: false, isBigEndian: false);
    }

    /// <summary>
    /// Reads a 256-bit unsigned integer in little-endian format.
    /// </summary>
    public System.Numerics.BigInteger ReadUInt256()
    {
        using var bytes = ReadPooledBytes(32);
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

        using var bytes = ReadPooledBytes(length);
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
    /// Reads the specified number of bytes, using ArrayPool for fragmented sequences.
    /// The caller must dispose the returned PooledBytes to return the array to the pool.
    /// </summary>
    /// <param name="count">The number of bytes to read.</param>
    /// <returns>A PooledBytes containing the data. Must be disposed after use.</returns>
    public PooledBytes ReadPooledBytes(int count)
    {
        if (count == 0)
            return PooledBytes.Empty;

        if (_reader.Remaining < count)
            throw new InvalidOperationException($"Unexpected end of data while reading {count} bytes, only {_reader.Remaining} available.");

        var slice = _reader.UnreadSequence.Slice(0, count);
        _reader.Advance(count);

        // If the slice is contiguous, return it directly without allocation
        if (slice.IsSingleSegment)
            return new PooledBytes(slice.First, pool: null, array: null);

        // Otherwise, rent from the pool and copy
        var pool = ArrayPool<byte>.Shared;
        var array = pool.Rent(count);
        slice.CopyTo(array);
        return new PooledBytes(array.AsMemory(0, count), pool, array);
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

    #region TrySkip Methods (Non-allocating validation)

    /// <summary>
    /// Tries to skip a variable-length encoded unsigned integer without allocation.
    /// Used for validating data completeness before parsing.
    /// </summary>
    /// <returns>True if successfully skipped; false if not enough data available.</returns>
    public bool TrySkipVarInt()
    {
        byte b;
        do
        {
            if (!_reader.TryRead(out b))
                return false;
        } while ((b & 0x80) != 0);

        return true;
    }

    /// <summary>
    /// Tries to read a variable-length encoded unsigned integer without throwing.
    /// </summary>
    /// <param name="value">The value read, or 0 if not enough data.</param>
    /// <returns>True if successfully read; false if not enough data available.</returns>
    public bool TryReadVarInt(out ulong value)
    {
        value = 0;
        int shift = 0;

        byte b;
        do
        {
            if (!_reader.TryRead(out b))
                return false;

            value |= (ulong)(b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);

        return true;
    }

    /// <summary>
    /// Tries to skip the specified number of bytes without allocation.
    /// Used for validating data completeness before parsing.
    /// </summary>
    /// <param name="count">The number of bytes to skip.</param>
    /// <returns>True if successfully skipped; false if not enough data available.</returns>
    public bool TrySkipBytes(long count)
    {
        if (count == 0)
            return true;

        if (_reader.Remaining < count)
            return false;

        _reader.Advance(count);
        return true;
    }

    /// <summary>
    /// Tries to skip a string (VarInt length prefix + bytes) without allocation.
    /// Used for validating data completeness before parsing.
    /// </summary>
    /// <returns>True if successfully skipped; false if not enough data available.</returns>
    public bool TrySkipString()
    {
        if (!TryReadVarInt(out var length))
            return false;

        if (length == 0)
            return true;

        return TrySkipBytes((long)length);
    }

    /// <summary>
    /// Tries to read a single byte without throwing.
    /// </summary>
    /// <param name="value">The byte read, or 0 if not enough data.</param>
    /// <returns>True if successfully read; false if not enough data available.</returns>
    public bool TryReadByte(out byte value)
    {
        return _reader.TryRead(out value);
    }

    /// <summary>
    /// Tries to read a 32-bit signed integer without throwing.
    /// </summary>
    /// <param name="value">The value read, or 0 if not enough data.</param>
    /// <returns>True if successfully read; false if not enough data available.</returns>
    public bool TryReadInt32(out int value)
    {
        value = 0;
        Span<byte> buffer = stackalloc byte[sizeof(int)];
        if (!_reader.TryCopyTo(buffer))
            return false;
        _reader.Advance(sizeof(int));
        value = BinaryPrimitives.ReadInt32LittleEndian(buffer);
        return true;
    }

    /// <summary>
    /// Tries to read a 64-bit unsigned integer without throwing.
    /// </summary>
    /// <param name="value">The value read, or 0 if not enough data.</param>
    /// <returns>True if successfully read; false if not enough data available.</returns>
    public bool TryReadUInt64(out ulong value)
    {
        value = 0;
        Span<byte> buffer = stackalloc byte[sizeof(ulong)];
        if (!_reader.TryCopyTo(buffer))
            return false;
        _reader.Advance(sizeof(ulong));
        value = BinaryPrimitives.ReadUInt64LittleEndian(buffer);
        return true;
    }

    #endregion

    #region Bulk Read Methods

    /// <summary>
    /// Tries to get a contiguous span of the next N bytes without copying.
    /// Only succeeds if the data is in a single memory segment.
    /// </summary>
    /// <param name="count">The number of bytes to read.</param>
    /// <param name="span">The contiguous span, if available.</param>
    /// <returns>True if the data is contiguous and span is valid; false otherwise.</returns>
    public bool TryGetContiguousSpan(int count, out ReadOnlySpan<byte> span)
    {
        if (_reader.Remaining < count)
        {
            span = default;
            return false;
        }

        var slice = _reader.UnreadSequence.Slice(0, count);
        if (slice.IsSingleSegment)
        {
            span = slice.FirstSpan;
            return true;
        }

        span = default;
        return false;
    }

    /// <summary>
    /// Advances the reader by the specified number of bytes.
    /// </summary>
    /// <param name="count">The number of bytes to advance.</param>
    public void Advance(int count)
    {
        _reader.Advance(count);
    }

    #endregion
}
