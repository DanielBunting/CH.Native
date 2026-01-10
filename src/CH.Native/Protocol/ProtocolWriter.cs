using System.Buffers;
using System.Buffers.Binary;
using System.Numerics;
using System.Text;

namespace CH.Native.Protocol;

/// <summary>
/// High-performance binary writer for the ClickHouse native protocol.
/// Wraps an <see cref="IBufferWriter{T}"/> for zero-allocation writes.
/// </summary>
public ref struct ProtocolWriter
{
    private readonly IBufferWriter<byte> _writer;

    /// <summary>
    /// Creates a new ProtocolWriter wrapping the specified buffer writer.
    /// </summary>
    /// <param name="writer">The buffer writer to write to.</param>
    public ProtocolWriter(IBufferWriter<byte> writer)
    {
        _writer = writer;
    }

    /// <summary>
    /// Writes a variable-length encoded unsigned integer.
    /// </summary>
    public void WriteVarInt(ulong value)
    {
        var span = _writer.GetSpan(VarInt.MaxLength);
        int written = VarInt.Write(span, value);
        _writer.Advance(written);
    }

    /// <summary>
    /// Writes a variable-length encoded signed integer.
    /// </summary>
    public void WriteVarInt(int value) => WriteVarInt((ulong)value);

    /// <summary>
    /// Writes a single byte.
    /// </summary>
    public void WriteByte(byte value)
    {
        var span = _writer.GetSpan(1);
        span[0] = value;
        _writer.Advance(1);
    }

    /// <summary>
    /// Writes a 16-bit signed integer in little-endian format.
    /// </summary>
    public void WriteInt16(short value)
    {
        var span = _writer.GetSpan(sizeof(short));
        BinaryPrimitives.WriteInt16LittleEndian(span, value);
        _writer.Advance(sizeof(short));
    }

    /// <summary>
    /// Writes a 16-bit unsigned integer in little-endian format.
    /// </summary>
    public void WriteUInt16(ushort value)
    {
        var span = _writer.GetSpan(sizeof(ushort));
        BinaryPrimitives.WriteUInt16LittleEndian(span, value);
        _writer.Advance(sizeof(ushort));
    }

    /// <summary>
    /// Writes a 32-bit signed integer in little-endian format.
    /// </summary>
    public void WriteInt32(int value)
    {
        var span = _writer.GetSpan(sizeof(int));
        BinaryPrimitives.WriteInt32LittleEndian(span, value);
        _writer.Advance(sizeof(int));
    }

    /// <summary>
    /// Writes a 32-bit unsigned integer in little-endian format.
    /// </summary>
    public void WriteUInt32(uint value)
    {
        var span = _writer.GetSpan(sizeof(uint));
        BinaryPrimitives.WriteUInt32LittleEndian(span, value);
        _writer.Advance(sizeof(uint));
    }

    /// <summary>
    /// Writes a 64-bit signed integer in little-endian format.
    /// </summary>
    public void WriteInt64(long value)
    {
        var span = _writer.GetSpan(sizeof(long));
        BinaryPrimitives.WriteInt64LittleEndian(span, value);
        _writer.Advance(sizeof(long));
    }

    /// <summary>
    /// Writes a 64-bit unsigned integer in little-endian format.
    /// </summary>
    public void WriteUInt64(ulong value)
    {
        var span = _writer.GetSpan(sizeof(ulong));
        BinaryPrimitives.WriteUInt64LittleEndian(span, value);
        _writer.Advance(sizeof(ulong));
    }

    /// <summary>
    /// Writes a 128-bit signed integer in little-endian format.
    /// </summary>
    public void WriteInt128(Int128 value)
    {
        var span = _writer.GetSpan(16);
        // Lower 64 bits first (little-endian)
        BinaryPrimitives.WriteUInt64LittleEndian(span, (ulong)value);
        // Upper 64 bits (signed)
        BinaryPrimitives.WriteInt64LittleEndian(span[8..], (long)(value >> 64));
        _writer.Advance(16);
    }

    /// <summary>
    /// Writes a 128-bit unsigned integer in little-endian format.
    /// </summary>
    public void WriteUInt128(UInt128 value)
    {
        var span = _writer.GetSpan(16);
        // Lower 64 bits first (little-endian)
        BinaryPrimitives.WriteUInt64LittleEndian(span, (ulong)value);
        // Upper 64 bits
        BinaryPrimitives.WriteUInt64LittleEndian(span[8..], (ulong)(value >> 64));
        _writer.Advance(16);
    }

    /// <summary>
    /// Writes a 256-bit signed integer in little-endian format.
    /// </summary>
    public void WriteInt256(BigInteger value)
    {
        var span = _writer.GetSpan(32);
        span.Clear(); // Initialize to zeros
        var bytes = value.ToByteArray(isUnsigned: false, isBigEndian: false);
        var copyLength = Math.Min(bytes.Length, 32);
        bytes.AsSpan(0, copyLength).CopyTo(span);
        // Sign extend if negative and shorter than 32 bytes
        if (value.Sign < 0 && bytes.Length < 32)
        {
            span[bytes.Length..32].Fill(0xFF);
        }
        _writer.Advance(32);
    }

    /// <summary>
    /// Writes a 256-bit unsigned integer in little-endian format.
    /// </summary>
    public void WriteUInt256(BigInteger value)
    {
        var span = _writer.GetSpan(32);
        span.Clear(); // Initialize to zeros
        var bytes = value.ToByteArray(isUnsigned: true, isBigEndian: false);
        var copyLength = Math.Min(bytes.Length, 32);
        bytes.AsSpan(0, copyLength).CopyTo(span);
        _writer.Advance(32);
    }

    /// <summary>
    /// Writes a single-precision floating point number in little-endian format.
    /// </summary>
    public void WriteFloat32(float value)
    {
        var span = _writer.GetSpan(sizeof(float));
        BinaryPrimitives.WriteSingleLittleEndian(span, value);
        _writer.Advance(sizeof(float));
    }

    /// <summary>
    /// Writes a double-precision floating point number in little-endian format.
    /// </summary>
    public void WriteFloat64(double value)
    {
        var span = _writer.GetSpan(sizeof(double));
        BinaryPrimitives.WriteDoubleLittleEndian(span, value);
        _writer.Advance(sizeof(double));
    }

    /// <summary>
    /// Writes a string with VarInt length prefix and UTF-8 encoding.
    /// </summary>
    public void WriteString(string value)
    {
        var byteCount = Encoding.UTF8.GetByteCount(value);
        WriteVarInt((ulong)byteCount);

        var span = _writer.GetSpan(byteCount);
        Encoding.UTF8.GetBytes(value, span);
        _writer.Advance(byteCount);
    }

    /// <summary>
    /// Writes raw bytes to the buffer.
    /// </summary>
    public void WriteBytes(ReadOnlySpan<byte> bytes)
    {
        var span = _writer.GetSpan(bytes.Length);
        bytes.CopyTo(span);
        _writer.Advance(bytes.Length);
    }
}
