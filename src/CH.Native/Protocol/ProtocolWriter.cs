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
    /// Writes a scaled decimal as a 128-bit signed integer in little-endian format.
    /// This avoids BigInteger allocation by directly extracting decimal bits.
    /// </summary>
    /// <param name="scaledDecimal">The decimal value (already scaled to an integer representation).</param>
    public void WriteDecimalAsInt128(decimal scaledDecimal)
    {
        // Get the internal bits of the decimal
        Span<int> bits = stackalloc int[4];
        decimal.GetBits(scaledDecimal, bits);

        // Extract the 96-bit mantissa (3 x 32-bit integers)
        ulong lo64 = (uint)bits[0] | ((ulong)(uint)bits[1] << 32);
        uint hi32 = (uint)bits[2];

        // Extract flags: bits 16-23 = scale, bit 31 = sign
        int flags = bits[3];
        int scale = (flags >> 16) & 0xFF;
        bool negative = (flags & unchecked((int)0x80000000)) != 0;

        // Build the 128-bit unsigned value from the 96-bit mantissa
        UInt128 mantissa = new UInt128(hi32, lo64);

        // If there's remaining scale, we need to truncate (divide by 10^scale)
        // This should rarely happen if the caller properly scaled the value
        if (scale > 0)
        {
            // Use a lookup table for common powers of 10 to avoid allocation
            mantissa = DivideByPowerOf10(mantissa, scale);
        }

        // Convert to signed and apply sign
        Int128 result = (Int128)mantissa;
        if (negative)
            result = -result;

        WriteInt128(result);
    }

    /// <summary>
    /// Writes a scaled decimal as a 256-bit signed integer in little-endian format.
    /// This avoids BigInteger allocation by directly extracting decimal bits.
    /// </summary>
    /// <param name="scaledDecimal">The decimal value (already scaled to an integer representation).</param>
    public void WriteDecimalAsInt256(decimal scaledDecimal)
    {
        var span = _writer.GetSpan(32);

        // Get the internal bits of the decimal
        Span<int> bits = stackalloc int[4];
        decimal.GetBits(scaledDecimal, bits);

        // Extract the 96-bit mantissa
        uint lo = (uint)bits[0];
        uint mid = (uint)bits[1];
        uint hi = (uint)bits[2];

        // Extract flags
        int flags = bits[3];
        int scale = (flags >> 16) & 0xFF;
        bool negative = (flags & unchecked((int)0x80000000)) != 0;

        // Build 128-bit mantissa and handle scale
        UInt128 mantissa = new UInt128(hi, ((ulong)mid << 32) | lo);
        if (scale > 0)
        {
            mantissa = DivideByPowerOf10(mantissa, scale);
        }

        // Write as little-endian 256-bit
        // Lower 128 bits
        BinaryPrimitives.WriteUInt64LittleEndian(span, (ulong)mantissa);
        BinaryPrimitives.WriteUInt64LittleEndian(span[8..], (ulong)(mantissa >> 64));

        // Upper 128 bits: sign extend
        if (negative)
        {
            // For negative, we need two's complement
            // First negate the mantissa
            UInt128 negated = (UInt128)(-(Int128)mantissa);
            BinaryPrimitives.WriteUInt64LittleEndian(span, (ulong)negated);
            BinaryPrimitives.WriteUInt64LittleEndian(span[8..], (ulong)(negated >> 64));
            // Sign extend upper 128 bits with 0xFF
            span[16..32].Fill(0xFF);
        }
        else
        {
            // Zero extend upper 128 bits
            span[16..32].Clear();
        }

        _writer.Advance(32);
    }

    /// <summary>
    /// Divides a UInt128 by a power of 10 without allocation.
    /// </summary>
    private static UInt128 DivideByPowerOf10(UInt128 value, int power)
    {
        // Powers of 10 up to 10^28 (max decimal scale)
        ReadOnlySpan<ulong> powersOf10 = [
            1UL,                     // 10^0
            10UL,                    // 10^1
            100UL,                   // 10^2
            1000UL,                  // 10^3
            10000UL,                 // 10^4
            100000UL,                // 10^5
            1000000UL,               // 10^6
            10000000UL,              // 10^7
            100000000UL,             // 10^8
            1000000000UL,            // 10^9
            10000000000UL,           // 10^10
            100000000000UL,          // 10^11
            1000000000000UL,         // 10^12
            10000000000000UL,        // 10^13
            100000000000000UL,       // 10^14
            1000000000000000UL,      // 10^15
            10000000000000000UL,     // 10^16
            100000000000000000UL,    // 10^17
            1000000000000000000UL,   // 10^18
            10000000000000000000UL,  // 10^19
        ];

        if (power <= 19)
        {
            return value / powersOf10[power];
        }

        // For larger powers, divide iteratively
        UInt128 result = value;
        while (power > 19)
        {
            result /= powersOf10[19];
            power -= 19;
        }
        return result / powersOf10[power];
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
    /// <remarks>
    /// The 'scoped' modifier ensures stackalloc spans can be passed safely to this method.
    /// </remarks>
    public void WriteBytes(scoped ReadOnlySpan<byte> bytes)
    {
        var span = _writer.GetSpan(bytes.Length);
        bytes.CopyTo(span);
        _writer.Advance(bytes.Length);
    }
}
