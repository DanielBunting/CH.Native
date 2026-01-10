using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace CH.Native.Compression;

/// <summary>
/// CityHash128 implementation compatible with ClickHouse's CityHash v1.0.2.
/// ClickHouse uses this for block checksums in the native protocol.
/// </summary>
/// <remarks>
/// Ported from: https://github.com/ClickHouse/ClickHouse/blob/master/contrib/cityhash102/src/city.cc
/// Original copyright: Copyright (c) 2011 Google, Inc. (MIT License)
/// </remarks>
public static class CityHash128
{
    // Constants from CityHash v1.0.2
    private const ulong K0 = 0xc3a5c85c97cb3127UL;
    private const ulong K1 = 0xb492b66fbe98f273UL;
    private const ulong K2 = 0x9ae16a3b2f90404fUL;
    private const ulong K3 = 0xc949d7c7509e6557UL;

    /// <summary>
    /// Computes the CityHash128 of the given data.
    /// </summary>
    /// <param name="data">The data to hash.</param>
    /// <returns>A tuple containing the low and high 64-bit parts of the 128-bit hash.</returns>
    public static (ulong Low, ulong High) Hash(ReadOnlySpan<byte> data)
    {
        var len = data.Length;

        if (len >= 16)
        {
            return CityHash128WithSeed(
                data, 16,
                (Fetch64(data, 0) ^ K3, Fetch64(data, 8)));
        }
        else if (len >= 8)
        {
            return CityHash128WithSeed(
                data, len,
                (Fetch64(data, len - 8) ^ K1, Fetch64(data, 0) ^ K0));
        }
        else
        {
            return CityHash128WithSeedShort(data, (K0, K1));
        }
    }

    /// <summary>
    /// Computes the CityHash128 and returns it as a 16-byte array (little-endian).
    /// </summary>
    /// <param name="data">The data to hash.</param>
    /// <returns>A 16-byte array containing the hash (low bytes first, then high bytes).</returns>
    public static byte[] HashBytes(ReadOnlySpan<byte> data)
    {
        var (low, high) = Hash(data);
        var result = new byte[16];
        BinaryPrimitives.WriteUInt64LittleEndian(result.AsSpan(0, 8), low);
        BinaryPrimitives.WriteUInt64LittleEndian(result.AsSpan(8, 8), high);
        return result;
    }

    /// <summary>
    /// Computes the CityHash128 and writes it to the destination span.
    /// </summary>
    /// <param name="data">The data to hash.</param>
    /// <param name="destination">The destination span (must be at least 16 bytes).</param>
    public static void HashBytes(ReadOnlySpan<byte> data, Span<byte> destination)
    {
        var (low, high) = Hash(data);
        BinaryPrimitives.WriteUInt64LittleEndian(destination[..8], low);
        BinaryPrimitives.WriteUInt64LittleEndian(destination[8..], high);
    }

    private static (ulong Low, ulong High) CityHash128WithSeed(
        ReadOnlySpan<byte> data,
        int startOffset,
        (ulong Low, ulong High) seed)
    {
        var len = data.Length - startOffset;

        if (len < 128)
        {
            return CityMurmur(data, startOffset, seed);
        }

        // Keep original data pointer and track position
        var originalLen = len;
        var pos = startOffset;

        var v = (Low: 0UL, High: 0UL);
        var w = (Low: 0UL, High: 0UL);

        var x = seed.Low;
        var y = seed.High;
        var z = (ulong)len * K1;

        v.Low = Rotate(y ^ K1, 49) * K1 + Fetch64(data, pos);
        v.High = Rotate(v.Low, 42) * K1 + Fetch64(data, pos + 8);
        w.Low = Rotate(y + z, 35) * K1 + x;
        w.High = Rotate(x + Fetch64(data, pos + 88), 53) * K1;

        // Main loop processing 128 bytes at a time
        do
        {
            x = Rotate(x + y + v.Low + Fetch64(data, pos + 16), 37) * K1;
            y = Rotate(y + v.High + Fetch64(data, pos + 48), 42) * K1;
            x ^= w.High;
            y ^= v.Low;
            z = Rotate(z ^ w.Low, 33);
            v = WeakHashLen32WithSeeds(data, pos, v.High * K1, x + w.Low);
            w = WeakHashLen32WithSeeds(data, pos + 32, z + w.High, y);
            (z, x) = (x, z);
            pos += 64;

            x = Rotate(x + y + v.Low + Fetch64(data, pos + 16), 37) * K1;
            y = Rotate(y + v.High + Fetch64(data, pos + 48), 42) * K1;
            x ^= w.High;
            y ^= v.Low;
            z = Rotate(z ^ w.Low, 33);
            v = WeakHashLen32WithSeeds(data, pos, v.High * K1, x + w.Low);
            w = WeakHashLen32WithSeeds(data, pos + 32, z + w.High, y);
            (z, x) = (x, z);
            pos += 64;

            len -= 128;
        } while (len >= 128);

        y += Rotate(w.Low, 37) * K0 + z;
        x += Rotate(v.Low + z, 49) * K0;

        // Process tail (less than 128 bytes remaining)
        // The tail loop reads backwards from the end of the data
        var endPos = startOffset + originalLen;
        for (var tailDone = 0; tailDone < len; tailDone += 32)
        {
            y = Rotate(y - x, 42) * K0 + v.High;
            w.Low += Fetch64(data, pos + len - tailDone - 32 + 16);
            x = Rotate(x, 49) * K0 + w.Low;
            w.Low += v.Low;
            v = WeakHashLen32WithSeeds(data, pos + len - tailDone - 32, v.Low, v.High);
        }

        x = HashLen16(x, v.Low);
        y = HashLen16(y, w.Low);

        return (HashLen16(x + v.High, w.High) + y, HashLen16(x + w.High, y + v.High));
    }

    private static (ulong Low, ulong High) CityHash128WithSeedShort(
        ReadOnlySpan<byte> data,
        (ulong Low, ulong High) seed)
    {
        // For inputs < 16 bytes, use CityMurmur
        return CityMurmur(data, 0, seed);
    }

    private static (ulong Low, ulong High) CityMurmur(
        ReadOnlySpan<byte> data,
        int startOffset,
        (ulong Low, ulong High) seed)
    {
        var len = data.Length - startOffset;
        var a = seed.Low;
        var b = seed.High;
        ulong c;
        ulong d;

        if (len <= 16)
        {
            a = ShiftMix(a * K1) * K1;
            c = b * K1 + HashLen0to16(data, startOffset, len);
            d = ShiftMix(a + (len >= 8 ? Fetch64(data, startOffset) : c));
        }
        else
        {
            c = HashLen16(Fetch64(data, startOffset + len - 8) + K1, a);
            d = HashLen16(b + (ulong)len, c + Fetch64(data, startOffset + len - 16));
            a += d;
            var pos = startOffset;
            var remaining = len;
            do
            {
                a ^= ShiftMix(Fetch64(data, pos) * K1) * K1;
                a *= K1;
                b ^= a;
                c ^= ShiftMix(Fetch64(data, pos + 8) * K1) * K1;
                c *= K1;
                d ^= c;
                pos += 16;
                remaining -= 16;
            } while (remaining > 16);
        }

        a = HashLen16(a, c);
        b = HashLen16(d, b);

        return (a ^ b, HashLen16(b, a));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong HashLen0to16(ReadOnlySpan<byte> data, int offset, int len)
    {
        if (len > 8)
        {
            var a = Fetch64(data, offset);
            var b = Fetch64(data, offset + len - 8);
            return HashLen16(a, RotateByAtLeast1(b + (ulong)len, len)) ^ b;
        }

        if (len >= 4)
        {
            var a = Fetch32(data, offset);
            return HashLen16((ulong)len + ((ulong)a << 3), Fetch32(data, offset + len - 4));
        }

        if (len > 0)
        {
            var a = data[offset];
            var b = data[offset + (len >> 1)];
            var c = data[offset + len - 1];
            var y = (uint)a + ((uint)b << 8);
            var z = (uint)len + ((uint)c << 2);
            return ShiftMix(y * K2 ^ z * K3) * K2;
        }

        return K2;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static (ulong Low, ulong High) WeakHashLen32WithSeeds(
        ReadOnlySpan<byte> data,
        int offset,
        ulong a,
        ulong b)
    {
        return WeakHashLen32WithSeeds(
            Fetch64(data, offset),
            Fetch64(data, offset + 8),
            Fetch64(data, offset + 16),
            Fetch64(data, offset + 24),
            a, b);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static (ulong Low, ulong High) WeakHashLen32WithSeeds(
        ulong w, ulong x, ulong y, ulong z, ulong a, ulong b)
    {
        a += w;
        b = Rotate(b + a + z, 21);
        var c = a;
        a += x;
        a += y;
        b += Rotate(a, 44);
        return (a + z, b + c);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong HashLen16(ulong u, ulong v)
    {
        return Hash128to64(u, v);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong Hash128to64(ulong low, ulong high)
    {
        // Murmur-inspired hashing
        const ulong kMul = 0x9ddfea08eb382d69UL;
        var a = (low ^ high) * kMul;
        a ^= a >> 47;
        var b = (high ^ a) * kMul;
        b ^= b >> 47;
        b *= kMul;
        return b;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong Rotate(ulong val, int shift)
    {
        return shift == 0 ? val : (val >> shift) | (val << (64 - shift));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong RotateByAtLeast1(ulong val, int shift)
    {
        return (val >> shift) | (val << (64 - shift));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong ShiftMix(ulong val)
    {
        return val ^ (val >> 47);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong Fetch64(ReadOnlySpan<byte> data, int offset)
    {
        return BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(offset, 8));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint Fetch32(ReadOnlySpan<byte> data, int offset)
    {
        return BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(offset, 4));
    }
}
