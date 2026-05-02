using System.Buffers.Binary;
using CH.Native.Compression;
using CH.Native.Exceptions;
using Xunit;

namespace CH.Native.Tests.Unit.Compression;

/// <summary>
/// Pre-fix the decoder allocated whatever <c>uncompressedSize</c> the wire
/// declared and cast <c>compressedSize</c> from <c>uint</c> to <c>int</c>
/// without checking. A hostile or corrupted server could request a 4 GiB
/// allocation per block (<c>uncompressedSize = uint.MaxValue</c>) or wrap the
/// compressed-size to a negative slice length. These tests pin: malformed
/// sizes are rejected as <see cref="ClickHouseProtocolException"/> before any
/// allocation.
/// </summary>
public class CompressedBlockSizeBoundsTests
{
    private const int ChecksumSize = 16;
    private const int HeaderSize = 9;

    [Fact]
    public void Decompress_UncompressedSizeAboveCap_Throws()
    {
        var bytes = BuildHeader(
            algorithm: 0x82,
            compressedSize: HeaderSize + 1,
            uncompressedSize: 1u * 1024 * 1024 * 1024 + 1); // 1 GiB + 1 — above default 256 MiB cap

        ProvideValidChecksum(bytes);
        // Append a fake compressed byte so total length matches expectedDataLength.
        var withPayload = AppendByte(bytes, 0x00);
        ProvideValidChecksum(withPayload);

        Assert.Throws<ClickHouseProtocolException>(() => CompressedBlock.Decompress(withPayload));
    }

    [Fact]
    public void DecompressPooled_UncompressedSizeAboveCap_Throws()
    {
        var bytes = BuildHeader(
            algorithm: 0x82,
            compressedSize: HeaderSize + 1,
            uncompressedSize: 512u * 1024 * 1024); // 512 MiB — above 256 MiB cap

        var withPayload = AppendByte(bytes, 0x00);
        ProvideValidChecksum(withPayload);

        Assert.Throws<ClickHouseProtocolException>(() => CompressedBlock.DecompressPooled(withPayload));
    }

    [Fact]
    public void Decompress_CompressedSizeOverflowsInt32_Throws()
    {
        // compressedSize > int.MaxValue would wrap on the (int) cast and produce
        // a negative slice length downstream.
        var bytes = BuildHeader(
            algorithm: 0x82,
            compressedSize: (uint)int.MaxValue + 100u,
            uncompressedSize: 16);

        ProvideValidChecksum(bytes);

        Assert.Throws<ClickHouseProtocolException>(() => CompressedBlock.Decompress(bytes));
    }

    [Fact]
    public void Decompress_KnownGoodRoundtrip_StillWorks()
    {
        // Sanity: the bounds-check fix must not break the happy path. Compress
        // some real data and decompress it back.
        var original = new byte[1024];
        for (int i = 0; i < original.Length; i++) original[i] = (byte)(i & 0xFF);

        using var compressed = CompressedBlock.CompressPooled(original, Lz4Compressor.Instance);
        var decompressed = CompressedBlock.Decompress(compressed.Span);
        Assert.Equal(original, decompressed);
    }

    private static byte[] BuildHeader(byte algorithm, uint compressedSize, uint uncompressedSize)
    {
        var bytes = new byte[ChecksumSize + HeaderSize];
        bytes[ChecksumSize] = algorithm;
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(ChecksumSize + 1), compressedSize);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(ChecksumSize + 5), uncompressedSize);
        return bytes;
    }

    private static byte[] AppendByte(byte[] src, byte b)
    {
        var dst = new byte[src.Length + 1];
        Buffer.BlockCopy(src, 0, dst, 0, src.Length);
        dst[^1] = b;
        return dst;
    }

    private static void ProvideValidChecksum(byte[] bytes)
    {
        // Compute the real checksum of everything past the 16-byte checksum slot
        // so the size-bounds check fires, not the checksum check.
        CityHash128.HashBytes(bytes.AsSpan(ChecksumSize), bytes.AsSpan(0, ChecksumSize));
    }
}
