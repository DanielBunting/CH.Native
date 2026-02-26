using CH.Native.Compression;
using Xunit;

namespace CH.Native.Tests.Unit.Compression;

public class CompressedBlockIntegrityTests
{
    [Fact]
    public void Compression_Checksum_TamperedBlock_DetectsCorruption()
    {
        // Create test data and compress it with LZ4
        var original = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
#pragma warning disable CS0618 // Suppress obsolete warning for Compress (test helper)
        var compressed = CompressedBlock.Compress(original, Lz4Compressor.Instance);
#pragma warning restore CS0618

        // Verify the compressed block is valid before tampering
        var decompressed = CompressedBlock.Decompress(compressed);
        Assert.Equal(original, decompressed);

        // Tamper with data bytes after the checksum (16 bytes) and header (9 bytes)
        // The header starts at offset 16, so actual compressed data starts at offset 25
        // Flip bits in the compressed data area to corrupt it
        compressed[25] ^= 0xFF;

        // Attempting to decompress a tampered block should throw InvalidDataException
        // because the CityHash128 checksum no longer matches the modified payload
        Assert.Throws<InvalidDataException>(() => CompressedBlock.Decompress(compressed));
    }
}
