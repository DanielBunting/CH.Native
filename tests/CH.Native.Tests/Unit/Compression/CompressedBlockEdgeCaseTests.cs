using CH.Native.Compression;
using CH.Native.Exceptions;
using Xunit;

namespace CH.Native.Tests.Unit.Compression;

/// <summary>
/// Edge-case coverage for <see cref="CompressedBlock"/> beyond the existing
/// integrity / size-bounds tests: undersized frames, header-only corruption,
/// algorithm rejection, and a seeded fuzz round-trip across both compressors.
/// </summary>
public class CompressedBlockEdgeCaseTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(15)]   // exactly one byte short of checksum complete
    [InlineData(16)]   // checksum complete, header missing
    [InlineData(24)]   // checksum + 8 of 9 header bytes
    public void Decompress_BelowMinimumSize_ThrowsInvalidData(int size)
    {
        // The min block size is 25 bytes (16 checksum + 9 header). Anything
        // smaller must be rejected with a typed exception, not an
        // IndexOutOfRangeException.
        InvalidDataException? caught = null;
        try { CompressedBlock.Decompress(new byte[size]); }
        catch (InvalidDataException ex) { caught = ex; }
        Assert.NotNull(caught);
        Assert.Contains("too small", caught!.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Decompress_TamperedAlgorithmByte_RejectedBeforeDecompression()
    {
        // Algorithm byte sits inside the checksum-protected payload, so a
        // tamper must surface as a checksum mismatch (not an unknown-algo
        // error from a downstream lookup).
        var original = new byte[] { 1, 2, 3, 4, 5 };
#pragma warning disable CS0618
        var compressed = CompressedBlock.Compress(original, Lz4Compressor.Instance);
#pragma warning restore CS0618

        compressed[16] = 0xFF;  // algorithm byte
        InvalidDataException? caught = null;
        try { _ = CompressedBlock.Decompress(compressed); }
        catch (InvalidDataException ex) { caught = ex; }
        Assert.NotNull(caught);
        Assert.Contains("checksum", caught!.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Decompress_UnknownAlgorithm_WithValidChecksum_Throws()
    {
        // Construct a frame whose checksum matches a payload that declares an
        // unknown algorithm. GetCompressor should reject the algorithm byte
        // with a typed exception.
        var algorithm = (byte)0x99;  // not LZ4 (0x82) and not Zstd (0x90)
        var payload = new byte[9];
        payload[0] = algorithm;
        // compressedSize = 9 (header only, no data); uncompressedSize = 0
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(1), 9u);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(5), 0u);

        var frame = new byte[16 + 9];
        payload.CopyTo(frame.AsSpan(16));
        CityHash128.HashBytes(payload, frame.AsSpan(0, 16));

        Exception? caught = null;
        try { _ = CompressedBlock.Decompress(frame); }
        catch (Exception ex) { caught = ex; }
        Assert.NotNull(caught);
        // GetCompressor throws for unknown algorithms — pin that path.
        Assert.True(caught is InvalidDataException or NotSupportedException,
            $"Expected InvalidDataException or NotSupportedException, got {caught!.GetType().Name}");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(31)]
    [InlineData(64)]
    [InlineData(1024)]
    [InlineData(64 * 1024)]
    public void Lz4_RoundTrips_AcrossSizes(int size)
    {
        var data = MakeRandomBuffer(seed: 42 + size, size);
#pragma warning disable CS0618
        var compressed = CompressedBlock.Compress(data, Lz4Compressor.Instance);
#pragma warning restore CS0618
        var decompressed = CompressedBlock.Decompress(compressed);

        Assert.Equal(data, decompressed);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(31)]
    [InlineData(64)]
    [InlineData(1024)]
    [InlineData(64 * 1024)]
    public void Zstd_RoundTrips_AcrossSizes(int size)
    {
        var data = MakeRandomBuffer(seed: 1337 + size, size);
#pragma warning disable CS0618
        var compressed = CompressedBlock.Compress(data, ZstdCompressor.Instance);
#pragma warning restore CS0618
        var decompressed = CompressedBlock.Decompress(compressed);

        Assert.Equal(data, decompressed);
    }

    [Fact]
    public void Lz4_FuzzedInputs_AlwaysRoundTrip()
    {
        // Seeded fuzz: 50 random buffers of varying lengths. Catches
        // boundary regressions that single-size tests miss (off-by-ones at
        // chunk boundaries, padding-byte issues, etc.).
        var rng = new Random(0xCAFE);
        for (int trial = 0; trial < 50; trial++)
        {
            var size = rng.Next(0, 8192);
            var data = new byte[size];
            rng.NextBytes(data);

#pragma warning disable CS0618
            var compressed = CompressedBlock.Compress(data, Lz4Compressor.Instance);
#pragma warning restore CS0618
            var decompressed = CompressedBlock.Decompress(compressed);
            Assert.Equal(data, decompressed);
        }
    }

    [Fact]
    public void DecompressPooled_TamperedPayload_DoesNotLeakBuffer()
    {
        // The pooled path rents a buffer before validating the decompressed
        // length. A throw in ValidateDecompressedLength must return the
        // buffer to the pool. Smoke this by tampering with the compressed
        // payload (after the header) so decompression yields the wrong byte
        // count, exercising the catch-and-return branch.
        var original = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
#pragma warning disable CS0618
        var compressed = CompressedBlock.Compress(original, Lz4Compressor.Instance);
#pragma warning restore CS0618

        // Tamper with the compressed payload bytes (after checksum + header).
        compressed[25] ^= 0xFF;
        // Recompute checksum so the tamper survives the integrity gate and
        // reaches the decompress-length validator.
        CityHash128.HashBytes(compressed.AsSpan(16), compressed.AsSpan(0, 16));

        Exception? caught = null;
        try { using var _ = CompressedBlock.DecompressPooled(compressed); }
        catch (Exception ex) { caught = ex; }
        Assert.NotNull(caught);
        // Either ClickHouseProtocolException (length mismatch) or codec error.
        Assert.True(caught is ClickHouseProtocolException or InvalidDataException,
            $"Got {caught!.GetType().Name}: {caught.Message}");
    }

    [Fact]
    public void GetRequiredLength_ReturnsChecksumPlusCompressedSize()
    {
        var original = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
#pragma warning disable CS0618
        var compressed = CompressedBlock.Compress(original, Lz4Compressor.Instance);
#pragma warning restore CS0618

        var required = CompressedBlock.GetRequiredLength(compressed);
        Assert.Equal(compressed.Length, required);
    }

    [Fact]
    public void GetRequiredLength_BelowMinimum_Throws()
    {
        Assert.Throws<InvalidOperationException>(() =>
            CompressedBlock.GetRequiredLength(new byte[10]));
    }

    [Theory]
    [InlineData(CompressionMethod.Lz4, 0x82)]
    [InlineData(CompressionMethod.Zstd, 0x90)]
    public void GetCompressor_ByMethod_ResolvesAlgorithmId(CompressionMethod method, byte expectedAlgorithmId)
    {
        var compressor = CompressedBlock.GetCompressor(method);
        Assert.NotNull(compressor);
        Assert.Equal(expectedAlgorithmId, compressor!.AlgorithmId);
    }

    [Fact]
    public void GetCompressor_NoneMethod_ReturnsNull()
    {
        // CompressionMethod.None = "no compression" — the lookup returns null,
        // not throws. Pin that contract so the connection layer can branch on it.
        Assert.Null(CompressedBlock.GetCompressor(CompressionMethod.None));
    }

    private static byte[] MakeRandomBuffer(int seed, int size)
    {
        var data = new byte[size];
        if (size == 0) return data;
        new Random(seed).NextBytes(data);
        return data;
    }
}
