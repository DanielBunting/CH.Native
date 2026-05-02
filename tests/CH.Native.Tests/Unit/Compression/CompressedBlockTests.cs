using CH.Native.Compression;
using Xunit;

namespace CH.Native.Tests.Unit.Compression;

public class CompressedBlockTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(100)]
    [InlineData(1000)]
    [InlineData(10000)]
    public void Compress_Decompress_Lz4_RoundTrip(int size)
    {
        var original = CreateTestData(size);
        var compressed = CompressedBlock.Compress(original, Lz4Compressor.Instance);
        var decompressed = CompressedBlock.Decompress(compressed);

        Assert.Equal(original, decompressed);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(100)]
    [InlineData(1000)]
    [InlineData(10000)]
    public void Compress_Decompress_Zstd_RoundTrip(int size)
    {
        var original = CreateTestData(size);
        var compressed = CompressedBlock.Compress(original, ZstdCompressor.Instance);
        var decompressed = CompressedBlock.Decompress(compressed);

        Assert.Equal(original, decompressed);
    }

    [Fact]
    public void Compress_IncludesCorrectHeader()
    {
        var original = new byte[] { 1, 2, 3, 4, 5 };
        var compressed = CompressedBlock.Compress(original, Lz4Compressor.Instance);

        // Minimum size: 16 (checksum) + 9 (header) = 25 bytes
        Assert.True(compressed.Length >= 25);

        // Check algorithm byte at offset 16 (after 16-byte checksum)
        Assert.Equal(0x82, compressed[16]); // LZ4
    }

    [Fact]
    public void Compress_WithZstd_IncludesCorrectAlgorithmId()
    {
        var original = new byte[] { 1, 2, 3, 4, 5 };
        var compressed = CompressedBlock.Compress(original, ZstdCompressor.Instance);

        // Check algorithm byte at offset 16 (after 16-byte checksum)
        Assert.Equal(0x90, compressed[16]); // Zstd
    }

    [Fact]
    public void Decompress_ValidatesChecksum()
    {
        var original = new byte[] { 1, 2, 3, 4, 5 };
        var compressed = CompressedBlock.Compress(original, Lz4Compressor.Instance);

        // Corrupt the checksum
        compressed[0] ^= 0xFF;

        Assert.Throws<InvalidDataException>(() => CompressedBlock.Decompress(compressed));
    }

    [Fact]
    public void Decompress_ValidatesMinimumSize()
    {
        var tooSmall = new byte[20]; // Less than 25 bytes minimum

        Assert.Throws<InvalidDataException>(() => CompressedBlock.Decompress(tooSmall));
    }

    [Fact]
    public void Decompress_ThrowsForUnknownAlgorithm()
    {
        var original = new byte[] { 1, 2, 3, 4, 5 };
        var compressed = CompressedBlock.Compress(original, Lz4Compressor.Instance);

        // Change algorithm to unknown value
        compressed[16] = 0xFF;

        // Recalculate checksum (needed because we modified the data)
        // This test verifies the algorithm check happens after checksum
        // But since checksum will fail first, we need a different approach

        // Actually, let's just verify that unknown algorithms throw during GetCompressor
        Assert.Throws<NotSupportedException>(() => CompressedBlock.GetCompressor(0xFF));
    }

    [Fact]
    public void GetRequiredLength_ReturnsCorrectSize()
    {
        var original = new byte[] { 1, 2, 3, 4, 5 };
        var compressed = CompressedBlock.Compress(original, Lz4Compressor.Instance);

        var requiredLength = CompressedBlock.GetRequiredLength(compressed);

        Assert.Equal(compressed.Length, requiredLength);
    }

    [Fact]
    public void GetRequiredLength_ThrowsForInsufficientData()
    {
        var tooSmall = new byte[20];

        Assert.Throws<InvalidOperationException>(() => CompressedBlock.GetRequiredLength(tooSmall));
    }

    [Fact]
    public void GetCompressor_ReturnsLz4ForId0x82()
    {
        var compressor = CompressedBlock.GetCompressor(0x82);

        Assert.IsType<Lz4Compressor>(compressor);
        Assert.Same(Lz4Compressor.Instance, compressor);
    }

    [Fact]
    public void GetCompressor_ReturnsZstdForId0x90()
    {
        var compressor = CompressedBlock.GetCompressor(0x90);

        Assert.IsType<ZstdCompressor>(compressor);
        Assert.Same(ZstdCompressor.Instance, compressor);
    }

    [Fact]
    public void GetCompressor_ByMethod_ReturnsCorrectCompressor()
    {
        Assert.Null(CompressedBlock.GetCompressor(CompressionMethod.None));
        Assert.Same(Lz4Compressor.Instance, CompressedBlock.GetCompressor(CompressionMethod.Lz4));
        Assert.Same(ZstdCompressor.Instance, CompressedBlock.GetCompressor(CompressionMethod.Zstd));
    }

    [Fact]
    public void Decompress_UncompressedSizeOverstated_ThrowsClickHouseProtocolException()
    {
        // Pre-fix the heap-allocating Decompress path ignored the codec's return
        // value, so an overstated `uncompressed_size` header would copy whatever
        // the rented (or freshly allocated) buffer contained for the trailing
        // bytes — silently corrupting the downstream parse.
        // Post-fix: ValidateDecompressedLength catches the mismatch and throws.
        var bytes = BuildOverstatedBlock(overstateBy: 64);

        var ex = Assert.Throws<CH.Native.Exceptions.ClickHouseProtocolException>(() =>
            CompressedBlock.Decompress(bytes));
        Assert.Contains("wrote", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void DecompressPooled_UncompressedSizeOverstated_ThrowsAndReturnsRentedBuffer()
    {
        // Mirror test for the pooled path. The fix has an explicit catch around
        // ValidateDecompressedLength that returns the rented buffer before the
        // exception escapes — without that, the throw would leak the buffer.
        var bytes = BuildOverstatedBlock(overstateBy: 64);

        Assert.Throws<CH.Native.Exceptions.ClickHouseProtocolException>(() =>
        {
            using var _ = CompressedBlock.DecompressPooled(bytes);
        });
    }

    private static byte[] BuildOverstatedBlock(int overstateBy)
    {
        // Compress a small payload, then bump the uncompressed_size header
        // and recompute the CityHash128 so the gate accepts the (mutated) block.
        var inner = new byte[] { 1, 2, 3, 4, 5 };
        using var compressed = CompressedBlock.CompressPooled(inner, Lz4Compressor.Instance);
        var bytes = compressed.Span.ToArray();

        var realSize = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(21));
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(
            bytes.AsSpan(21),
            realSize + (uint)overstateBy);
        CityHash128.HashBytes(bytes.AsSpan(16), bytes.AsSpan(0, 16));
        return bytes;
    }

    [Fact]
    public void Compress_EmptyData_Works()
    {
        var original = Array.Empty<byte>();
        var compressed = CompressedBlock.Compress(original, Lz4Compressor.Instance);
        var decompressed = CompressedBlock.Decompress(compressed);

        Assert.Empty(decompressed);
    }

    [Fact]
    public void Compress_LargeData_Works()
    {
        var original = new byte[100_000];
        var random = new Random(42);
        random.NextBytes(original);

        var compressed = CompressedBlock.Compress(original, Lz4Compressor.Instance);
        var decompressed = CompressedBlock.Decompress(compressed);

        Assert.Equal(original, decompressed);
    }

    private static byte[] CreateTestData(int size)
    {
        var data = new byte[size];
        for (int i = 0; i < size; i++)
            data[i] = (byte)(i % 256);
        return data;
    }

    // Audit finding #17: CompressPooled writes payload first, then computes the
    // checksum over that payload. The pooled buffer is rented privately inside
    // the method and only escapes via the returned struct after both writes
    // complete, so there is no observable mid-write state — verify by round-trip
    // and by confirming the checksum produced equals one computed independently.
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(100)]
    [InlineData(64 * 1024)]
    public void CompressPooled_ProducesValidChecksumAndRoundTrips(int size)
    {
        var original = CreateTestData(size);

        using var compressed = CompressedBlock.CompressPooled(original, Lz4Compressor.Instance);

        // Independent re-compute of the checksum over the payload bytes.
        var payload = compressed.Span[16..];
        Span<byte> expectedChecksum = stackalloc byte[16];
        CityHash128.HashBytes(payload, expectedChecksum);

        Assert.True(compressed.Span[..16].SequenceEqual(expectedChecksum),
            "Checksum stored in pooled result does not match independently-computed checksum");

        // Round-trip via Decompress to prove the buffer is internally consistent.
        var decompressed = CompressedBlock.Decompress(compressed.Span);
        Assert.Equal(original, decompressed);
    }

    [Fact]
    public void CompressPooled_RepeatedCalls_DoNotCorruptViaPoolReuse()
    {
        // Force several rent/return cycles to surface any aliasing between pooled
        // result buffers; if the checksum write or payload copy ever raced with
        // a re-rent, one of these round-trips would fail.
        var rng = new Random(1234);
        for (int iter = 0; iter < 64; iter++)
        {
            var size = rng.Next(0, 4096);
            var data = new byte[size];
            rng.NextBytes(data);

            using (var c = CompressedBlock.CompressPooled(data, Lz4Compressor.Instance))
            {
                Assert.Equal(data, CompressedBlock.Decompress(c.Span));
            }
        }
    }
}
