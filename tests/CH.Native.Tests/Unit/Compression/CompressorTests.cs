using CH.Native.Compression;
using Xunit;

namespace CH.Native.Tests.Unit.Compression;

public class CompressorTests
{
    [Fact]
    public void Lz4Compressor_AlgorithmId_IsCorrect()
    {
        Assert.Equal(0x82, Lz4Compressor.Instance.AlgorithmId);
    }

    [Fact]
    public void ZstdCompressor_AlgorithmId_IsCorrect()
    {
        Assert.Equal(0x90, ZstdCompressor.Instance.AlgorithmId);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(100)]
    [InlineData(1000)]
    [InlineData(10000)]
    public void Lz4Compressor_RoundTrip_ReturnsOriginalData(int size)
    {
        var compressor = Lz4Compressor.Instance;
        var original = CreateTestData(size);

        // Compress
        var maxCompressedSize = compressor.GetMaxCompressedSize(original.Length);
        var compressed = new byte[maxCompressedSize];
        var compressedLength = compressor.Compress(original, compressed);

        // Decompress
        var decompressed = new byte[original.Length];
        compressor.Decompress(compressed.AsSpan(0, compressedLength), decompressed, original.Length);

        Assert.Equal(original, decompressed);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(100)]
    [InlineData(1000)]
    [InlineData(10000)]
    public void ZstdCompressor_RoundTrip_ReturnsOriginalData(int size)
    {
        var compressor = ZstdCompressor.Instance;
        var original = CreateTestData(size);

        // Compress
        var maxCompressedSize = compressor.GetMaxCompressedSize(original.Length);
        var compressed = new byte[maxCompressedSize];
        var compressedLength = compressor.Compress(original, compressed);

        // Decompress
        var decompressed = new byte[original.Length];
        compressor.Decompress(compressed.AsSpan(0, compressedLength), decompressed, original.Length);

        Assert.Equal(original, decompressed);
    }

    [Fact]
    public void Lz4Compressor_CompressesRepetitiveData()
    {
        var compressor = Lz4Compressor.Instance;
        var original = new byte[1000];
        // Fill with repetitive pattern
        for (int i = 0; i < original.Length; i++)
            original[i] = (byte)(i % 4);

        var maxCompressedSize = compressor.GetMaxCompressedSize(original.Length);
        var compressed = new byte[maxCompressedSize];
        var compressedLength = compressor.Compress(original, compressed);

        // Repetitive data should compress well
        Assert.True(compressedLength < original.Length);
    }

    [Fact]
    public void ZstdCompressor_CompressesRepetitiveData()
    {
        var compressor = ZstdCompressor.Instance;
        var original = new byte[1000];
        // Fill with repetitive pattern
        for (int i = 0; i < original.Length; i++)
            original[i] = (byte)(i % 4);

        var maxCompressedSize = compressor.GetMaxCompressedSize(original.Length);
        var compressed = new byte[maxCompressedSize];
        var compressedLength = compressor.Compress(original, compressed);

        // Repetitive data should compress well
        Assert.True(compressedLength < original.Length);
    }

    [Fact]
    public void Lz4Compressor_HandlesIncompressibleData()
    {
        var compressor = Lz4Compressor.Instance;
        var original = CreateRandomData(1000);

        var maxCompressedSize = compressor.GetMaxCompressedSize(original.Length);
        var compressed = new byte[maxCompressedSize];
        var compressedLength = compressor.Compress(original, compressed);

        // Even incompressible data should round-trip correctly
        var decompressed = new byte[original.Length];
        compressor.Decompress(compressed.AsSpan(0, compressedLength), decompressed, original.Length);

        Assert.Equal(original, decompressed);
    }

    [Fact]
    public void ZstdCompressor_HandlesIncompressibleData()
    {
        var compressor = ZstdCompressor.Instance;
        var original = CreateRandomData(1000);

        var maxCompressedSize = compressor.GetMaxCompressedSize(original.Length);
        var compressed = new byte[maxCompressedSize];
        var compressedLength = compressor.Compress(original, compressed);

        // Even incompressible data should round-trip correctly
        var decompressed = new byte[original.Length];
        compressor.Decompress(compressed.AsSpan(0, compressedLength), decompressed, original.Length);

        Assert.Equal(original, decompressed);
    }

    [Fact]
    public void Lz4Compressor_Singleton_ReturnsSameInstance()
    {
        var instance1 = Lz4Compressor.Instance;
        var instance2 = Lz4Compressor.Instance;

        Assert.Same(instance1, instance2);
    }

    [Fact]
    public void ZstdCompressor_Singleton_ReturnsSameInstance()
    {
        var instance1 = ZstdCompressor.Instance;
        var instance2 = ZstdCompressor.Instance;

        Assert.Same(instance1, instance2);
    }

    private static byte[] CreateTestData(int size)
    {
        var data = new byte[size];
        for (int i = 0; i < size; i++)
            data[i] = (byte)(i % 256);
        return data;
    }

    private static byte[] CreateRandomData(int size)
    {
        var random = new Random(42);
        var data = new byte[size];
        random.NextBytes(data);
        return data;
    }
}
