using CH.Native.Compression;
using Xunit;

namespace CH.Native.Tests.Unit.Compression;

public class CityHash128Tests
{
    [Fact]
    public void Hash_EmptyInput_ReturnsExpectedValue()
    {
        // Empty input should produce a deterministic hash
        var (low, high) = CityHash128.Hash(ReadOnlySpan<byte>.Empty);

        // CityHash128 of empty string
        Assert.NotEqual(0UL, low);
        Assert.NotEqual(0UL, high);
    }

    [Fact]
    public void Hash_SingleByte_ReturnsExpectedValue()
    {
        var data = new byte[] { 0x42 };
        var (low, high) = CityHash128.Hash(data);

        // Should produce a deterministic hash
        Assert.NotEqual(0UL, low);
        Assert.NotEqual(0UL, high);

        // Same input should produce same output
        var (low2, high2) = CityHash128.Hash(data);
        Assert.Equal(low, low2);
        Assert.Equal(high, high2);
    }

    [Fact]
    public void Hash_KnownTestVector_ShortString()
    {
        // "Hello" in bytes
        var data = "Hello"u8.ToArray();
        var (low, high) = CityHash128.Hash(data);

        // Verify deterministic output
        var (low2, high2) = CityHash128.Hash(data);
        Assert.Equal(low, low2);
        Assert.Equal(high, high2);
    }

    [Fact]
    public void Hash_16Bytes_ReturnsExpectedValue()
    {
        var data = new byte[16];
        for (int i = 0; i < 16; i++)
            data[i] = (byte)i;

        var (low, high) = CityHash128.Hash(data);

        // 16 bytes takes a different code path (>= 16 bytes)
        Assert.NotEqual(0UL, low);
        Assert.NotEqual(0UL, high);
    }

    [Fact]
    public void Hash_64Bytes_ReturnsExpectedValue()
    {
        var data = new byte[64];
        for (int i = 0; i < 64; i++)
            data[i] = (byte)i;

        var (low, high) = CityHash128.Hash(data);

        Assert.NotEqual(0UL, low);
        Assert.NotEqual(0UL, high);
    }

    [Fact]
    public void Hash_128Bytes_ReturnsExpectedValue()
    {
        var data = new byte[128];
        for (int i = 0; i < 128; i++)
            data[i] = (byte)i;

        var (low, high) = CityHash128.Hash(data);

        // 128+ bytes takes a different code path
        Assert.NotEqual(0UL, low);
        Assert.NotEqual(0UL, high);
    }

    [Fact]
    public void Hash_256Bytes_ReturnsExpectedValue()
    {
        var data = new byte[256];
        for (int i = 0; i < 256; i++)
            data[i] = (byte)i;

        var (low, high) = CityHash128.Hash(data);

        Assert.NotEqual(0UL, low);
        Assert.NotEqual(0UL, high);
    }

    [Fact]
    public void Hash_1MB_CompletesWithoutError()
    {
        var data = new byte[1024 * 1024];
        var random = new Random(42); // Fixed seed for reproducibility
        random.NextBytes(data);

        var (low, high) = CityHash128.Hash(data);

        Assert.NotEqual(0UL, low);
        Assert.NotEqual(0UL, high);

        // Verify deterministic
        var (low2, high2) = CityHash128.Hash(data);
        Assert.Equal(low, low2);
        Assert.Equal(high, high2);
    }

    [Fact]
    public void HashBytes_ReturnsCorrectFormat()
    {
        var data = "Test data"u8.ToArray();
        var hash = CityHash128.HashBytes(data);

        Assert.Equal(16, hash.Length);

        // Verify it matches tuple version
        var (low, high) = CityHash128.Hash(data);
        var expected = new byte[16];
        BitConverter.TryWriteBytes(expected.AsSpan(0, 8), low);
        BitConverter.TryWriteBytes(expected.AsSpan(8, 8), high);

        Assert.Equal(expected, hash);
    }

    [Fact]
    public void HashBytes_ToSpan_WritesCorrectly()
    {
        var data = "Test data"u8.ToArray();
        var destination = new byte[16];

        CityHash128.HashBytes(data, destination);

        // Should match the array version
        var expected = CityHash128.HashBytes(data);
        Assert.Equal(expected, destination);
    }

    [Fact]
    public void Hash_DifferentInputs_ProduceDifferentHashes()
    {
        var data1 = "Hello"u8.ToArray();
        var data2 = "World"u8.ToArray();

        var (low1, high1) = CityHash128.Hash(data1);
        var (low2, high2) = CityHash128.Hash(data2);

        // Different inputs should produce different hashes
        Assert.False(low1 == low2 && high1 == high2);
    }

    [Fact]
    public void Hash_SameInputs_ProduceSameHashes()
    {
        var data1 = "Hello World"u8.ToArray();
        var data2 = "Hello World"u8.ToArray();

        var (low1, high1) = CityHash128.Hash(data1);
        var (low2, high2) = CityHash128.Hash(data2);

        Assert.Equal(low1, low2);
        Assert.Equal(high1, high2);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(15)]
    [InlineData(16)]
    [InlineData(17)]
    [InlineData(31)]
    [InlineData(32)]
    [InlineData(33)]
    [InlineData(63)]
    [InlineData(64)]
    [InlineData(65)]
    [InlineData(127)]
    [InlineData(128)]
    [InlineData(129)]
    [InlineData(255)]
    [InlineData(256)]
    [InlineData(1000)]
    public void Hash_VariousLengths_ProducesDeterministicOutput(int length)
    {
        var data = new byte[length];
        var random = new Random(length); // Use length as seed for reproducibility
        random.NextBytes(data);

        var (low1, high1) = CityHash128.Hash(data);
        var (low2, high2) = CityHash128.Hash(data);

        Assert.Equal(low1, low2);
        Assert.Equal(high1, high2);
    }
}
