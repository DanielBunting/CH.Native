using ZstdSharp;

namespace CH.Native.Compression;

/// <summary>
/// Zstandard compressor implementation for ClickHouse native protocol.
/// </summary>
public sealed class ZstdCompressor : ICompressor
{
    /// <summary>
    /// Gets the singleton instance of the Zstd compressor.
    /// </summary>
    public static ZstdCompressor Instance { get; } = new();

    /// <summary>
    /// Gets the algorithm ID for Zstd in ClickHouse protocol (0x90).
    /// </summary>
    public byte AlgorithmId => 0x90;

    private ZstdCompressor()
    {
    }

    /// <inheritdoc />
    public int Compress(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        using var compressor = new Compressor();
        return compressor.Wrap(source, destination);
    }

    /// <inheritdoc />
    public int Decompress(ReadOnlySpan<byte> source, Span<byte> destination, int uncompressedSize)
    {
        using var decompressor = new Decompressor();
        return decompressor.Unwrap(source, destination);
    }

    /// <inheritdoc />
    public int GetMaxCompressedSize(int inputSize)
    {
        return Compressor.GetCompressBound(inputSize);
    }
}
