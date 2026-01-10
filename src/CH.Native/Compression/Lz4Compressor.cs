using K4os.Compression.LZ4;

namespace CH.Native.Compression;

/// <summary>
/// LZ4 compressor implementation for ClickHouse native protocol.
/// </summary>
public sealed class Lz4Compressor : ICompressor
{
    /// <summary>
    /// Gets the singleton instance of the LZ4 compressor.
    /// </summary>
    public static Lz4Compressor Instance { get; } = new();

    /// <summary>
    /// Gets the algorithm ID for LZ4 in ClickHouse protocol (0x82).
    /// </summary>
    public byte AlgorithmId => 0x82;

    private Lz4Compressor()
    {
    }

    /// <inheritdoc />
    public int Compress(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        return LZ4Codec.Encode(source, destination);
    }

    /// <inheritdoc />
    public int Decompress(ReadOnlySpan<byte> source, Span<byte> destination, int uncompressedSize)
    {
        return LZ4Codec.Decode(source, destination);
    }

    /// <inheritdoc />
    public int GetMaxCompressedSize(int inputSize)
    {
        return LZ4Codec.MaximumOutputSize(inputSize);
    }
}
