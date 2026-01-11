namespace CH.Native.Compression;

/// <summary>
/// Interface for compression algorithms used in ClickHouse native protocol.
/// </summary>
public interface ICompressor
{
    /// <summary>
    /// Gets the algorithm identifier byte used in the wire format.
    /// </summary>
    /// <remarks>
    /// 0x82 = LZ4, 0x90 = Zstd
    /// </remarks>
    byte AlgorithmId { get; }

    /// <summary>
    /// Compresses the source data into the destination buffer.
    /// </summary>
    /// <param name="source">The data to compress.</param>
    /// <param name="destination">The buffer to write compressed data to.</param>
    /// <returns>The number of bytes written to the destination.</returns>
    int Compress(ReadOnlySpan<byte> source, Span<byte> destination);

    /// <summary>
    /// Decompresses the source data into the destination buffer.
    /// </summary>
    /// <param name="source">The compressed data.</param>
    /// <param name="destination">The buffer to write decompressed data to.</param>
    /// <param name="uncompressedSize">The expected size of the decompressed data.</param>
    /// <returns>The number of bytes written to the destination.</returns>
    int Decompress(ReadOnlySpan<byte> source, Span<byte> destination, int uncompressedSize);

    /// <summary>
    /// Gets the maximum size the compressed data could be for a given input size.
    /// </summary>
    /// <param name="inputSize">The size of the uncompressed data.</param>
    /// <returns>The maximum possible compressed size.</returns>
    int GetMaxCompressedSize(int inputSize);
}
