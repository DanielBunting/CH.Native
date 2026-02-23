using System.Buffers;
using System.Buffers.Binary;
using CH.Native.Protocol;

namespace CH.Native.Compression;

/// <summary>
/// Handles reading and writing compressed data blocks in ClickHouse native protocol.
/// </summary>
/// <remarks>
/// Wire format (25+ bytes total):
/// <list type="table">
/// <item><term>Checksum</term><description>[16 bytes] CityHash128 of everything after checksum (low 8 bytes, then high 8 bytes)</description></item>
/// <item><term>Algorithm</term><description>[1 byte] 0x82=LZ4, 0x90=Zstd</description></item>
/// <item><term>CompressedSize</term><description>[4 bytes] Size of header+data (includes 9-byte header)</description></item>
/// <item><term>UncompressedSize</term><description>[4 bytes] Original data size</description></item>
/// <item><term>CompressedData</term><description>[varies] Compressed payload</description></item>
/// </list>
/// </remarks>
public static class CompressedBlock
{
    private const int ChecksumSize = 16;
    private const int HeaderSize = 9; // Algorithm (1) + CompressedSize (4) + UncompressedSize (4)
    private const int MinBlockSize = ChecksumSize + HeaderSize;

    /// <summary>
    /// Compresses data and wraps it in the ClickHouse compressed block format.
    /// Returns a pooled result that must be disposed after use.
    /// </summary>
    /// <param name="data">The uncompressed data.</param>
    /// <param name="compressor">The compressor to use.</param>
    /// <returns>The compressed block including checksum and header. Must be disposed after use.</returns>
    public static CompressedResult CompressPooled(ReadOnlySpan<byte> data, ICompressor compressor)
    {
        // Get maximum compressed size and rent buffer from pool
        var maxCompressedSize = compressor.GetMaxCompressedSize(data.Length);
        var compressedBuffer = ArrayPool<byte>.Shared.Rent(maxCompressedSize);

        try
        {
            // Compress the data
            var compressedLength = compressor.Compress(data, compressedBuffer);

            // Build the result: checksum (16) + header (9) + compressed data
            var totalSize = ChecksumSize + HeaderSize + compressedLength;
            var result = ArrayPool<byte>.Shared.Rent(totalSize);

            // Write header after checksum space (at offset 16)
            // Header format: algorithm (1) + compressed_size (4) + uncompressed_size (4)
            result[ChecksumSize] = compressor.AlgorithmId;
            BinaryPrimitives.WriteUInt32LittleEndian(result.AsSpan(ChecksumSize + 1), (uint)(HeaderSize + compressedLength));
            BinaryPrimitives.WriteUInt32LittleEndian(result.AsSpan(ChecksumSize + 5), (uint)data.Length);

            // Copy compressed data
            compressedBuffer.AsSpan(0, compressedLength).CopyTo(result.AsSpan(ChecksumSize + HeaderSize));

            // Calculate and write checksum (hash of everything after checksum)
            CityHash128.HashBytes(result.AsSpan(ChecksumSize, totalSize - ChecksumSize), result.AsSpan(0, ChecksumSize));

            return new CompressedResult(result, totalSize);
        }
        finally
        {
            // Return the temporary compression buffer immediately
            ArrayPool<byte>.Shared.Return(compressedBuffer);
        }
    }

    /// <summary>
    /// Compresses data and wraps it in the ClickHouse compressed block format.
    /// </summary>
    /// <param name="data">The uncompressed data.</param>
    /// <param name="compressor">The compressor to use.</param>
    /// <returns>The compressed block including checksum and header.</returns>
    [Obsolete("Use CompressPooled for better performance. This method allocates new arrays.")]
    public static byte[] Compress(ReadOnlySpan<byte> data, ICompressor compressor)
    {
        // Get maximum compressed size and allocate buffer
        var maxCompressedSize = compressor.GetMaxCompressedSize(data.Length);
        var compressedBuffer = new byte[maxCompressedSize];

        // Compress the data
        var compressedLength = compressor.Compress(data, compressedBuffer);

        // Build the result: checksum (16) + header (9) + compressed data
        var totalSize = ChecksumSize + HeaderSize + compressedLength;
        var result = new byte[totalSize];

        // Write header after checksum space (at offset 16)
        // Header format: algorithm (1) + compressed_size (4) + uncompressed_size (4)
        result[ChecksumSize] = compressor.AlgorithmId;
        BinaryPrimitives.WriteUInt32LittleEndian(result.AsSpan(ChecksumSize + 1), (uint)(HeaderSize + compressedLength));
        BinaryPrimitives.WriteUInt32LittleEndian(result.AsSpan(ChecksumSize + 5), (uint)data.Length);

        // Copy compressed data
        compressedBuffer.AsSpan(0, compressedLength).CopyTo(result.AsSpan(ChecksumSize + HeaderSize));

        // Calculate and write checksum (hash of everything after checksum)
        CityHash128.HashBytes(result.AsSpan(ChecksumSize), result.AsSpan(0, ChecksumSize));

        return result;
    }

    /// <summary>
    /// Decompresses a ClickHouse compressed block.
    /// </summary>
    /// <param name="data">The compressed block data (including checksum and header).</param>
    /// <returns>The decompressed data.</returns>
    /// <exception cref="InvalidDataException">Thrown if checksum validation fails or algorithm is unknown.</exception>
    public static byte[] Decompress(ReadOnlySpan<byte> data)
    {
        if (data.Length < MinBlockSize)
            throw new InvalidDataException($"Compressed block too small: {data.Length} bytes, minimum {MinBlockSize}");

        // Read and verify checksum
        var expectedChecksum = data[..ChecksumSize];
        var payloadSpan = data[ChecksumSize..];

        Span<byte> actualChecksum = stackalloc byte[16];
        CityHash128.HashBytes(payloadSpan, actualChecksum);

        if (!expectedChecksum.SequenceEqual(actualChecksum))
            throw new InvalidDataException("Compressed block checksum mismatch");

        // Read header (format: algorithm + compressed_size + uncompressed_size)
        var algorithm = data[ChecksumSize];
        var compressedSize = BinaryPrimitives.ReadUInt32LittleEndian(data[(ChecksumSize + 1)..]);
        var uncompressedSize = BinaryPrimitives.ReadUInt32LittleEndian(data[(ChecksumSize + 5)..]);

        // Validate sizes
        if (compressedSize < HeaderSize)
            throw new InvalidDataException($"Invalid compressed size: {compressedSize}");

        var expectedDataLength = ChecksumSize + compressedSize;
        if (data.Length < expectedDataLength)
            throw new InvalidDataException($"Incomplete compressed block: expected {expectedDataLength} bytes, got {data.Length}");

        // Get compressor based on algorithm
        var compressor = GetCompressor(algorithm);

        // Decompress
        var result = new byte[uncompressedSize];
        var compressedData = data[(ChecksumSize + HeaderSize)..];
        var actualCompressedLength = (int)compressedSize - HeaderSize;

        compressor.Decompress(compressedData[..actualCompressedLength], result, (int)uncompressedSize);

        return result;
    }

    /// <summary>
    /// Decompresses a ClickHouse compressed block using pooled buffers.
    /// Returns a pooled result that must be disposed after use.
    /// </summary>
    /// <param name="data">The compressed block data (including checksum and header).</param>
    /// <returns>The decompressed data in a pooled buffer. Must be disposed after use.</returns>
    /// <exception cref="InvalidDataException">Thrown if checksum validation fails or algorithm is unknown.</exception>
    public static DecompressedResult DecompressPooled(ReadOnlySpan<byte> data)
    {
        if (data.Length < MinBlockSize)
            throw new InvalidDataException($"Compressed block too small: {data.Length} bytes, minimum {MinBlockSize}");

        // Read and verify checksum
        var expectedChecksum = data[..ChecksumSize];
        var payloadSpan = data[ChecksumSize..];

        Span<byte> actualChecksum = stackalloc byte[16];
        CityHash128.HashBytes(payloadSpan, actualChecksum);

        if (!expectedChecksum.SequenceEqual(actualChecksum))
            throw new InvalidDataException("Compressed block checksum mismatch");

        // Read header (format: algorithm + compressed_size + uncompressed_size)
        var algorithm = data[ChecksumSize];
        var compressedSize = BinaryPrimitives.ReadUInt32LittleEndian(data[(ChecksumSize + 1)..]);
        var uncompressedSize = BinaryPrimitives.ReadUInt32LittleEndian(data[(ChecksumSize + 5)..]);

        // Validate sizes
        if (compressedSize < HeaderSize)
            throw new InvalidDataException($"Invalid compressed size: {compressedSize}");

        var expectedDataLength = ChecksumSize + compressedSize;
        if (data.Length < expectedDataLength)
            throw new InvalidDataException($"Incomplete compressed block: expected {expectedDataLength} bytes, got {data.Length}");

        // Get compressor based on algorithm
        var compressor = GetCompressor(algorithm);

        // Decompress into pooled buffer
        var pool = ArrayPool<byte>.Shared;
        var result = pool.Rent((int)uncompressedSize);
        var compressedData = data[(ChecksumSize + HeaderSize)..];
        var actualCompressedLength = (int)compressedSize - HeaderSize;

        compressor.Decompress(compressedData[..actualCompressedLength], result, (int)uncompressedSize);

        return new DecompressedResult(result, (int)uncompressedSize);
    }

    /// <summary>
    /// Gets the total length required to read a complete compressed block from the given data.
    /// </summary>
    /// <param name="data">The start of a compressed block (must be at least 25 bytes).</param>
    /// <returns>The total number of bytes needed for the complete block.</returns>
    /// <exception cref="InvalidOperationException">Thrown if there isn't enough data to read the header.</exception>
    public static int GetRequiredLength(ReadOnlySpan<byte> data)
    {
        if (data.Length < MinBlockSize)
            throw new InvalidOperationException("Not enough data to read compressed block header");

        var compressedSize = BinaryPrimitives.ReadUInt32LittleEndian(data[(ChecksumSize + 1)..]);
        return ChecksumSize + (int)compressedSize;
    }

    /// <summary>
    /// Reads a compressed block from a ProtocolReader and returns the raw compressed bytes.
    /// </summary>
    /// <param name="reader">The protocol reader.</param>
    /// <returns>The raw compressed block bytes (checksum + header + data). Must be disposed after use.</returns>
    /// <exception cref="InvalidOperationException">Thrown if not enough data is available.</exception>
    public static PooledCompressedData ReadFromProtocol(ref ProtocolReader reader)
    {
        // Read the header to get the compressed size
        // We need at least 25 bytes: 16 (checksum) + 9 (header minimum)
        if (reader.Remaining < MinBlockSize)
            throw new InvalidOperationException("Not enough data to read compressed block header");

        // Peek at compressed size (at offset 17 from current position: 16 checksum + 1 method byte)
        // We can't easily peek in ProtocolReader, so read the full header worth
        using var headerBytes = reader.ReadPooledBytes(MinBlockSize);
        var compressedSize = BinaryPrimitives.ReadUInt32LittleEndian(headerBytes.Span[(ChecksumSize + 1)..]);

        // Total block size is checksum + compressed size (which includes header)
        var totalBlockSize = ChecksumSize + (int)compressedSize;
        var remainingDataSize = totalBlockSize - MinBlockSize;

        if (remainingDataSize < 0)
            throw new InvalidDataException($"Invalid compressed size: {compressedSize}");

        // Rent from pool and combine header + remaining data
        var pool = ArrayPool<byte>.Shared;
        var result = pool.Rent(totalBlockSize);
        headerBytes.Span.CopyTo(result);

        if (remainingDataSize > 0)
        {
            using var remainingData = reader.ReadPooledBytes(remainingDataSize);
            remainingData.Span.CopyTo(result.AsSpan(MinBlockSize));
        }

        return new PooledCompressedData(result, totalBlockSize);
    }

    /// <summary>
    /// Gets the compressor for the given algorithm ID.
    /// </summary>
    /// <param name="algorithmId">The algorithm ID byte.</param>
    /// <returns>The compressor instance.</returns>
    /// <exception cref="NotSupportedException">Thrown if the algorithm is not supported.</exception>
    public static ICompressor GetCompressor(byte algorithmId)
    {
        return algorithmId switch
        {
            0x82 => Lz4Compressor.Instance,
            0x90 => ZstdCompressor.Instance,
            _ => throw new NotSupportedException($"Unknown compression algorithm: 0x{algorithmId:X2}")
        };
    }

    /// <summary>
    /// Gets the compressor for the given compression method.
    /// </summary>
    /// <param name="method">The compression method.</param>
    /// <returns>The compressor instance, or null for None.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if the method is invalid.</exception>
    public static ICompressor? GetCompressor(CompressionMethod method)
    {
        return method switch
        {
            CompressionMethod.None => null,
            CompressionMethod.Lz4 => Lz4Compressor.Instance,
            CompressionMethod.Zstd => ZstdCompressor.Instance,
            _ => throw new ArgumentOutOfRangeException(nameof(method), method, "Unknown compression method")
        };
    }
}

/// <summary>
/// Represents a compressed block result using a pooled byte array.
/// Must be disposed after use to return the buffer to the pool.
/// </summary>
public readonly struct CompressedResult : IDisposable
{
    private readonly byte[] _buffer;

    /// <summary>
    /// Gets the length of the actual compressed data (may be less than buffer length).
    /// </summary>
    public int Length { get; }

    /// <summary>
    /// Gets the compressed data as a span.
    /// </summary>
    public ReadOnlySpan<byte> Span => _buffer.AsSpan(0, Length);

    /// <summary>
    /// Gets the compressed data as memory.
    /// </summary>
    public ReadOnlyMemory<byte> Memory => _buffer.AsMemory(0, Length);

    internal CompressedResult(byte[] buffer, int length)
    {
        _buffer = buffer;
        Length = length;
    }

    /// <summary>
    /// Returns the buffer to the array pool.
    /// </summary>
    public void Dispose()
    {
        if (_buffer != null)
        {
            ArrayPool<byte>.Shared.Return(_buffer);
        }
    }
}

/// <summary>
/// Represents a decompressed block result using a pooled byte array.
/// Must be disposed after use to return the buffer to the pool.
/// </summary>
public readonly struct DecompressedResult : IDisposable
{
    private readonly byte[] _buffer;

    /// <summary>
    /// Gets the length of the actual decompressed data (may be less than buffer length).
    /// </summary>
    public int Length { get; }

    /// <summary>
    /// Gets the decompressed data as a span.
    /// </summary>
    public ReadOnlySpan<byte> Span => _buffer.AsSpan(0, Length);

    /// <summary>
    /// Gets the decompressed data as memory.
    /// </summary>
    public ReadOnlyMemory<byte> Memory => _buffer.AsMemory(0, Length);

    internal DecompressedResult(byte[] buffer, int length)
    {
        _buffer = buffer;
        Length = length;
    }

    /// <summary>
    /// Returns the buffer to the array pool.
    /// </summary>
    public void Dispose()
    {
        if (_buffer != null)
        {
            ArrayPool<byte>.Shared.Return(_buffer);
        }
    }
}

/// <summary>
/// Represents compressed block data read from a ProtocolReader, using a pooled byte array.
/// Must be disposed after use to return the buffer to the pool.
/// </summary>
public readonly struct PooledCompressedData : IDisposable
{
    private readonly byte[] _buffer;

    /// <summary>
    /// Gets the length of the actual compressed data (may be less than buffer length).
    /// </summary>
    public int Length { get; }

    /// <summary>
    /// Gets the compressed data as a span.
    /// </summary>
    public ReadOnlySpan<byte> Span => _buffer.AsSpan(0, Length);

    internal PooledCompressedData(byte[] buffer, int length)
    {
        _buffer = buffer;
        Length = length;
    }

    /// <summary>
    /// Returns the buffer to the array pool.
    /// </summary>
    public void Dispose()
    {
        if (_buffer != null)
        {
            ArrayPool<byte>.Shared.Return(_buffer);
        }
    }
}
