namespace CH.Native.Compression;

/// <summary>
/// Compression methods supported by the ClickHouse native protocol.
/// </summary>
public enum CompressionMethod
{
    /// <summary>
    /// No compression.
    /// </summary>
    None = 0,

    /// <summary>
    /// LZ4 compression (protocol ID 0x82).
    /// Fast compression with good balance of speed and ratio.
    /// </summary>
    Lz4 = 1,

    /// <summary>
    /// Zstandard compression (protocol ID 0x90).
    /// Higher compression ratio than LZ4, slightly slower.
    /// </summary>
    Zstd = 2
}
