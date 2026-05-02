using CH.Native.Compression;
using Xunit;

namespace CH.Native.Tests.Unit.Compression;

/// <summary>
/// Symmetric to F1 (decompressor underrun): probes whether the COMPRESS
/// side validates the codec's return value before claiming the buffer's
/// final length. If a codec returns a smaller-than-expected value but the
/// caller writes the un-truncated length to the wire header, the trailing
/// bytes of the rented buffer would be sent as part of the compressed
/// payload.
///
/// <para>
/// <see cref="CompressedBlock.CompressPooled"/> reads the codec's return
/// value into <c>compressedLength</c> and uses it for the buffer slice
/// (line 70: <c>compressedBuffer.AsSpan(0, compressedLength).CopyTo(...)</c>).
/// So the write-side already respects the codec's claim. This test pins
/// that — a regression that ignored the return and used the buffer's
/// rent-size would silently emit junk bytes on the wire.
/// </para>
/// </summary>
public class CompressorWriteReturnValueTests
{
    [Fact]
    public void CompressPooled_RoundTrips_PreservesCodecReturnedLength()
    {
        // Sanity: the bytes between offsets [25, totalSize) match the
        // independent compress operation. If write-side ignored the codec
        // return value, the trailing bytes would be uninitialized rented
        // memory rather than the codec output.
        var data = System.Text.Encoding.UTF8.GetBytes("hello world hello world hello world");
        using var compressed = CompressedBlock.CompressPooled(data, Lz4Compressor.Instance);

        // Independent compression for cross-check.
        var maxSize = Lz4Compressor.Instance.GetMaxCompressedSize(data.Length);
        var indep = new byte[maxSize];
        var indepLen = Lz4Compressor.Instance.Compress(data, indep);

        // Compressed payload starts at offset 25 (16 checksum + 9 header).
        var payload = compressed.Span[25..];
        Assert.Equal(indepLen, payload.Length);
        Assert.True(payload.SequenceEqual(indep.AsSpan(0, indepLen)),
            "CompressedBlock payload must match independent codec output exactly — " +
            "any divergence indicates the rent buffer's trailing bytes leaked into the payload.");
    }

    [Fact]
    public void CompressPooled_HighlyCompressibleInput_PayloadIsActuallyShorterThanInput()
    {
        // Sanity: compressing a 10kB run of identical bytes produces a
        // payload much smaller than 10kB. If the code mistakenly used the
        // rent size, the payload would be ~10kB regardless.
        var data = new byte[10_000];
        Array.Fill(data, (byte)0x42);

        using var compressed = CompressedBlock.CompressPooled(data, Lz4Compressor.Instance);

        // 25 bytes header + a small compressed payload.
        Assert.True(compressed.Length < 1_000,
            $"Highly compressible input should produce a small block; got {compressed.Length} bytes.");
    }
}
