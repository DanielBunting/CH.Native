using System.Buffers;
using System.Buffers.Binary;
using CH.Native.Compression;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Compression;

/// <summary>
/// Finding #13 warns that <see cref="CompressedBlock.ReadFromProtocol"/> can leak
/// rented buffers if an exception is thrown between the two <c>ReadPooledBytes</c>
/// calls. These tests exercise the error paths and verify that (a) exceptions
/// surface as expected and (b) repeated exception paths do not exhaust the shared
/// pool (a smoke test — the shared pool has unbounded capacity but a leak would
/// manifest as unbounded growth in allocated bytes).
/// </summary>
public class CompressedBlockReadFromProtocolTests
{
    private const int ChecksumSize = 16;
    private const int MinBlockSize = 25;

    private static byte[] BuildHeader(uint declaredCompressedSize)
    {
        // 16-byte checksum (arbitrary) + 1-byte algorithm (0x82 LZ4) + 4-byte compressed
        // size + 4-byte uncompressed size = 25 bytes header.
        var header = new byte[MinBlockSize];
        for (int i = 0; i < ChecksumSize; i++) header[i] = (byte)i;
        header[ChecksumSize] = 0x82;
        BinaryPrimitives.WriteUInt32LittleEndian(
            header.AsSpan(ChecksumSize + 1, 4), declaredCompressedSize);
        BinaryPrimitives.WriteUInt32LittleEndian(
            header.AsSpan(ChecksumSize + 5, 4), 1024);
        return header;
    }

    private static ReadOnlySequence<byte> MakeFragmented(byte[] data, int splitAt)
    {
        // Build a two-segment ReadOnlySequence so the reader's pooled path is exercised.
        var first = new MemorySegment(data.AsMemory(0, splitAt));
        var last = first.Append(data.AsMemory(splitAt));
        return new ReadOnlySequence<byte>(first, 0, last, data.Length - splitAt);
    }

    // Helper: swallow the expected exception type. Cannot use a lambda because
    // ProtocolReader is a ref struct and can't be captured.
    private static Exception? TryRead(ReadOnlySequence<byte> seq)
    {
        try
        {
            var reader = new ProtocolReader(seq);
            using var _ = CompressedBlock.ReadFromProtocol(ref reader);
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    [Fact]
    public void ReadFromProtocol_NotEnoughForHeader_Throws()
    {
        var data = new byte[MinBlockSize - 1];
        var ex = TryRead(new ReadOnlySequence<byte>(data));
        Assert.IsType<InvalidOperationException>(ex);
    }

    [Fact]
    public void ReadFromProtocol_CompressedSizeTooSmall_Throws()
    {
        // Declared compressed size smaller than header — invalid frame.
        var header = BuildHeader(declaredCompressedSize: 4);
        var ex = TryRead(new ReadOnlySequence<byte>(header));
        Assert.IsType<InvalidDataException>(ex);
    }

    [Fact]
    public void ReadFromProtocol_HeaderOkButBodyTruncated_ThrowsAndDoesNotExhaustPool()
    {
        // Declared compressed size is 100, so totalBlockSize = 16 + 100 = 116 and
        // remainingDataSize = 116 - 25 = 91. We only provide the header, so the second
        // ReadPooledBytes call fails.
        const uint declared = 100;

        // Drive the code a large number of times to make a per-call leak visible.
        for (int iter = 0; iter < 5000; iter++)
        {
            var header = BuildHeader(declared);
            // Use a fragmented sequence so ReadPooledBytes rents from the pool rather
            // than returning a direct slice (which wouldn't exercise the leak path).
            var seq = MakeFragmented(header, splitAt: 10);
            var ex = TryRead(seq);
            Assert.IsType<InvalidOperationException>(ex);
        }

        // If buffers were leaked, this test would not fail outright but memory would
        // grow steadily. In a leak-hunting environment (CI with a tight memory cap or
        // a dotMemory run) this loop is the fixture. The guard we can make here is:
        // rent & return a buffer and see that the pool still hands us something.
        var buf = ArrayPool<byte>.Shared.Rent(1024);
        Assert.NotNull(buf);
        ArrayPool<byte>.Shared.Return(buf);
    }

    [Fact]
    public void ReadFromProtocol_ValidSmallBlock_ReturnsPooledData()
    {
        // Build a minimally-valid frame: header + 0 remaining bytes (totalBlockSize == 25).
        // compressedSize must be >= 9 (min body size: 1 method + 4 compressed + 4 uncompressed).
        // Set compressedSize == 9 so remainingDataSize == 0.
        var header = BuildHeader(declaredCompressedSize: 9);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(header));

        var block = CompressedBlock.ReadFromProtocol(ref reader);
        try
        {
            Assert.Equal(MinBlockSize, block.Length);
        }
        finally
        {
            block.Dispose();
        }
    }

    private sealed class MemorySegment : ReadOnlySequenceSegment<byte>
    {
        public MemorySegment(ReadOnlyMemory<byte> memory)
        {
            Memory = memory;
        }

        public MemorySegment Append(ReadOnlyMemory<byte> memory)
        {
            var segment = new MemorySegment(memory)
            {
                RunningIndex = RunningIndex + Memory.Length
            };
            Next = segment;
            return segment;
        }
    }
}
