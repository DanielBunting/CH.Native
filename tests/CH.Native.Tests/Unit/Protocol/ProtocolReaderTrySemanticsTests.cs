using System.Buffers;
using CH.Native.Exceptions;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Protocol;

/// <summary>
/// Pins the boundary contract documented on <see cref="ProtocolReader"/>:
/// <list type="bullet">
///   <item>Try* returns <c>true</c> on success and advances exactly past the value.</item>
///   <item>Try* returns <c>false</c> when the stream is incomplete; reader position is
///         unspecified and the caller must rebuild a fresh reader from the original buffer.</item>
///   <item>Try* throws when the bytes are structurally malformed; connection must be torn down.</item>
/// </list>
/// Soft failures (false-on-malformed) deadlock the pump waiting for bytes that will
/// never arrive; hard failures (throw-on-incomplete) kill healthy connections under
/// network fragmentation. These tests guard both halves.
/// </summary>
public class ProtocolReaderTrySemanticsTests
{
    // ---------------- TryReadVarInt ----------------

    [Fact]
    public void TryReadVarInt_HappyPath_SingleSegment_ReturnsValue()
    {
        // 150 encodes as 0x96 0x01 (continuation + 7 bits, then high bits with no continuation).
        var seq = ProtocolByteBuilder.AsSingleSegment(new byte[] { 0x96, 0x01 });
        var reader = new ProtocolReader(seq);

        Assert.True(reader.TryReadVarInt(out var value));
        Assert.Equal(150UL, value);
        Assert.Equal(2, reader.Consumed);
    }

    [Fact]
    public void TryReadVarInt_HappyPath_FragmentedAcrossSegments_ReturnsValue()
    {
        // Same 150 payload but each byte in its own segment — exercises the
        // SequenceReader segment-crossing path that single-segment tests miss.
        var seq = ProtocolByteBuilder.AsFragmented(new byte[] { 0x96, 0x01 }, segmentSize: 1);
        var reader = new ProtocolReader(seq);

        Assert.True(reader.TryReadVarInt(out var value));
        Assert.Equal(150UL, value);
        Assert.Equal(2, reader.Consumed);
    }

    [Fact]
    public void TryReadVarInt_PartialBytes_ReturnsFalse()
    {
        // Only the continuation byte arrived — caller must wait for more bytes.
        // The reader's position after `false` is unspecified by contract; do not
        // assert on it (see ProtocolReader doc comment). The test below documents
        // the rewind behaviour separately.
        var seq = ProtocolByteBuilder.AsSingleSegment(new byte[] { 0x96 });
        var reader = new ProtocolReader(seq);

        Assert.False(reader.TryReadVarInt(out _));
    }

    [Fact]
    public void TryReadVarInt_FalseReturn_DocumentsCallerRewindContract()
    {
        // The contract is: on false, the caller throws away the reader and rebuilds
        // a fresh one from the original sequence start (typically after pumping more
        // bytes from the pipe). This test pins that pattern by simulating it: the
        // first attempt fails on a one-byte truncation, then we "receive more bytes"
        // and rebuild — the second attempt succeeds and yields the correct value.
        var bufferSoFar = new byte[] { 0x96 };

        // Attempt 1: not enough bytes.
        var reader1 = new ProtocolReader(ProtocolByteBuilder.AsSingleSegment(bufferSoFar));
        Assert.False(reader1.TryReadVarInt(out _));

        // Pretend the next chunk arrived. Build a fresh reader from the start of
        // the (now larger) buffer — DO NOT continue from reader1's position.
        var fullBuffer = new byte[] { 0x96, 0x01 };
        var reader2 = new ProtocolReader(ProtocolByteBuilder.AsSingleSegment(fullBuffer));
        Assert.True(reader2.TryReadVarInt(out var value));
        Assert.Equal(150UL, value);
    }

    [Fact]
    public void TryReadVarInt_TenContinuationBytes_Throws_InvalidDataException()
    {
        // VarInt.MaxLength = 10. Ten consecutive bytes with the continuation bit
        // set is structurally malformed — must throw, not return false.
        var ten80s = new byte[10];
        Array.Fill(ten80s, (byte)0x80);
        var seq = ProtocolByteBuilder.AsSingleSegment(ten80s);
        var reader = new ProtocolReader(seq);

        // Can't capture a ref struct in a lambda for Assert.Throws — drive the
        // call inline and rethrow into a typed assert.
        InvalidDataException? caught = null;
        try { reader.TryReadVarInt(out _); }
        catch (InvalidDataException ex) { caught = ex; }

        Assert.NotNull(caught);
        Assert.Contains("continuation bit", caught!.Message);
    }

    // ---------------- TryReadByte ----------------

    [Fact]
    public void TryReadByte_OnEmptySequence_ReturnsFalse()
    {
        var reader = new ProtocolReader(ReadOnlySequence<byte>.Empty);
        Assert.False(reader.TryReadByte(out _));
    }

    // ---------------- TryReadUInt64 ----------------

    [Fact]
    public void TryReadUInt64_FivePartialBytes_ReturnsFalse()
    {
        // UInt64 needs 8 bytes; 5 is not enough.
        var reader = new ProtocolReader(ProtocolByteBuilder.AsSingleSegment(new byte[5]));
        Assert.False(reader.TryReadUInt64(out _));
    }

    [Fact]
    public void TryReadUInt64_HappyPath_ReturnsLittleEndianValue()
    {
        // 0x0102030405060708 little-endian
        var bytes = new byte[] { 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01 };
        var reader = new ProtocolReader(ProtocolByteBuilder.AsSingleSegment(bytes));
        Assert.True(reader.TryReadUInt64(out var value));
        Assert.Equal(0x0102030405060708UL, value);
    }

    // ---------------- TrySkipBytes ----------------

    [Fact]
    public void TrySkipBytes_AcrossManySegments_AdvancesCorrectly()
    {
        // 1 KiB split into 1024 single-byte segments — worst-case fragmentation.
        var payload = new byte[1024];
        new Random(42).NextBytes(payload);
        var seq = ProtocolByteBuilder.AsFragmented(payload, segmentSize: 1);
        var reader = new ProtocolReader(seq);

        Assert.True(reader.TrySkipBytes(1024));
        Assert.Equal(1024, reader.Consumed);
    }

    [Fact]
    public void TrySkipBytes_NotEnoughBytes_ReturnsFalse()
    {
        var reader = new ProtocolReader(ProtocolByteBuilder.AsSingleSegment(new byte[5]));
        Assert.False(reader.TrySkipBytes(10));
    }

    // ---------------- TrySkipString ----------------

    [Fact]
    public void TrySkipString_HappyPath_ReturnsTrue()
    {
        var bytes = ProtocolByteBuilder.Build((ref ProtocolWriter w) => w.WriteString("hello"));
        var reader = new ProtocolReader(ProtocolByteBuilder.AsSingleSegment(bytes));

        Assert.True(reader.TrySkipString());
        Assert.Equal(bytes.Length, reader.Consumed);
    }

    [Fact]
    public void TrySkipString_TruncatedPayload_ReturnsFalse()
    {
        // Length prefix says 10 bytes; supply only 5.
        var bytes = ProtocolByteBuilder.Build((ref ProtocolWriter w) =>
        {
            w.WriteVarInt(10);
            w.WriteBytes(new byte[5]);
        });
        var reader = new ProtocolReader(ProtocolByteBuilder.AsSingleSegment(bytes));

        Assert.False(reader.TrySkipString());
    }

    [Fact]
    public void TrySkipString_TruncatedLengthPrefix_ReturnsFalse()
    {
        // Single continuation byte — length VarInt is incomplete, no payload.
        var reader = new ProtocolReader(ProtocolByteBuilder.AsSingleSegment(new byte[] { 0x80 }));
        Assert.False(reader.TrySkipString());
    }

    // ---------------- ReadString (allocating) ----------------

    [Fact]
    public void ReadString_LengthExceedsConfiguredCap_ThrowsClickHouseProtocolException()
    {
        // Configure a tiny cap and supply a length-prefix that exceeds it. The
        // length-prefix must be checked BEFORE allocation — that's the whole point
        // of the cap (defends against a hostile server forcing a multi-GiB allocation).
        var bytes = ProtocolByteBuilder.Build((ref ProtocolWriter w) =>
        {
            w.WriteVarInt(1024);                // length: 1 KiB
            w.WriteBytes(new byte[1024]);       // payload (test data — won't be read)
        });
        var reader = new ProtocolReader(ProtocolByteBuilder.AsSingleSegment(bytes))
        {
            MaxStringLengthBytes = 100,         // tiny cap — 1 KiB > 100 should throw
        };

        ClickHouseProtocolException? caught = null;
        try { reader.ReadString(); }
        catch (ClickHouseProtocolException ex) { caught = ex; }

        Assert.NotNull(caught);
        Assert.Contains("MaxStringLengthBytes", caught!.Message);
        Assert.Contains("100", caught.Message);
    }

    [Fact]
    public void TrySkipString_LengthExceedsConfiguredCap_ThrowsClickHouseProtocolException()
    {
        // The skip pass must reject oversized lengths too — otherwise a malformed
        // length on the scan pass would either skip past phantom payload (resyncing
        // to garbage) or wait forever for bytes that will never arrive.
        var bytes = ProtocolByteBuilder.Build((ref ProtocolWriter w) =>
        {
            w.WriteVarInt(1024);
        });
        var reader = new ProtocolReader(ProtocolByteBuilder.AsSingleSegment(bytes))
        {
            MaxStringLengthBytes = 100,
        };

        ClickHouseProtocolException? caught = null;
        try { reader.TrySkipString(); }
        catch (ClickHouseProtocolException ex) { caught = ex; }

        Assert.NotNull(caught);
        Assert.Contains("MaxStringLengthBytes", caught!.Message);
    }

    [Fact]
    public void ReadString_LengthExceedsInt32_ThrowsClickHouseProtocolException()
    {
        // Encode a VarInt = ulong.MaxValue to simulate a hostile/malformed length
        // prefix. ReadVarIntAsInt32 -> ProtocolGuards.ToInt32 should reject.
        var bytes = ProtocolByteBuilder.Build((ref ProtocolWriter w) => w.WriteVarInt(ulong.MaxValue));
        var reader = new ProtocolReader(ProtocolByteBuilder.AsSingleSegment(bytes));

        ClickHouseProtocolException? caught = null;
        try { reader.ReadString(); }
        catch (ClickHouseProtocolException ex) { caught = ex; }

        Assert.NotNull(caught);
        Assert.Contains("string length", caught!.Message);
        Assert.Contains("Int32.MaxValue", caught.Message);
    }
}
