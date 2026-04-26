using System.Buffers;
using CH.Native.Protocol;
using CH.Native.Protocol.Messages;
using Xunit;

namespace CH.Native.Tests.Unit.Protocol;

/// <summary>
/// Pins the contract on <see cref="ExceptionMessage.TryScan"/> and
/// <see cref="ProgressMessage.TryScan"/>: returns false on incomplete bytes,
/// true on a complete message, throws on structurally malformed bytes. These
/// scans are the gate that lets the connection pump distinguish "wait for more
/// bytes" from "tear this connection down" for non-Data messages — without them
/// the pump used to rely on catching <see cref="InvalidOperationException"/>,
/// which couldn't tell incomplete data from a real parser bug.
/// </summary>
public class MessageTryScanTests
{
    private static byte[] BuildExceptionBytes(int code, string name, string message, string stackTrace, bool hasNested)
    {
        return ProtocolByteBuilder.Build((ref ProtocolWriter w) =>
        {
            w.WriteInt32(code);
            w.WriteString(name);
            w.WriteString(message);
            w.WriteString(stackTrace);
            w.WriteByte(hasNested ? (byte)1 : (byte)0);
        });
    }

    [Fact]
    public void ExceptionMessage_TryScan_CompleteMessage_ReturnsTrue()
    {
        var bytes = BuildExceptionBytes(241, "DB::MemoryLimit", "out of memory", "stack here", hasNested: false);
        var reader = new ProtocolReader(ProtocolByteBuilder.AsSingleSegment(bytes));
        Assert.True(ExceptionMessage.TryScan(ref reader));
        Assert.Equal(bytes.Length, reader.Consumed);
    }

    [Fact]
    public void ExceptionMessage_TryScan_TruncatedAfterCode_ReturnsFalse()
    {
        var full = BuildExceptionBytes(241, "DB::SomeError", "some message", "stack", hasNested: false);
        // Keep just the 4-byte code, drop everything else.
        var truncated = full.AsSpan(0, 4).ToArray();
        var reader = new ProtocolReader(ProtocolByteBuilder.AsSingleSegment(truncated));
        Assert.False(ExceptionMessage.TryScan(ref reader));
    }

    [Fact]
    public void ExceptionMessage_TryScan_TruncatedMidString_ReturnsFalse()
    {
        var full = BuildExceptionBytes(241, "name", "messageVeryLong", "stack", hasNested: false);
        // Drop the trailing few bytes — the truncation lands inside the stack string.
        var truncated = full.AsSpan(0, full.Length - 3).ToArray();
        var reader = new ProtocolReader(ProtocolByteBuilder.AsSingleSegment(truncated));
        Assert.False(ExceptionMessage.TryScan(ref reader));
    }

    [Fact]
    public void ExceptionMessage_TryScan_NestedExceptionComplete_ReturnsTrue()
    {
        // hasNested=1 followed by a second complete exception.
        var nestedBytes = BuildExceptionBytes(242, "Inner", "inner msg", "inner stack", hasNested: false);
        var outerBytes = BuildExceptionBytes(241, "Outer", "outer msg", "outer stack", hasNested: true);
        var combined = outerBytes.Concat(nestedBytes).ToArray();

        var reader = new ProtocolReader(ProtocolByteBuilder.AsSingleSegment(combined));
        Assert.True(ExceptionMessage.TryScan(ref reader));
        Assert.Equal(combined.Length, reader.Consumed);
    }

    [Fact]
    public void ExceptionMessage_TryScan_NestedExceptionTruncated_ReturnsFalse()
    {
        var nestedBytes = BuildExceptionBytes(242, "Inner", "inner msg", "inner stack", hasNested: false);
        var outerBytes = BuildExceptionBytes(241, "Outer", "outer msg", "outer stack", hasNested: true);
        var combined = outerBytes.Concat(nestedBytes.Take(2)).ToArray(); // chop the nested

        var reader = new ProtocolReader(ProtocolByteBuilder.AsSingleSegment(combined));
        Assert.False(ExceptionMessage.TryScan(ref reader));
    }

    [Fact]
    public void ExceptionMessage_TryScan_FragmentedAcrossSegments_ReturnsTrue()
    {
        // Same complete payload, but split into 1-byte segments — exercises the
        // SequenceReader segment-crossing path that single-segment tests miss.
        var bytes = BuildExceptionBytes(241, "DB::Err", "msg", "stack", hasNested: false);
        var reader = new ProtocolReader(ProtocolByteBuilder.AsFragmented(bytes, segmentSize: 1));
        Assert.True(ExceptionMessage.TryScan(ref reader));
    }

    // ---------------- ProgressMessage ----------------

    private static byte[] BuildProgressBytes(int protocolRevision)
    {
        return ProtocolByteBuilder.Build((ref ProtocolWriter w) =>
        {
            w.WriteVarInt(100);   // rows
            w.WriteVarInt(2048);  // bytes
            w.WriteVarInt(1000);  // totalRows
            if (protocolRevision >= ProtocolVersion.WithTotalBytesInProgress) w.WriteVarInt(20480);
            if (protocolRevision >= ProtocolVersion.WithClientWriteInfo) { w.WriteVarInt(0); w.WriteVarInt(0); }
            if (protocolRevision >= ProtocolVersion.WithServerQueryTimeInProgress) w.WriteVarInt(123456);
        });
    }

    [Fact]
    public void ProgressMessage_TryScan_CompleteMessage_ReturnsTrue()
    {
        var bytes = BuildProgressBytes(ProtocolVersion.Current);
        var reader = new ProtocolReader(ProtocolByteBuilder.AsSingleSegment(bytes));
        Assert.True(ProgressMessage.TryScan(ref reader, ProtocolVersion.Current));
        Assert.Equal(bytes.Length, reader.Consumed);
    }

    [Fact]
    public void ProgressMessage_TryScan_TruncatedReturnsFalse()
    {
        var full = BuildProgressBytes(ProtocolVersion.Current);
        var truncated = full.AsSpan(0, full.Length - 1).ToArray();
        var reader = new ProtocolReader(ProtocolByteBuilder.AsSingleSegment(truncated));
        Assert.False(ProgressMessage.TryScan(ref reader, ProtocolVersion.Current));
    }

    [Fact]
    public void ProgressMessage_TryScan_OldRevisionRespectsFieldGating()
    {
        // For an older revision that doesn't have the WithClientWriteInfo / elapsed_ns
        // fields, the scan must succeed on a shorter payload.
        var oldRevision = ProtocolVersion.WithTotalBytesInProgress;
        var bytes = BuildProgressBytes(oldRevision);
        var reader = new ProtocolReader(ProtocolByteBuilder.AsSingleSegment(bytes));
        Assert.True(ProgressMessage.TryScan(ref reader, oldRevision));
    }

    // ---------------- ProfileInfo (skip-only — no parsed message type) ----------------
    //
    // Implementation lives on ClickHouseConnection.TrySkipProfileInfo (private) so we
    // can't call it directly from a unit test. Instead validate the wire shape by
    // building bytes that mirror what TrySkipProfileInfo expects to consume — the
    // scan equivalent uses TrySkipVarInt + TryReadByte directly, so a round-trip
    // through ProtocolByteBuilder + manual scan exercises the same code paths.
    //
    // The system tests in BulkInsertPumpTests.MultiMessageSequence cover the
    // integrated pump-level behaviour.

    private static byte[] BuildProfileInfoBytes(int protocolRevision)
    {
        return ProtocolByteBuilder.Build((ref ProtocolWriter w) =>
        {
            w.WriteVarInt(100);  // rows
            w.WriteVarInt(2);    // blocks
            w.WriteVarInt(2048); // bytes
            w.WriteByte(0);      // applied_limit
            w.WriteVarInt(0);    // rows_before_limit
            w.WriteByte(0);      // calculated_rows_before_limit
            if (protocolRevision >= ProtocolVersion.WithRowsBeforeAggregation)
            {
                w.WriteByte(0);     // applied_aggregation
                w.WriteVarInt(0);   // rows_before_aggregation
            }
        });
    }

    [Fact]
    public void ProfileInfo_WireShape_RoundTripsThroughManualScan()
    {
        // Manually mirror TrySkipProfileInfo's field sequence to lock in the wire
        // shape. If a future contributor adds a field to SkipProfileInfo without
        // also updating TrySkipProfileInfo, this test won't catch it directly —
        // but it *does* document the expected layout in test code that lives
        // alongside the parser, so the drift will be obvious in review.
        var revision = ProtocolVersion.Current;
        var bytes = BuildProfileInfoBytes(revision);
        var reader = new ProtocolReader(ProtocolByteBuilder.AsSingleSegment(bytes));

        Assert.True(reader.TrySkipVarInt());
        Assert.True(reader.TrySkipVarInt());
        Assert.True(reader.TrySkipVarInt());
        Assert.True(reader.TryReadByte(out _));
        Assert.True(reader.TrySkipVarInt());
        Assert.True(reader.TryReadByte(out _));
        if (revision >= ProtocolVersion.WithRowsBeforeAggregation)
        {
            Assert.True(reader.TryReadByte(out _));
            Assert.True(reader.TrySkipVarInt());
        }
        Assert.Equal(bytes.Length, reader.Consumed);
    }

    [Fact]
    public void ProfileInfo_TruncatedReturnsFalse_ManualScan()
    {
        // Truncate so the scan can't complete — reader's TrySkip* returns false.
        var bytes = BuildProfileInfoBytes(ProtocolVersion.Current);
        var truncated = bytes.AsSpan(0, bytes.Length - 1).ToArray();
        var reader = new ProtocolReader(ProtocolByteBuilder.AsSingleSegment(truncated));

        // Walk through the fields — at least one Try* call must return false on
        // a truncated payload.
        bool allOk =
            reader.TrySkipVarInt() &&
            reader.TrySkipVarInt() &&
            reader.TrySkipVarInt() &&
            reader.TryReadByte(out _) &&
            reader.TrySkipVarInt() &&
            reader.TryReadByte(out _);
        if (allOk && ProtocolVersion.Current >= ProtocolVersion.WithRowsBeforeAggregation)
        {
            allOk = reader.TryReadByte(out _) && reader.TrySkipVarInt();
        }
        Assert.False(allOk);
    }
}
