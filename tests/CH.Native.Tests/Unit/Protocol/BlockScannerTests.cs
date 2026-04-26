using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnSkippers;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Protocol;

/// <summary>
/// Pins the two-pass scan contract for <see cref="Block"/>:
/// <list type="bullet">
///   <item><see cref="Block.TryReadBlockHeader"/> returns null on incomplete bytes
///         so the pump can fetch more without losing position.</item>
///   <item><see cref="Block.TrySkipBlockColumns"/> returns false on truncated column
///         data, throws on structurally bad column metadata.</item>
/// </list>
/// The scan pass is what lets the connection pump decide between "wait for more bytes"
/// and "tear this connection down" — without it the pump would either deadlock waiting
/// for bytes that will never arrive, or attempt to parse half a block and corrupt the
/// next read.
/// </summary>
public class BlockScannerTests
{
    // ------ TryReadBlockHeader ------

    [Fact]
    public void TryReadBlockHeader_TruncatedBlockInfo_ReturnsNull()
    {
        // Compose a full empty block, then strip the trailing bytes one at a time
        // to be sure the truncation lands inside the BlockInfo / count VarInts.
        var full = ProtocolByteBuilder.Build((ref ProtocolWriter w) =>
        {
            BlockInfo.Default.Write(ref w);
            w.WriteVarInt(0); // num columns
            w.WriteVarInt(0); // num rows
        });

        var truncated = full.AsSpan(0, full.Length - 2).ToArray();
        var reader = new ProtocolReader(ProtocolByteBuilder.AsSingleSegment(truncated));

        Assert.Null(Block.TryReadBlockHeader(ref reader));
    }

    [Fact]
    public void TryReadBlockHeader_CompleteEmptyHeader_ReturnsHeader()
    {
        var bytes = ProtocolByteBuilder.Build((ref ProtocolWriter w) =>
        {
            BlockInfo.Default.Write(ref w);
            w.WriteVarInt(0);
            w.WriteVarInt(0);
        });

        var reader = new ProtocolReader(ProtocolByteBuilder.AsSingleSegment(bytes));
        var header = Block.TryReadBlockHeader(ref reader);

        Assert.NotNull(header);
        Assert.Equal(0, header!.Value.ColumnCount);
        Assert.Equal(0, header.Value.RowCount);
    }

    // ------ TrySkipBlockColumns ------

    [Fact]
    public void TrySkipBlockColumns_ZeroColumns_ReturnsTrue()
    {
        // No columns => nothing to skip; should succeed against an empty buffer.
        var reader = new ProtocolReader(ReadOnlySequence<byte>.Empty);
        Assert.True(Block.TrySkipBlockColumns(
            ref reader, ColumnSkipperRegistry.Default,
            columnCount: 0, rowCount: 0, protocolVersion: ProtocolVersion.Current));
    }

    [Fact]
    public void TrySkipBlockColumns_CompleteSingleColumnBlock_ReturnsTrue()
    {
        // One UInt8 column with three rows. Wire shape:
        //   string(name) string(type) [custom-serialization byte 0] N raw bytes of data
        var bytes = ProtocolByteBuilder.Build((ref ProtocolWriter w) =>
        {
            w.WriteString("c");
            w.WriteString("UInt8");
            w.WriteByte(0); // hasCustom = 0 (protocol >= WithCustomSerialization)
            w.WriteByte(1); w.WriteByte(2); w.WriteByte(3);
        });

        var reader = new ProtocolReader(ProtocolByteBuilder.AsSingleSegment(bytes));
        Assert.True(Block.TrySkipBlockColumns(
            ref reader, ColumnSkipperRegistry.Default,
            columnCount: 1, rowCount: 3, protocolVersion: ProtocolVersion.Current));

        Assert.Equal(bytes.Length, reader.Consumed);
    }

    [Fact]
    public void TrySkipBlockColumns_TruncatedColumnHeader_ReturnsFalse()
    {
        // Build a complete header then chop bytes off the type-name string.
        var full = ProtocolByteBuilder.Build((ref ProtocolWriter w) =>
        {
            w.WriteString("c");
            w.WriteString("UInt8");
            w.WriteByte(0);
            w.WriteByte(1); w.WriteByte(2); w.WriteByte(3);
        });

        // Strip the data bytes plus a few from the type-name payload — the truncation
        // must land before the (header completes) checkpoint inside TrySkipBlockColumns.
        var truncated = full.AsSpan(0, 4).ToArray(); // keep "c" + start of "UInt8"
        var reader = new ProtocolReader(ProtocolByteBuilder.AsSingleSegment(truncated));

        Assert.False(Block.TrySkipBlockColumns(
            ref reader, ColumnSkipperRegistry.Default,
            columnCount: 1, rowCount: 3, protocolVersion: ProtocolVersion.Current));
    }

    [Fact]
    public void TrySkipBlockColumns_TruncatedColumnData_ReturnsFalse()
    {
        // Header is complete but only 2 of 3 data bytes arrive.
        var bytes = ProtocolByteBuilder.Build((ref ProtocolWriter w) =>
        {
            w.WriteString("c");
            w.WriteString("UInt8");
            w.WriteByte(0);
            w.WriteByte(1); w.WriteByte(2);
        });

        var reader = new ProtocolReader(ProtocolByteBuilder.AsSingleSegment(bytes));

        Assert.False(Block.TrySkipBlockColumns(
            ref reader, ColumnSkipperRegistry.Default,
            columnCount: 1, rowCount: 3, protocolVersion: ProtocolVersion.Current));
    }

    [Fact]
    public void TrySkipBlockColumns_UnknownColumnTypeName_Throws()
    {
        // Header parses cleanly but the type name doesn't map to any registered
        // skipper. The skip pass must throw — not return false — because no amount
        // of additional bytes will turn "DefinitelyNotAType" into something we know
        // how to skip. Returning false here would deadlock the connection pump.
        var bytes = ProtocolByteBuilder.Build((ref ProtocolWriter w) =>
        {
            w.WriteString("c");
            w.WriteString("DefinitelyNotAType");
            w.WriteByte(0);
        });

        var reader = new ProtocolReader(ProtocolByteBuilder.AsSingleSegment(bytes));

        NotSupportedException? caught = null;
        try
        {
            Block.TrySkipBlockColumns(
                ref reader, ColumnSkipperRegistry.Default,
                columnCount: 1, rowCount: 1, protocolVersion: ProtocolVersion.Current);
        }
        catch (NotSupportedException ex) { caught = ex; }

        Assert.NotNull(caught);
        Assert.Contains("DefinitelyNotAType", caught!.Message);
    }
}
