using System.Buffers;
using CH.Native.Data;
using CH.Native.Exceptions;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data;

/// <summary>
/// Pins that server-declared counts in a block header are sanity-gated against
/// the bytes actually available BEFORE any allocation sized by them. A varint
/// costs the sender 5 bytes; without these gates a hostile or desynced stream
/// could declare ~2^31 columns/rows and drive multi-GB allocations (or instant
/// OutOfMemoryException) from a handful of wire bytes. The compressed read path
/// has no pre-scan, so these gates are its only defense.
/// </summary>
public class ResourceBoundBlockTests
{
    private static ColumnReaderRegistry Registry => ColumnReaderRegistry.Default;

    private static TypedBlock Parse(byte[] bytes)
    {
        // ProtocolReader is a ref struct, so it cannot cross a lambda boundary —
        // construct and consume it inside this helper.
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        return Block.ReadTypedBlockWithTableName(ref reader, Registry, tableName: "");
    }

    private static byte[] BuildBlockHeader(ulong columnCount, ulong rowCount, (string Name, string Type)[]? columns = null)
    {
        var bw = new ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);
        BlockInfo.Default.Write(ref w);
        w.WriteVarInt(columnCount);
        w.WriteVarInt(rowCount);
        foreach (var (name, type) in columns ?? Array.Empty<(string, string)>())
        {
            w.WriteString(name);
            w.WriteString(type);
        }
        // Deliberately no column DATA — the guards must fire before any
        // count-sized allocation, never get as far as underrunning on values.
        return bw.WrittenMemory.ToArray();
    }

    [Fact]
    public void HugeColumnCount_ThrowsProtocolException_BeforeAllocating()
    {
        // 5 wire bytes declare int.MaxValue columns. Pre-guard this attempted
        // three count-sized arrays (a ~17 GB reference array → instant OOM at
        // best, a real multi-GB allocation at worst). The guard must reject the
        // count against Remaining with a typed protocol exception instead.
        var bytes = BuildBlockHeader(columnCount: int.MaxValue, rowCount: 0);

        var ex = Assert.Throws<ClickHouseProtocolException>(() => Parse(bytes));
        Assert.Contains("column count", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void HugeRowCount_FixedWidthColumn_ThrowsProtocolException_BeforeRenting()
    {
        // One valid Int32 column, but a declared rowCount whose byte size
        // (~2 GB) vastly exceeds the bytes on the wire. Pre-guard this rented a
        // ~2 GB array from the pool before the value reads underran. The guard
        // must reject rowCount against Remaining before renting.
        const ulong hugeRows = 536_870_000; // ×4 bytes just under int.MaxValue → passes the overflow guard
        var bytes = BuildBlockHeader(columnCount: 1, rowCount: hugeRows, new[] { ("c", "Int32") });

        var ex = Assert.Throws<ClickHouseProtocolException>(() => Parse(bytes));
        Assert.Contains("rowCount", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void PlausibleCounts_StillParse()
    {
        // Guard must not reject legitimate blocks: 1 column, 2 rows, data present.
        var bw = new ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);
        BlockInfo.Default.Write(ref w);
        w.WriteVarInt(1);          // columns
        w.WriteVarInt(2);          // rows
        w.WriteString("c");
        w.WriteString("Int32");
        w.WriteInt32(7);
        w.WriteInt32(9);
        var block = Parse(bw.WrittenMemory.ToArray());
        Assert.Equal(1, block.Columns.Length);
        Assert.Equal(2, block.RowCount);
    }
}
