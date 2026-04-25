using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnSkippers;
using CH.Native.Exceptions;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Protocol;

/// <summary>
/// Regression tests for the silent (int)ulong truncation bug on wire-supplied
/// lengths/offsets. Every cast that was unchecked should now throw a typed
/// ClickHouseProtocolException when a value exceeds int.MaxValue rather than
/// silently wrapping to a negative int (or throwing raw OverflowException —
/// the typed wrapper lets the connection layer recognise it as fatal and
/// tear the protocol stream down cleanly).
/// </summary>
public class WireOverflowGuardTests
{
    // First ulong that overflows a signed int: 2^31 = 0x8000_0000.
    private const ulong OverflowUInt = (ulong)int.MaxValue + 1;

    private static ReadOnlySequence<byte> Seq(ArrayBufferWriter<byte> buf)
        => new(buf.WrittenMemory);

    // ---- ProtocolReader.ReadString ---------------------------------------

    [Fact]
    public void ReadString_LengthOverflowsInt_Throws()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);
        writer.WriteVarInt(OverflowUInt);
        var seq = Seq(buf);

        Assert.Throws<ClickHouseProtocolException>(() =>
        {
            var reader = new ProtocolReader(seq);
            reader.ReadString();
        });
    }

    // ---- Block header -----------------------------------------------------

    [Fact]
    public void ReadTypedBlockWithTableName_ColumnCountOverflows_Throws()
    {
        var seq = Seq(BlockHeaderBuffer(columnCount: OverflowUInt, rowCount: 0));
        Assert.Throws<ClickHouseProtocolException>(() =>
        {
            var pr = new ProtocolReader(seq);
            Block.ReadTypedBlockWithTableName(ref pr, ColumnReaderRegistry.Default, "t");
        });
    }

    [Fact]
    public void ReadTypedBlockWithTableName_RowCountOverflows_Throws()
    {
        var seq = Seq(BlockHeaderBuffer(columnCount: 0, rowCount: OverflowUInt));
        Assert.Throws<ClickHouseProtocolException>(() =>
        {
            var pr = new ProtocolReader(seq);
            Block.ReadTypedBlockWithTableName(ref pr, ColumnReaderRegistry.Default, "t");
        });
    }

    [Fact]
    public void TryReadBlockHeader_ColumnCountOverflows_Throws()
    {
        var seq = Seq(BlockHeaderBuffer(columnCount: OverflowUInt, rowCount: 0));
        Assert.Throws<ClickHouseProtocolException>(() =>
        {
            var pr = new ProtocolReader(seq);
            Block.TryReadBlockHeader(ref pr);
        });
    }

    private static ArrayBufferWriter<byte> BlockHeaderBuffer(ulong columnCount, ulong rowCount)
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);
        // BlockInfo: field_num=1, is_overflows=0; field_num=2, bucket_num=-1; terminator=0
        writer.WriteVarInt(1ul);
        writer.WriteByte(0);
        writer.WriteVarInt(2ul);
        writer.WriteInt32(-1);
        writer.WriteVarInt(0ul);
        writer.WriteVarInt(columnCount);
        writer.WriteVarInt(rowCount);
        return buf;
    }

    // ---- StringColumnReader lazy path (length VarInt) --------------------

    [Fact]
    public void StringColumnReader_Raw_LengthOverflows_Throws()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);
        writer.WriteVarInt(OverflowUInt); // first string length overflows
        var seq = Seq(buf);

        var reader = (StringColumnReader)ColumnReaderRegistry.LazyStrings.GetReader("String");
        Assert.Throws<ClickHouseProtocolException>(() =>
        {
            var pr = new ProtocolReader(seq);
            reader.ReadRawColumn(ref pr, rowCount: 1);
        });
    }

    // ---- ArrayColumnReader offsets ---------------------------------------

    [Fact]
    public void ArrayColumnReader_ReadTypedColumn_OffsetOverflows_Throws()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);
        writer.WriteUInt64(OverflowUInt); // single-row cumulative offset
        var seq = Seq(buf);

        var reader = ColumnReaderRegistry.Default.GetReader("Array(Int32)");
        Assert.Throws<ClickHouseProtocolException>(() =>
        {
            var pr = new ProtocolReader(seq);
            reader.ReadTypedColumn(ref pr, 1);
        });
    }

    [Fact]
    public void ArrayColumnReader_ReadValue_OffsetOverflows_Throws()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);
        writer.WriteUInt64(OverflowUInt);
        var seq = Seq(buf);

        var reader = (ArrayColumnReader<int>)ColumnReaderRegistry.Default.GetReader("Array(Int32)");
        Assert.Throws<ClickHouseProtocolException>(() =>
        {
            var pr = new ProtocolReader(seq);
            reader.ReadValue(ref pr);
        });
    }

    // ---- MapColumnReader offsets -----------------------------------------

    [Fact]
    public void MapColumnReader_ReadTypedColumn_OffsetOverflows_Throws()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);
        writer.WriteUInt64(OverflowUInt); // single-row cumulative offset
        var seq = Seq(buf);

        var reader = ColumnReaderRegistry.Default.GetReader("Map(String, Int32)");
        Assert.Throws<ClickHouseProtocolException>(() =>
        {
            var pr = new ProtocolReader(seq);
            reader.ReadTypedColumn(ref pr, 1);
        });
    }

    [Fact]
    public void MapColumnReader_ReadValue_CountOverflows_Throws()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);
        writer.WriteUInt64(OverflowUInt);
        var seq = Seq(buf);

        var reader = (MapColumnReader<string, int>)
            ColumnReaderRegistry.Default.GetReader("Map(String, Int32)");
        Assert.Throws<ClickHouseProtocolException>(() =>
        {
            var pr = new ProtocolReader(seq);
            reader.ReadValue(ref pr);
        });
    }

    // ---- LowCardinalityColumnReader --------------------------------------

    [Fact]
    public void LowCardinalityColumnReader_DictSizeOverflows_Throws()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);
        writer.WriteUInt64(1);             // KeysSerializationVersion (ReadPrefix)
        writer.WriteUInt64(0);             // flags (indexType = UInt8)
        writer.WriteUInt64(OverflowUInt);  // dictSize — overflows
        var seq = Seq(buf);

        var reader = ColumnReaderRegistry.Default.GetReader("LowCardinality(String)");
        Assert.Throws<ClickHouseProtocolException>(() =>
        {
            var pr = new ProtocolReader(seq);
            reader.ReadPrefix(ref pr);
            reader.ReadTypedColumn(ref pr, 1);
        });
    }

    [Fact]
    public void LowCardinalityColumnReader_DictionaryEncoded_UInt64IndexOverflows_Throws()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);
        writer.WriteUInt64(1);             // KeysSerializationVersion
        writer.WriteUInt64(3);             // flags: indexType = UInt64
        writer.WriteUInt64(1);             // dictSize = 1
        writer.WriteVarInt(0ul);           // dictionary entry: empty string (length 0)
        writer.WriteUInt64(1);             // indexCount = 1
        writer.WriteUInt64(OverflowUInt);  // single index, overflows int
        var seq = Seq(buf);

        var reader = (LowCardinalityColumnReader<string>)
            ColumnReaderRegistry.Default.GetReader("LowCardinality(String)");
        Assert.Throws<ClickHouseProtocolException>(() =>
        {
            var pr = new ProtocolReader(seq);
            reader.ReadPrefix(ref pr);
            reader.ReadDictionaryEncodedColumn(ref pr, 1);
        });
    }

    // ---- Skippers --------------------------------------------------------

    [Fact]
    public void ArrayColumnSkipper_OffsetOverflows_Throws()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);
        writer.WriteUInt64(OverflowUInt);
        var seq = Seq(buf);

        var skipper = ColumnSkipperRegistry.Default.GetSkipper("Array(Int32)");
        Assert.Throws<ClickHouseProtocolException>(() =>
        {
            var pr = new ProtocolReader(seq);
            skipper.TrySkipColumn(ref pr, 1);
        });
    }

    [Fact]
    public void MapColumnSkipper_OffsetOverflows_Throws()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);
        writer.WriteUInt64(OverflowUInt);
        var seq = Seq(buf);

        var skipper = ColumnSkipperRegistry.Default.GetSkipper("Map(String, Int32)");
        Assert.Throws<ClickHouseProtocolException>(() =>
        {
            var pr = new ProtocolReader(seq);
            skipper.TrySkipColumn(ref pr, 1);
        });
    }

    [Fact]
    public void LowCardinalityColumnSkipper_DictSizeOverflows_Throws()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);
        writer.WriteUInt64(1);             // version
        writer.WriteUInt64(0);             // flags (indexType = UInt8)
        writer.WriteUInt64(OverflowUInt);  // dictSize — overflows
        var seq = Seq(buf);

        var skipper = ColumnSkipperRegistry.Default.GetSkipper("LowCardinality(String)");
        Assert.Throws<ClickHouseProtocolException>(() =>
        {
            var pr = new ProtocolReader(seq);
            skipper.TrySkipColumn(ref pr, 1);
        });
    }
}
