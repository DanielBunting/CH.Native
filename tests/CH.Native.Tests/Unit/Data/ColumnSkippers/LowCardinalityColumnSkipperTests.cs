using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnSkippers;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnSkippers;

/// <summary>
/// Tests covering the contract between the LowCardinality reader and skipper paths.
/// </summary>
/// <remarks>
/// The reader uses the caller-supplied <c>rowCount</c> to decode indices, while the
/// skipper reads an <c>indexCount</c> field from the wire and uses that to advance.
/// The two diverge when <c>indexCount != rowCount</c>. These tests document the
/// current behavior and will catch regressions if it changes.
/// </remarks>
public class LowCardinalityColumnSkipperTests
{
    private static ReadOnlySequence<byte> EncodeLowCardinality(string[] values)
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);
        var inner = new StringColumnWriter();
        var lc = new LowCardinalityColumnWriter<string>(inner, isNullable: false);

        // Both prefix (version) and column body. The skipper expects to see both.
        lc.WritePrefix(ref writer);
        lc.WriteColumn(ref writer, values);

        return new ReadOnlySequence<byte>(buf.WrittenMemory);
    }

    [Fact]
    public void TrySkip_MatchingRowCount_AdvancesToEnd()
    {
        var values = new[] { "a", "b", "a", "c", "b" };
        var seq = EncodeLowCardinality(values);

        var reader = new ProtocolReader(seq);
        var skipper = new LowCardinalityColumnSkipper(new StringColumnSkipper(), "String");

        Assert.True(skipper.TrySkipColumn(ref reader, values.Length));
        Assert.Equal(0, reader.Remaining);
    }

    // Local helper because ProtocolReader is a ref struct (cannot be captured in lambdas).
    private static InvalidDataException AssertSkipThrows(ReadOnlySequence<byte> seq, int rowCount)
    {
        try
        {
            var reader = new ProtocolReader(seq);
            var skipper = new LowCardinalityColumnSkipper(new StringColumnSkipper(), "String");
            skipper.TrySkipColumn(ref reader, rowCount);
        }
        catch (InvalidDataException ex)
        {
            return ex;
        }
        throw new Xunit.Sdk.XunitException("Expected InvalidDataException");
    }

    [Fact]
    public void TrySkip_IndexCountLessThanRowCount_ThrowsInvalidData()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);

        writer.WriteUInt64(1);
        ulong flags = 0UL | (1UL << 9) | (1UL << 10);
        writer.WriteUInt64(flags);
        writer.WriteUInt64(2);
        var inner = new StringColumnWriter();
        inner.WriteValue(ref writer, "a");
        inner.WriteValue(ref writer, "b");
        writer.WriteUInt64(3); // LIE — wire claims 3 indices
        for (int i = 0; i < 5; i++) writer.WriteByte((byte)(i % 2));

        var ex = AssertSkipThrows(new ReadOnlySequence<byte>(buf.WrittenMemory), rowCount: 5);
        Assert.Contains("3", ex.Message);
        Assert.Contains("5", ex.Message);
    }

    [Fact]
    public void TrySkip_IndexCountGreaterThanRowCount_ThrowsInvalidData()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);

        writer.WriteUInt64(1);
        ulong flags = 0UL | (1UL << 9) | (1UL << 10);
        writer.WriteUInt64(flags);
        writer.WriteUInt64(1);
        var inner = new StringColumnWriter();
        inner.WriteValue(ref writer, "only");
        writer.WriteUInt64(7);
        for (int i = 0; i < 7; i++) writer.WriteByte(0);

        var ex = AssertSkipThrows(new ReadOnlySequence<byte>(buf.WrittenMemory), rowCount: 3);
        Assert.Contains("7", ex.Message);
        Assert.Contains("3", ex.Message);
    }

    [Fact]
    public void TrySkip_ZeroRowCount_ConsumesNothing()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);
        writer.WriteUInt32(0x11223344);

        var seq = new ReadOnlySequence<byte>(buf.WrittenMemory);
        var reader = new ProtocolReader(seq);
        var skipper = new LowCardinalityColumnSkipper(new StringColumnSkipper(), "String");

        Assert.True(skipper.TrySkipColumn(ref reader, rowCount: 0));
        Assert.Equal(0x11223344u, reader.ReadUInt32());
    }

    [Fact]
    public void ReaderVsSkipper_WhenWellFormed_ConsumeSameBytes()
    {
        var values = new[] { "x", "y", "x", "z" };
        var seq = EncodeLowCardinality(values);
        var totalBytes = (int)seq.Length;

        // Reader path
        var readerR = new ProtocolReader(seq);
        var lcReader = new LowCardinalityColumnReader<string>(new StringColumnReader(), isNullable: false);
        lcReader.ReadPrefix(ref readerR);
        using var column = lcReader.ReadTypedColumn(ref readerR, values.Length);
        var readerConsumed = totalBytes - (int)readerR.Remaining;

        // Skipper path
        var readerS = new ProtocolReader(seq);
        var skipper = new LowCardinalityColumnSkipper(new StringColumnSkipper(), "String");
        Assert.True(skipper.TrySkipColumn(ref readerS, values.Length));
        var skipperConsumed = totalBytes - (int)readerS.Remaining;

        Assert.Equal(readerConsumed, skipperConsumed);
    }
}
