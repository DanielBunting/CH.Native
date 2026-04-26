using System.Buffers;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnSkippers;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnSkippers;

/// <summary>
/// Sanity checks that ArrayColumnSkipper consumes the same number of bytes as the
/// reader. If the reader ever truncates offsets via a cast, the skipper would advance
/// a different amount, desyncing subsequent columns.
/// </summary>
public class ArrayColumnSkipperTests
{
    [Fact]
    public void ReaderVsSkipper_ConsumeSameBytes()
    {
        var rows = new[]
        {
            new[] { 1, 2, 3 },
            Array.Empty<int>(),
            new[] { 4, 5 },
        };

        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);
        new ArrayColumnWriter<int>(new Int32ColumnWriter()).WriteColumn(ref writer, rows);
        var seq = new System.Buffers.ReadOnlySequence<byte>(buf.WrittenMemory);
        var totalBytes = (int)seq.Length;

        // Skipper path
        var readerS = new ProtocolReader(seq);
        var skipper = new ArrayColumnSkipper(new Int32ColumnSkipper(), "Int32");
        Assert.True(skipper.TrySkipColumn(ref readerS, rows.Length));
        Assert.Equal(0, readerS.Remaining);
        var skipperConsumed = totalBytes;

        // Reader path
        var readerR = new ProtocolReader(seq);
        using var col = new ArrayColumnReader<int>(new Int32ColumnReader()).ReadTypedColumn(ref readerR, rows.Length);
        var readerConsumed = totalBytes - (int)readerR.Remaining;

        Assert.Equal(skipperConsumed, readerConsumed);
    }

    [Fact]
    public void TrySkip_PreservesStreamAfterColumn()
    {
        var rows = new[] { new[] { 10, 20 }, new[] { 30 } };

        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);
        new ArrayColumnWriter<int>(new Int32ColumnWriter()).WriteColumn(ref writer, rows);
        writer.WriteUInt32(0xDEADBEEF);

        var reader = new ProtocolReader(new System.Buffers.ReadOnlySequence<byte>(buf.WrittenMemory));
        Assert.True(new ArrayColumnSkipper(new Int32ColumnSkipper(), "Int32")
            .TrySkipColumn(ref reader, rows.Length));
        Assert.Equal(0xDEADBEEFu, reader.ReadUInt32());
    }
}
