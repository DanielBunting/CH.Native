using System.Buffers;
using System.Text;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnSkippers;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnSkippers;

public class StringColumnSkipperTests
{
    [Fact]
    public void Skip_VariableLengthStrings_AdvancesPastAll()
    {
        var values = new string[100];
        for (int i = 0; i < values.Length; i++)
            values[i] = new string('x', i % 17); // varying lengths 0-16

        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) => new StringColumnWriter().WriteColumn(ref w, values));

        var reader = new ProtocolReader(seq);
        Assert.True(new StringColumnSkipper().TrySkipColumn(ref reader, values.Length));
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void Skip_EmptyStrings_HandledCorrectly()
    {
        var values = new string[10];
        for (int i = 0; i < values.Length; i++) values[i] = string.Empty;
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) => new StringColumnWriter().WriteColumn(ref w, values));

        Assert.Equal(values.Length, seq.Length); // each empty string is one varint(0) byte

        var reader = new ProtocolReader(seq);
        Assert.True(new StringColumnSkipper().TrySkipColumn(ref reader, values.Length));
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void Skip_TruncatedVarIntPrefix_ReturnsFalse()
    {
        // 9 strings of length 200 — varint prefix is 2 bytes — then truncate inside the
        // 10th string's prefix.
        var values = new string[10];
        for (int i = 0; i < 10; i++) values[i] = new string('a', 200);
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) => new StringColumnWriter().WriteColumn(ref w, values));

        // Truncate to all of rows 0-8 (9 strings) plus 1 byte of row 9's varint.
        const int perRow = 2 + 200;
        var truncated = SkipperTestBase.Truncate(seq, 9 * perRow + 1);

        var reader = new ProtocolReader(truncated);
        Assert.False(new StringColumnSkipper().TrySkipColumn(ref reader, values.Length));
    }

    [Fact]
    public void Skip_StringPayloadTruncated_ReturnsFalse()
    {
        // Hand-craft: varint length=50, but supply only 30 bytes of payload.
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) =>
        {
            w.WriteVarInt(50);
            w.WriteBytes(new byte[30]);
        });

        var reader = new ProtocolReader(seq);
        Assert.False(new StringColumnSkipper().TrySkipColumn(ref reader, rowCount: 1));
    }

    [Fact]
    public void Skip_FragmentedAcrossSequenceSegments_IdenticalResult()
    {
        var values = new[] { "alpha", "beta", "gamma", "delta", "epsilon" };
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) => new StringColumnWriter().WriteColumn(ref w, values));
        var totalLen = (int)seq.Length;

        // Carve into uneven segments to land splits inside payload bytes and varint prefixes.
        var sizes = new[] { 1, 3, 5, 7, totalLen - 16 };
        var fragmented = SkipperTestBase.Fragment(seq, sizes);

        var reader = new ProtocolReader(fragmented);
        Assert.True(new StringColumnSkipper().TrySkipColumn(ref reader, values.Length));
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void ReaderVsSkipper_String_ConsumeSameBytes()
    {
        var values = new[] { "", "x", new string('y', 130), "hello", new string('z', 10000) };
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) => new StringColumnWriter().WriteColumn(ref w, values));

        SkipperTestBase.AssertParity(
            seq, values.Length,
            (ref ProtocolReader r) => { },
            (ref ProtocolReader r, int rc) =>
            {
                using var col = new StringColumnReader().ReadTypedColumn(ref r, rc);
            },
            (ref ProtocolReader r, int rc) => new StringColumnSkipper().TrySkipColumn(ref r, rc));
    }

    [Fact]
    public void Skip_FixedString_HappyPath_AdvancesExactly()
    {
        const int len = 12;
        const int rowCount = 64;
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) => w.WriteBytes(new byte[len * rowCount]));

        var reader = new ProtocolReader(seq);
        Assert.True(new FixedStringColumnSkipper(len).TrySkipColumn(ref reader, rowCount));
        Assert.Equal(0, reader.Remaining);
    }
}
