using System.Buffers;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnSkippers;
using CH.Native.Data.ColumnWriters;
using CH.Native.Exceptions;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnSkippers;

public class MapColumnSkipperTests
{
    private static MapColumnSkipper StringInt32Skipper() =>
        new(new StringColumnSkipper(), new Int32ColumnSkipper(), "String", "Int32");

    [Fact]
    public void Skip_NonEmptyMap_OffsetsKeysValues()
    {
        var values = new[]
        {
            new Dictionary<string, int> { ["a"] = 1, ["b"] = 2 },
            new Dictionary<string, int> { ["x"] = 9 },
            new Dictionary<string, int>(),
            new Dictionary<string, int> { ["c"] = 3, ["d"] = 4, ["e"] = 5 },
            new Dictionary<string, int> { ["z"] = 7 },
        };

        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) =>
            new MapColumnWriter<string, int>(new StringColumnWriter(), new Int32ColumnWriter())
                .WriteColumn(ref w, values));

        var reader = new ProtocolReader(seq);
        Assert.True(StringInt32Skipper().TrySkipColumn(ref reader, values.Length));
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void Skip_AllEmptyRows_OffsetsZeroNoNested()
    {
        const int rowCount = 5;
        var values = new Dictionary<string, int>[rowCount];
        for (int i = 0; i < rowCount; i++) values[i] = new Dictionary<string, int>();

        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) =>
            new MapColumnWriter<string, int>(new StringColumnWriter(), new Int32ColumnWriter())
                .WriteColumn(ref w, values));

        // Wire is exactly rowCount * 8 bytes of zero offsets, no nested keys/values.
        Assert.Equal(rowCount * 8, seq.Length);

        var reader = new ProtocolReader(seq);
        Assert.True(StringInt32Skipper().TrySkipColumn(ref reader, rowCount));
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void Skip_LastOffsetExceedsInt32_ThrowsProtocolException()
    {
        // Hand-craft 5 offsets, the last being ulong.MaxValue. Skipper reads the last
        // offset as the total entry count; ProtocolGuards.ToInt32 throws.
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) =>
        {
            w.WriteUInt64(0);
            w.WriteUInt64(0);
            w.WriteUInt64(0);
            w.WriteUInt64(0);
            w.WriteUInt64(ulong.MaxValue);
        });

        ClickHouseProtocolException? caught = null;
        try
        {
            var reader = new ProtocolReader(seq);
            StringInt32Skipper().TrySkipColumn(ref reader, rowCount: 5);
        }
        catch (ClickHouseProtocolException ex) { caught = ex; }
        Assert.NotNull(caught);
    }

    [Fact]
    public void Skip_TruncatedOffsetArray_ReturnsFalse()
    {
        // Write 5 valid offsets then truncate to only 3 offsets' worth.
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) =>
        {
            for (ulong i = 1; i <= 5; i++) w.WriteUInt64(i);
        });
        var truncated = SkipperTestBase.Truncate(seq, 3 * 8);

        var reader = new ProtocolReader(truncated);
        Assert.False(StringInt32Skipper().TrySkipColumn(ref reader, rowCount: 5));
    }

    [Fact]
    public void Skip_MapNestedInArray_OffsetAlignment()
    {
        // Array(Map(String, Int32)) — outer Array offsets, inner Map offsets, then keys+values.
        var values = new[]
        {
            new[]
            {
                new Dictionary<string, int> { ["k1"] = 1 },
                new Dictionary<string, int> { ["k2"] = 2, ["k3"] = 3 },
            },
            new Dictionary<string, int>[0],
            new[] { new Dictionary<string, int> { ["x"] = 9 } },
        };

        var inner = new MapColumnWriter<string, int>(new StringColumnWriter(), new Int32ColumnWriter());
        var outer = new ArrayColumnWriter<Dictionary<string, int>>(inner);
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) => outer.WriteColumn(ref w, values));

        SkipperTestBase.AssertParity(
            seq, values.Length,
            (ref ProtocolReader r) => { },
            (ref ProtocolReader r, int rc) =>
            {
                using var col = new ArrayColumnReader<Dictionary<string, int>>(
                    new MapColumnReader<string, int>(new StringColumnReader(), new Int32ColumnReader()))
                    .ReadTypedColumn(ref r, rc);
            },
            (ref ProtocolReader r, int rc) => new ArrayColumnSkipper(StringInt32Skipper(), "Map(String, Int32)")
                .TrySkipColumn(ref r, rc));
    }
}
