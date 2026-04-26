using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnSkippers;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnSkippers;

public class TupleColumnSkipperTests
{
    [Fact]
    public void Skip_HomogeneousTuple_AllElementsAdvance()
    {
        // Tuple(Int32, Int32, Int32) — columnar layout: rowCount * 4 bytes per element = rowCount * 12.
        const int rowCount = 100;
        var rows = new object[rowCount][];
        for (int i = 0; i < rowCount; i++)
            rows[i] = new object[] { i, i * 2, i * 3 };

        var writer = new TupleColumnWriter(new IColumnWriter[]
        {
            new Int32ColumnWriter(),
            new Int32ColumnWriter(),
            new Int32ColumnWriter(),
        });
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) => writer.WriteColumn(ref w, rows));

        Assert.Equal(rowCount * 12, seq.Length);

        var skipper = new TupleColumnSkipper(
            new IColumnSkipper[] { new Int32ColumnSkipper(), new Int32ColumnSkipper(), new Int32ColumnSkipper() },
            new[] { "Int32", "Int32", "Int32" });

        var reader = new ProtocolReader(seq);
        Assert.True(skipper.TrySkipColumn(ref reader, rowCount));
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void Skip_HeterogeneousTuple_VarLength_MatchesReader()
    {
        // Tuple(String, Nullable(Int32)) — String + bitmap+values. (Skipping Array(String)
        // here because TupleColumnWriter.WriteColumn for nested arrays takes the
        // object[][] codepath, which doesn't compose cleanly with our skipper assertions.)
        var rows = new object[][]
        {
            new object[] { "alpha", (int?)1 },
            new object[] { "", (int?)null },
            new object[] { "longer string", (int?)42 },
            new object[] { "x", (int?)null },
        };

        var writer = new TupleColumnWriter(new IColumnWriter[]
        {
            new StringColumnWriter(),
            new NullableColumnWriter<int>(new Int32ColumnWriter()),
        });
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) => writer.WriteColumn(ref w, rows));

        var skipper = new TupleColumnSkipper(
            new IColumnSkipper[]
            {
                new StringColumnSkipper(),
                new NullableColumnSkipper(new Int32ColumnSkipper(), "Int32"),
            },
            new[] { "String", "Nullable(Int32)" });

        var reader = new ProtocolReader(seq);
        Assert.True(skipper.TrySkipColumn(ref reader, rows.Length));
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void Skip_NestedSkipperFails_PropagatesFalse()
    {
        // Truncate inside the second element's bytes. First element succeeds (best-effort
        // cursor advances past it), second returns false.
        const int rowCount = 8;
        var rows = new object[rowCount][];
        for (int i = 0; i < rowCount; i++) rows[i] = new object[] { i, i + 100 };

        var writer = new TupleColumnWriter(new IColumnWriter[]
        {
            new Int32ColumnWriter(),
            new Int32ColumnWriter(),
        });
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) => writer.WriteColumn(ref w, rows));
        // First element occupies rowCount*4 = 32 bytes; truncate at 32 + 5 (mid second element).
        var truncated = SkipperTestBase.Truncate(seq, rowCount * 4 + 5);

        var skipper = new TupleColumnSkipper(
            new IColumnSkipper[] { new Int32ColumnSkipper(), new Int32ColumnSkipper() },
            new[] { "Int32", "Int32" });

        var reader = new ProtocolReader(truncated);
        Assert.False(skipper.TrySkipColumn(ref reader, rowCount));
    }

    [Fact]
    public void Skip_NestedSkipperThrows_PropagatesException()
    {
        // First element is a Map; provide a malformed last-offset so ProtocolGuards.ToInt32
        // throws. The exception bubbles out of the Tuple skipper without being swallowed.
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) =>
        {
            // 5 map offsets, last is ulong.MaxValue
            for (int i = 0; i < 4; i++) w.WriteUInt64(0);
            w.WriteUInt64(ulong.MaxValue);
        });

        var skipper = new TupleColumnSkipper(
            new IColumnSkipper[]
            {
                new MapColumnSkipper(new StringColumnSkipper(), new Int32ColumnSkipper(), "String", "Int32"),
                new Int32ColumnSkipper(),
            },
            new[] { "Map(String, Int32)", "Int32" });

        Exceptions.ClickHouseProtocolException? caught = null;
        try
        {
            var reader = new ProtocolReader(seq);
            skipper.TrySkipColumn(ref reader, rowCount: 5);
        }
        catch (Exceptions.ClickHouseProtocolException ex) { caught = ex; }
        Assert.NotNull(caught);
    }

    [Fact]
    public void ReaderVsSkipper_HomogeneousTuple_ConsumeSameBytes()
    {
        var rows = new object[][]
        {
            new object[] { 1, 2, 3 },
            new object[] { 4, 5, 6 },
        };

        var writer = new TupleColumnWriter(new IColumnWriter[]
        {
            new Int32ColumnWriter(), new Int32ColumnWriter(), new Int32ColumnWriter(),
        });
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) => writer.WriteColumn(ref w, rows));

        SkipperTestBase.AssertParity(
            seq, rows.Length,
            (ref ProtocolReader r) => { },
            (ref ProtocolReader r, int rc) =>
            {
                using var col = new TupleColumnReader(new IColumnReader[]
                {
                    new Int32ColumnReader(), new Int32ColumnReader(), new Int32ColumnReader(),
                }).ReadTypedColumn(ref r, rc);
            },
            (ref ProtocolReader r, int rc) => new TupleColumnSkipper(
                    new IColumnSkipper[] { new Int32ColumnSkipper(), new Int32ColumnSkipper(), new Int32ColumnSkipper() },
                    new[] { "Int32", "Int32", "Int32" })
                .TrySkipColumn(ref r, rc));
    }
}
