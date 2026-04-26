using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnSkippers;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnSkippers;

/// <summary>
/// Nested has the same wire format as Tuple of Arrays (matches what
/// <c>ColumnSkipperFactory.CreateNestedSkipper</c> emits at runtime).
/// </summary>
public class NestedColumnSkipperTests
{
    [Fact]
    public void Skip_HomogeneousNested_AllFieldsAdvance()
    {
        // Nested(ids Array(Int32), names Array(String)) — emit via Tuple(Array, Array).
        var rows = new object[][]
        {
            new object[] { new[] { 1, 2, 3 }, new[] { "a", "b" } },
            new object[] { Array.Empty<int>(), Array.Empty<string>() },
            new object[] { new[] { 9 }, new[] { "z" } },
        };

        var writer = new TupleColumnWriter(new IColumnWriter[]
        {
            new ArrayColumnWriter<int>(new Int32ColumnWriter()),
            new ArrayColumnWriter<string>(new StringColumnWriter()),
        });
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) => writer.WriteColumn(ref w, rows));

        var skipper = new NestedColumnSkipper(
            new IColumnSkipper[]
            {
                new ArrayColumnSkipper(new Int32ColumnSkipper(), "Int32"),
                new ArrayColumnSkipper(new StringColumnSkipper(), "String"),
            },
            "Nested(ids Int32, names String)");

        var reader = new ProtocolReader(seq);
        Assert.True(skipper.TrySkipColumn(ref reader, rows.Length));
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void Skip_TruncatedSecondField_ReturnsFalse()
    {
        var rows = new object[][]
        {
            new object[] { new[] { 1 }, new[] { "a" } },
            new object[] { new[] { 2 }, new[] { "b" } },
        };

        var writer = new TupleColumnWriter(new IColumnWriter[]
        {
            new ArrayColumnWriter<int>(new Int32ColumnWriter()),
            new ArrayColumnWriter<string>(new StringColumnWriter()),
        });
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) => writer.WriteColumn(ref w, rows));
        var truncated = SkipperTestBase.Truncate(seq, (int)seq.Length - 1);

        var skipper = new NestedColumnSkipper(
            new IColumnSkipper[]
            {
                new ArrayColumnSkipper(new Int32ColumnSkipper(), "Int32"),
                new ArrayColumnSkipper(new StringColumnSkipper(), "String"),
            },
            "Nested(ids Int32, names String)");

        var reader = new ProtocolReader(truncated);
        Assert.False(skipper.TrySkipColumn(ref reader, rows.Length));
    }
}
