using CH.Native.Data;
using CH.Native.Data.ColumnSkippers;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnSkippers;

/// <summary>
/// A Nested column is parallel arrays sharing ONE offsets block (sent once), then each
/// field's flat values. The skipper reads the offsets once and skips each field's flat
/// elements — it must consume exactly what <see cref="NestedColumnWriter"/> emits.
/// </summary>
public class NestedColumnSkipperTests
{
    private static NestedColumnWriter Writer() => new(
        new IColumnWriter[] { new Int32ColumnWriter(), new StringColumnWriter() },
        new[] { "ids", "names" });

    private static NestedColumnSkipper Skipper() => new(
        new IColumnSkipper[] { new Int32ColumnSkipper(), new StringColumnSkipper() },
        "Nested(ids Int32, names String)");

    [Fact]
    public void Skip_HomogeneousNested_ConsumesExactlyWhatWriterEmitted()
    {
        var rows = new[]
        {
            new object[] { new[] { 1, 2, 3 }, new[] { "a", "b", "c" } },
            new object[] { System.Array.Empty<int>(), System.Array.Empty<string>() },
            new object[] { new[] { 9 }, new[] { "z" } },
        };

        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) => Writer().WriteColumn(ref w, rows));

        var reader = new ProtocolReader(seq);
        Assert.True(Skipper().TrySkipColumn(ref reader, rows.Length));
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void Skip_TruncatedData_ReturnsFalse()
    {
        var rows = new[]
        {
            new object[] { new[] { 1 }, new[] { "a" } },
            new object[] { new[] { 2 }, new[] { "b" } },
        };

        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) => Writer().WriteColumn(ref w, rows));
        var truncated = SkipperTestBase.Truncate(seq, (int)seq.Length - 1);

        var reader = new ProtocolReader(truncated);
        Assert.False(Skipper().TrySkipColumn(ref reader, rows.Length));
    }
}
