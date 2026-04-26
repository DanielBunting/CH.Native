using System.Buffers;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnSkippers;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnSkippers;

public class NullableColumnSkipperTests
{
    [Fact]
    public void Skip_NullableInt32_BitmapPlusValues()
    {
        // 10 rows, mix of nulls and values. Wire: 10 bitmap bytes + 10 * 4 value bytes = 50.
        var values = new int?[] { 1, null, 3, null, null, 6, 7, 8, null, 10 };
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) =>
            new NullableColumnWriter<int>(new Int32ColumnWriter()).WriteColumn(ref w, values));

        Assert.Equal(values.Length + values.Length * 4, seq.Length);

        var reader = new ProtocolReader(seq);
        var skipper = new NullableColumnSkipper(new Int32ColumnSkipper(), "Int32");
        Assert.True(skipper.TrySkipColumn(ref reader, values.Length));
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void Skip_NullableString_VariableInner_AdvancesPastAll()
    {
        var values = new string?[] { "a", null, "longer", null, "" };

        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) =>
            new NullableRefColumnWriter<string>(new StringColumnWriter()).WriteColumn(ref w, values));

        var skipper = new NullableColumnSkipper(new StringColumnSkipper(), "String");

        var reader = new ProtocolReader(seq);
        Assert.True(skipper.TrySkipColumn(ref reader, values.Length));
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void Skip_NullableArrayString_ColumnarInner_RoundTrips()
    {
        // Regression test: NullableRefColumnWriter<T[]>(ArrayColumnWriter<T>) used to emit
        // per-row Array framing (length+elements per row) while the reader/skipper expect
        // columnar offsets. NullableRefColumnWriter.WriteColumn now delegates to the inner
        // writer's WriteColumn so the columnar layout is preserved.
        var values = new string[]?[]
        {
            new[] { "a", "b" },
            null,
            new[] { "longer string" },
            null,
            Array.Empty<string>(),
        };

        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) =>
            new NullableRefColumnWriter<string[]>(new ArrayColumnWriter<string>(new StringColumnWriter()))
                .WriteColumn(ref w, values));

        // Wire = bitmap(5) + Array(String) columnar = 5 + 5*8 (offsets) + 3 string-payloads.
        // Verify both reader and skipper consume the same bytes.
        SkipperTestBase.AssertParity(
            seq, values.Length,
            (ref ProtocolReader r) => { },
            (ref ProtocolReader r, int rc) =>
            {
                using var col = new NullableRefColumnReader<string[]>(
                    new ArrayColumnReader<string>(new StringColumnReader()))
                    .ReadTypedColumn(ref r, rc);
            },
            (ref ProtocolReader r, int rc) => new NullableColumnSkipper(
                    new ArrayColumnSkipper(new StringColumnSkipper(), "String"),
                    "Array(String)")
                .TrySkipColumn(ref r, rc));
    }

    [Fact]
    public void Skip_NullableMap_ColumnarInner_RoundTrips()
    {
        // Same fix benefits Nullable(Map(K,V)) — MapColumnWriter.WriteColumn emits
        // columnar offsets+keys+values that the reader/skipper now correctly see.
        var values = new Dictionary<string, int>?[]
        {
            new() { ["a"] = 1, ["b"] = 2 },
            null,
            new() { ["c"] = 3 },
        };

        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) =>
            new NullableRefColumnWriter<Dictionary<string, int>>(
                new MapColumnWriter<string, int>(new StringColumnWriter(), new Int32ColumnWriter()))
                .WriteColumn(ref w, values));

        var skipper = new NullableColumnSkipper(
            new MapColumnSkipper(new StringColumnSkipper(), new Int32ColumnSkipper(), "String", "Int32"),
            "Map(String, Int32)");

        var reader = new ProtocolReader(seq);
        Assert.True(skipper.TrySkipColumn(ref reader, values.Length));
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void Skip_TruncatedBitmap_ReturnsFalse()
    {
        var values = new int?[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) =>
            new NullableColumnWriter<int>(new Int32ColumnWriter()).WriteColumn(ref w, values));

        // Supply only 5 of 10 bitmap bytes — values won't even start.
        var truncated = SkipperTestBase.Truncate(seq, 5);

        var reader = new ProtocolReader(truncated);
        var positionBefore = reader.Consumed;
        Assert.False(new NullableColumnSkipper(new Int32ColumnSkipper(), "Int32")
            .TrySkipColumn(ref reader, values.Length));
        // Bitmap is the first read; failure happens before any advance.
        Assert.Equal(positionBefore, reader.Consumed);
    }

    [Fact]
    public void Skip_BitmapCompleteValuesPartial_ReturnsFalse()
    {
        var values = new int?[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) =>
            new NullableColumnWriter<int>(new Int32ColumnWriter()).WriteColumn(ref w, values));

        // Full bitmap (10 bytes) + half the values (20 of 40 bytes).
        var truncated = SkipperTestBase.Truncate(seq, 10 + 20);

        var reader = new ProtocolReader(truncated);
        Assert.False(new NullableColumnSkipper(new Int32ColumnSkipper(), "Int32")
            .TrySkipColumn(ref reader, values.Length));
    }

    [Fact]
    public void ReaderVsSkipper_NullableInt32_ConsumeSameBytes()
    {
        var values = new int?[] { 1, null, 3, 4, null };
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) =>
            new NullableColumnWriter<int>(new Int32ColumnWriter()).WriteColumn(ref w, values));

        SkipperTestBase.AssertParity(
            seq, values.Length,
            (ref ProtocolReader r) => { },
            (ref ProtocolReader r, int rc) =>
            {
                using var col = new NullableColumnReader<int>(new Int32ColumnReader())
                    .ReadTypedColumn(ref r, rc);
            },
            (ref ProtocolReader r, int rc) => new NullableColumnSkipper(new Int32ColumnSkipper(), "Int32")
                .TrySkipColumn(ref r, rc));
    }
}
