using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnSkippers;
using CH.Native.Data.ColumnWriters;
using CH.Native.Data.Variant;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnSkippers;

public class VariantColumnSkipperTests
{
    private static IColumnSkipper[] StringIntFloatSkippers() => new IColumnSkipper[]
    {
        new StringColumnSkipper(),
        new Int32ColumnSkipper(),
        new Float64ColumnSkipper(),
    };

    private static IColumnWriter[] StringIntFloatWriters() => new IColumnWriter[]
    {
        new StringColumnWriter(),
        new Int32ColumnWriter(),
        new Float64ColumnWriter(),
    };

    [Fact]
    public void Skip_HappyPath_AllArmsCounted()
    {
        var values = new ClickHouseVariant[]
        {
            ClickHouseVariant.Of(0, "alpha"),
            ClickHouseVariant.Of(1, 42),
            ClickHouseVariant.Of(2, 3.14),
            ClickHouseVariant.Of(0, "beta"),
            ClickHouseVariant.Of(1, -7),
            ClickHouseVariant.Of(0, "gamma"),
        };

        var writer = new VariantColumnWriter(StringIntFloatWriters());
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) =>
        {
            writer.WritePrefix(ref w);
            writer.WriteColumn(ref w, values);
        });

        var skipper = new VariantColumnSkipper(StringIntFloatSkippers(), new[] { "String", "Int32", "Float64" });
        var reader = new ProtocolReader(seq);
        Assert.True(skipper.TrySkipColumn(ref reader, values.Length));
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void Skip_NullDiscriminator_SkipsNoData()
    {
        // All-null rows: discriminators all == 255, no per-row data. Wire = version + 4 disc bytes.
        var values = new[]
        {
            ClickHouseVariant.Null, ClickHouseVariant.Null, ClickHouseVariant.Null, ClickHouseVariant.Null,
        };

        var writer = new VariantColumnWriter(StringIntFloatWriters());
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) =>
        {
            writer.WritePrefix(ref w);
            writer.WriteColumn(ref w, values);
        });

        // 8 (version UInt64) + 4 (discriminators) = 12 bytes only. No arm data.
        Assert.Equal(8 + values.Length, seq.Length);

        var skipper = new VariantColumnSkipper(StringIntFloatSkippers(), new[] { "String", "Int32", "Float64" });
        var reader = new ProtocolReader(seq);
        Assert.True(skipper.TrySkipColumn(ref reader, values.Length));
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void Skip_InvalidDiscriminator_ReturnsFalse()
    {
        // Hand-craft: version + one discriminator = 99 (out of range for 3-arm Variant).
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) =>
        {
            w.WriteUInt64(0); // discriminator version
            w.WriteByte(99);  // invalid (only arms 0/1/2 + 255 null are valid)
        });

        var skipper = new VariantColumnSkipper(StringIntFloatSkippers(), new[] { "String", "Int32", "Float64" });
        var reader = new ProtocolReader(seq);
        Assert.False(skipper.TrySkipColumn(ref reader, rowCount: 1));
    }

    [Fact]
    public void Skip_TruncatedDiscriminators_ReturnsFalse_AndReturnsArrayPoolBuffer()
    {
        // Pool accounting: even on early-return failure, the rented buffer should be returned.
        var pool = new TestArrayPool<byte>();
        var skipper = new VariantColumnSkipper(StringIntFloatSkippers(), new[] { "String", "Int32", "Float64" }, pool);

        // Version byte present, but only 2 of 5 discriminators supplied.
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) =>
        {
            w.WriteUInt64(0);
            w.WriteByte(0);
            w.WriteByte(1);
        });

        var reader = new ProtocolReader(seq);
        Assert.False(skipper.TrySkipColumn(ref reader, rowCount: 5));
        Assert.Equal(pool.Rents, pool.Returns);
        Assert.True(pool.Rents > 0);
    }

    [Fact]
    public void Skip_HappyPath_PoolBalanced()
    {
        var pool = new TestArrayPool<byte>();
        var values = new ClickHouseVariant[]
        {
            ClickHouseVariant.Of(0, "a"),
            ClickHouseVariant.Of(1, 1),
            ClickHouseVariant.Of(2, 1.0),
        };

        var writer = new VariantColumnWriter(StringIntFloatWriters());
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) =>
        {
            writer.WritePrefix(ref w);
            writer.WriteColumn(ref w, values);
        });

        var skipper = new VariantColumnSkipper(StringIntFloatSkippers(), new[] { "String", "Int32", "Float64" }, pool);
        var reader = new ProtocolReader(seq);
        Assert.True(skipper.TrySkipColumn(ref reader, values.Length));
        Assert.Equal(pool.Rents, pool.Returns);
        Assert.True(pool.Rents > 0);
    }

    [Fact]
    public void ReaderVsSkipper_Variant_ConsumeSameBytes()
    {
        var values = new ClickHouseVariant[]
        {
            ClickHouseVariant.Of(0, "x"),
            ClickHouseVariant.Of(1, 7),
            ClickHouseVariant.Null,
            ClickHouseVariant.Of(2, 2.5),
        };

        var writer = new VariantColumnWriter(StringIntFloatWriters());
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) =>
        {
            writer.WritePrefix(ref w);
            writer.WriteColumn(ref w, values);
        });

        var reader = new VariantColumnReader(new IColumnReader[]
        {
            new StringColumnReader(), new Int32ColumnReader(), new Float64ColumnReader(),
        });

        SkipperTestBase.AssertParity(
            seq, values.Length,
            (ref ProtocolReader r) => reader.ReadPrefix(ref r),
            (ref ProtocolReader r, int rc) =>
            {
                using var col = reader.ReadTypedColumn(ref r, rc);
            },
            (ref ProtocolReader r, int rc) => new VariantColumnSkipper(
                    StringIntFloatSkippers(), new[] { "String", "Int32", "Float64" })
                .TrySkipColumn(ref r, rc));
    }
}
