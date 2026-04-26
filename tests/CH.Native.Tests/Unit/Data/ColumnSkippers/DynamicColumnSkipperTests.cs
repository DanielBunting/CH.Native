using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnSkippers;
using CH.Native.Data.ColumnWriters;
using CH.Native.Data.Dynamic;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnSkippers;

public class DynamicColumnSkipperTests
{
    private static ColumnSkipperFactory SkipperFactory() => new(ColumnSkipperRegistry.Default);
    private static ColumnWriterFactory WriterFactory() => new(ColumnWriterRegistry.Default);
    private static ColumnReaderFactory ReaderFactory() => new(ColumnReaderRegistry.Default);

    [Fact]
    public void Skip_TwoArmDynamic_DistributesRowsCorrectly()
    {
        var values = new ClickHouseDynamic[]
        {
            new(0, "alpha", "String"),
            new(1, 42, "Int32"),
            new(0, "beta", "String"),
            new(1, -7, "Int32"),
            new(0, "gamma", "String"),
        };

        var writer = new DynamicColumnWriter(WriterFactory());
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) =>
        {
            writer.WritePrefix(ref w);
            writer.WriteColumn(ref w, values);
        });

        var skipper = new DynamicColumnSkipper(SkipperFactory(), "Dynamic");
        var reader = new ProtocolReader(seq);
        Assert.True(skipper.TrySkipColumn(ref reader, values.Length));
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void Skip_DiscriminatorWidth_TwoByte_HandledCorrectly()
    {
        // Build wire by hand: numberOfTypes = 256 → totalIndexValues = 257 → 2-byte discriminators.
        // Use FixedString(1)..FixedString(256) for 256 distinct, parseable type names.
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) =>
        {
            w.WriteUInt64(3); // structure version FLATTENED
            w.WriteVarInt(256); // number of types
            for (int i = 1; i <= 256; i++) w.WriteString($"FixedString({i})");
            // 1 row, discriminator = 0 (UInt16 little-endian)
            w.WriteUInt16(0);
            // Arm 0 is FixedString(1) → 1 byte for the single row
            w.WriteByte(0xAB);
            // Arms 1..255 get rowCount=0 → no bytes
        });

        var skipper = new DynamicColumnSkipper(SkipperFactory(), "Dynamic");
        var reader = new ProtocolReader(seq);
        Assert.True(skipper.TrySkipColumn(ref reader, rowCount: 1));
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void Skip_DiscriminatorWidth_FourByte_HandledCorrectly()
    {
        // numberOfTypes = 65536 → totalIndexValues = 65537 → forces 4-byte discriminators.
        // Use FixedString(1)..FixedString(65536) for distinct, parseable type names.
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) =>
        {
            w.WriteUInt64(3); // structure version FLATTENED
            w.WriteVarInt(65536); // number of types
            for (int i = 1; i <= 65536; i++) w.WriteString($"FixedString({i})");
            // 1 row, discriminator = 0 (UInt32 little-endian)
            w.WriteUInt32(0);
            // Arm 0 is FixedString(1) → 1 byte for the single row
            w.WriteByte(0xCD);
            // Arms 1..65535 get rowCount=0 → no bytes
        });

        var skipper = new DynamicColumnSkipper(SkipperFactory(), "Dynamic");
        var reader = new ProtocolReader(seq);
        Assert.True(skipper.TrySkipColumn(ref reader, rowCount: 1));
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void Skip_DiscriminatorWidth_FourByte_TruncatedTail_ReturnsFalse()
    {
        // Same 4-byte path, but truncate the trailing arm-0 byte.
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) =>
        {
            w.WriteUInt64(3);
            w.WriteVarInt(65536);
            for (int i = 1; i <= 65536; i++) w.WriteString($"FixedString({i})");
            w.WriteUInt32(0);
            // Omit the arm-0 byte → truncated
        });

        var skipper = new DynamicColumnSkipper(SkipperFactory(), "Dynamic");
        var reader = new ProtocolReader(seq);
        Assert.False(skipper.TrySkipColumn(ref reader, rowCount: 1));
    }

    [Fact]
    public void Skip_DiscriminatorOutOfRange_ReturnsFalse()
    {
        // 2-arm Dynamic, totalIndexValues = 3. Discriminator 5 is out of range → returns false.
        // (Spec called for "Throws", actual code returns false.)
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) =>
        {
            w.WriteUInt64(3);
            w.WriteVarInt(2);
            w.WriteString("Int32");
            w.WriteString("String");
            w.WriteByte(5); // out of range
        });

        var skipper = new DynamicColumnSkipper(SkipperFactory(), "Dynamic");
        var reader = new ProtocolReader(seq);
        Assert.False(skipper.TrySkipColumn(ref reader, rowCount: 1));
    }

    [Fact]
    public void Skip_TruncatedDiscriminatorArray_ReturnsFalse()
    {
        // 2-arm Dynamic, claim rowCount=10, supply only 9 discriminators.
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) =>
        {
            w.WriteUInt64(3);
            w.WriteVarInt(2);
            w.WriteString("Int32");
            w.WriteString("String");
            for (int i = 0; i < 9; i++) w.WriteByte(0);
        });

        var skipper = new DynamicColumnSkipper(SkipperFactory(), "Dynamic");
        var reader = new ProtocolReader(seq);
        Assert.False(skipper.TrySkipColumn(ref reader, rowCount: 10));
    }

    [Fact]
    public void Skip_UnsupportedStructureVersion_ReturnsFalse()
    {
        // Pin behaviour: structure version != 3 returns false (does not throw).
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) => w.WriteUInt64(2));

        var skipper = new DynamicColumnSkipper(SkipperFactory(), "Dynamic");
        var reader = new ProtocolReader(seq);
        Assert.False(skipper.TrySkipColumn(ref reader, rowCount: 1));
    }

    [Fact]
    public void Skip_FactoryFailsForUnknownArmType_ReturnsFalse()
    {
        // Type name "ThisDoesNotExist" — factory throws inside the skipper, caught and false returned.
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) =>
        {
            w.WriteUInt64(3);
            w.WriteVarInt(1);
            w.WriteString("ThisDoesNotExist");
            w.WriteByte(0);
        });

        var skipper = new DynamicColumnSkipper(SkipperFactory(), "Dynamic");
        var reader = new ProtocolReader(seq);
        Assert.False(skipper.TrySkipColumn(ref reader, rowCount: 1));
    }

    [Fact]
    public void ReaderVsSkipper_Dynamic_ConsumeSameBytes()
    {
        var values = new ClickHouseDynamic[]
        {
            new(0, "x", "String"),
            new(1, 7, "Int32"),
            ClickHouseDynamic.Null,
            new(0, "y", "String"),
        };

        var writer = new DynamicColumnWriter(WriterFactory());
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) =>
        {
            writer.WritePrefix(ref w);
            writer.WriteColumn(ref w, values);
        });

        var dynReader = new DynamicColumnReader(ReaderFactory());

        SkipperTestBase.AssertParity(
            seq, values.Length,
            (ref ProtocolReader r) => dynReader.ReadPrefix(ref r),
            (ref ProtocolReader r, int rc) =>
            {
                using var col = dynReader.ReadTypedColumn(ref r, rc);
            },
            (ref ProtocolReader r, int rc) =>
                new DynamicColumnSkipper(SkipperFactory(), "Dynamic").TrySkipColumn(ref r, rc));
    }
}
