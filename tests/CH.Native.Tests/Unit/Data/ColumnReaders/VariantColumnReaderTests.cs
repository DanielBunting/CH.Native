using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Data.ColumnSkippers;
using CH.Native.Data.Variant;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

/// <summary>
/// Unit tests for VariantColumnReader covering single-arm, multi-arm, mixed, NULL,
/// and bad-discriminator cases. Also round-trip tests against VariantColumnWriter.
/// </summary>
public class VariantColumnReaderTests
{
    private static readonly ColumnReaderFactory ReaderFactory = new(ColumnReaderRegistry.Default);
    private static readonly ColumnWriterFactory WriterFactory = new(ColumnWriterRegistry.Default);
    private static readonly ColumnSkipperFactory SkipperFactory = new(ColumnSkipperRegistry.Default);

    [Fact]
    public void ReadTypedColumn_AllNull_ReturnsAllNullVariants()
    {
        var reader = (VariantColumnReader)ReaderFactory.CreateReader("Variant(Int64, String)");

        using var buffer = new PooledBufferWriter();
        var writer = new ProtocolWriter(buffer);
        writer.WriteUInt64(0); // version
        writer.WriteByte(255); // discriminator NULL
        writer.WriteByte(255);
        writer.WriteByte(255);
        // No per-arm data (counts are zero).

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        reader.ReadPrefix(ref pr);
        using var col = reader.ReadTypedColumn(ref pr, 3);

        Assert.Equal(3, col.Count);
        Assert.True(((ClickHouseVariant)col.GetValue(0)!).IsNull);
        Assert.True(((ClickHouseVariant)col.GetValue(1)!).IsNull);
        Assert.True(((ClickHouseVariant)col.GetValue(2)!).IsNull);
    }

    [Fact]
    public void ReadTypedColumn_AllArmZero_ReturnsInt64Values()
    {
        var reader = (VariantColumnReader)ReaderFactory.CreateReader("Variant(Int64, String)");

        using var buffer = new PooledBufferWriter();
        var writer = new ProtocolWriter(buffer);
        writer.WriteUInt64(0); // version
        writer.WriteByte(0);
        writer.WriteByte(0);
        writer.WriteByte(0);

        // Arm 0 packed column: 3 Int64 values
        writer.WriteInt64(10);
        writer.WriteInt64(20);
        writer.WriteInt64(30);
        // Arm 1 packed column: 0 String values (nothing to write).

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        reader.ReadPrefix(ref pr);
        using var col = reader.ReadTypedColumn(ref pr, 3);

        Assert.Equal(3, col.Count);
        var r0 = (ClickHouseVariant)col.GetValue(0)!;
        var r1 = (ClickHouseVariant)col.GetValue(1)!;
        var r2 = (ClickHouseVariant)col.GetValue(2)!;
        Assert.Equal((byte)0, r0.Discriminator);
        Assert.Equal(10L, r0.Value);
        Assert.Equal(20L, r1.Value);
        Assert.Equal(30L, r2.Value);
    }

    [Fact]
    public void ReadTypedColumn_MixedArmsAndNull_ReturnsCorrectRows()
    {
        var reader = (VariantColumnReader)ReaderFactory.CreateReader("Variant(Int64, String)");

        using var buffer = new PooledBufferWriter();
        var writer = new ProtocolWriter(buffer);
        writer.WriteUInt64(0); // version
        // Rows: [Int64 42], [String "hi"], [NULL], [Int64 7], [String "ok"]
        writer.WriteByte(0);
        writer.WriteByte(1);
        writer.WriteByte(255);
        writer.WriteByte(0);
        writer.WriteByte(1);

        // Arm 0 (Int64) — 2 values in row order
        writer.WriteInt64(42);
        writer.WriteInt64(7);
        // Arm 1 (String) — 2 values in row order
        writer.WriteString("hi");
        writer.WriteString("ok");

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        reader.ReadPrefix(ref pr);
        using var col = reader.ReadTypedColumn(ref pr, 5);

        var row0 = (ClickHouseVariant)col.GetValue(0)!;
        var row1 = (ClickHouseVariant)col.GetValue(1)!;
        var row2 = (ClickHouseVariant)col.GetValue(2)!;
        var row3 = (ClickHouseVariant)col.GetValue(3)!;
        var row4 = (ClickHouseVariant)col.GetValue(4)!;
        Assert.Equal((byte)0, row0.Discriminator);
        Assert.Equal(42L, row0.Value);
        Assert.Equal((byte)1, row1.Discriminator);
        Assert.Equal("hi", row1.Value);
        Assert.True(row2.IsNull);
        Assert.Equal(7L, row3.Value);
        Assert.Equal("ok", row4.Value);
    }

    [Fact]
    public void ReadTypedColumn_ZeroRows_StillConsumesVersionHeader()
    {
        var reader = (VariantColumnReader)ReaderFactory.CreateReader("Variant(Int64)");

        using var buffer = new PooledBufferWriter();
        var writer = new ProtocolWriter(buffer);
        writer.WriteUInt64(0);

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        reader.ReadPrefix(ref pr);
        using var col = reader.ReadTypedColumn(ref pr, 0);

        Assert.Equal(0, col.Count);
        Assert.Equal(0, pr.Remaining);
    }

    [Fact]
    public void ReadTypedColumn_DiscriminatorOutOfRange_Throws()
    {
        var reader = (VariantColumnReader)ReaderFactory.CreateReader("Variant(Int64, String)");

        using var buffer = new PooledBufferWriter();
        var writer = new ProtocolWriter(buffer);
        writer.WriteUInt64(0);
        writer.WriteByte(5); // invalid: only arms 0, 1 or 255 are valid.

        Assert.Throws<InvalidOperationException>(() =>
        {
            var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
            reader.ReadPrefix(ref pr);
            using var _ = reader.ReadTypedColumn(ref pr, 1);
        });
    }

    [Fact]
    public void ReadPrefix_UnsupportedVersion_Throws()
    {
        var reader = (VariantColumnReader)ReaderFactory.CreateReader("Variant(Int64)");

        using var buffer = new PooledBufferWriter();
        var writer = new ProtocolWriter(buffer);
        writer.WriteUInt64(99);

        Assert.Throws<NotSupportedException>(() =>
        {
            var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
            reader.ReadPrefix(ref pr);
        });
    }

    [Fact]
    public void RoundTrip_WriterThenReader_PreservesMixedRows()
    {
        var typeName = "Variant(Int64, String)";
        var w = (VariantColumnWriter)WriterFactory.CreateWriter(typeName);
        var r = (VariantColumnReader)ReaderFactory.CreateReader(typeName);

        var source = new[]
        {
            new ClickHouseVariant(0, 42L),
            new ClickHouseVariant(1, "hello"),
            ClickHouseVariant.Null,
            new ClickHouseVariant(0, -1L),
            new ClickHouseVariant(1, ""),
        };

        using var buffer = new PooledBufferWriter();
        var pw = new ProtocolWriter(buffer);
        w.WriteColumn(ref pw, source);

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var col = r.ReadTypedColumn(ref pr, source.Length);

        Assert.Equal(source.Length, col.Count);
        for (int i = 0; i < source.Length; i++)
            Assert.Equal(source[i], (ClickHouseVariant)col.GetValue(i)!);
    }

    [Fact]
    public void TypedGetters_ReadUnboxed()
    {
        var typeName = "Variant(Int64, String)";
        var w = (VariantColumnWriter)WriterFactory.CreateWriter(typeName);
        var r = (VariantColumnReader)ReaderFactory.CreateReader(typeName);

        var source = new[]
        {
            new ClickHouseVariant(0, 42L),
            new ClickHouseVariant(1, "hello"),
            ClickHouseVariant.Null,
            new ClickHouseVariant(0, -1L),
        };

        using var buffer = new PooledBufferWriter();
        var pw = new ProtocolWriter(buffer);
        w.WriteColumn(ref pw, source);

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var col = (VariantTypedColumn)r.ReadTypedColumn(ref pr, source.Length);
        try
        {
            Assert.Equal((byte)0, col.GetDiscriminator(0));
            Assert.Equal(42L, col.GetTyped<long, string>(0).Arm0);
            Assert.Equal("hello", col.GetTyped<long, string>(1).Arm1);
            Assert.True(col.GetTyped<long, string>(2).IsNull);
            Assert.Equal(-1L, col.GetTyped<long, string>(3).Arm0);

            // Arm access returns typed column without boxing.
            var armLong = col.GetArm<long>(0);
            Assert.Equal(2, armLong.Count);
            Assert.Equal(42L, armLong[0]);
            Assert.Equal(-1L, armLong[1]);
        }
        finally { col.Dispose(); }
    }

    [Fact]
    public void TypedGetters_MismatchedArmType_Throws()
    {
        var typeName = "Variant(Int64, String)";
        var w = (VariantColumnWriter)WriterFactory.CreateWriter(typeName);
        var r = (VariantColumnReader)ReaderFactory.CreateReader(typeName);

        var source = new[] { new ClickHouseVariant(0, 42L) };

        using var buffer = new PooledBufferWriter();
        var pw = new ProtocolWriter(buffer);
        w.WriteColumn(ref pw, source);

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var col = (VariantTypedColumn)r.ReadTypedColumn(ref pr, 1);
        try
        {
            Assert.Throws<InvalidCastException>(() => col.GetTyped<int, string>(0));
        }
        finally { col.Dispose(); }
    }

    [Fact]
    public void RoundTrip_LargeRandomMix_10kRows()
    {
        var typeName = "Variant(Int64, String)";
        var w = (VariantColumnWriter)WriterFactory.CreateWriter(typeName);
        var r = (VariantColumnReader)ReaderFactory.CreateReader(typeName);

        var rand = new Random(42);
        var rows = new ClickHouseVariant[10_000];
        for (int i = 0; i < rows.Length; i++)
        {
            var roll = rand.Next(3);
            rows[i] = roll switch
            {
                0 => ClickHouseVariant.Null,
                1 => new ClickHouseVariant(0, (long)rand.Next()),
                _ => new ClickHouseVariant(1, $"s{rand.Next():X}"),
            };
        }

        using var buffer = new PooledBufferWriter();
        var pw = new ProtocolWriter(buffer);
        w.WriteColumn(ref pw, rows);

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var col = r.ReadTypedColumn(ref pr, rows.Length);

        Assert.Equal(rows.Length, col.Count);
        for (int i = 0; i < rows.Length; i++)
            Assert.Equal(rows[i], (ClickHouseVariant)col.GetValue(i)!);
    }

    [Fact]
    public void Skipper_RoundTrip_ConsumesAllBytes()
    {
        var typeName = "Variant(Int64, String)";
        var w = (VariantColumnWriter)WriterFactory.CreateWriter(typeName);
        var s = SkipperFactory.CreateSkipper(typeName);

        var source = new[]
        {
            new ClickHouseVariant(0, 5L),
            new ClickHouseVariant(1, "x"),
            ClickHouseVariant.Null,
        };

        using var buffer = new PooledBufferWriter();
        var pw = new ProtocolWriter(buffer);
        // Match the Block dispatch sequence: prefix bytes (state prefix) precede
        // the column's bulk data, which is what the skipper expects on the wire.
        w.WritePrefix(ref pw);
        w.WriteColumn(ref pw, source);

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        Assert.True(s.TrySkipColumn(ref pr, source.Length));
        Assert.Equal(0, pr.Remaining);
    }
}
