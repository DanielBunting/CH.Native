using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Data.Geo;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

public class GeometryColumnReaderTests
{
    [Fact]
    public void TypeName_IsGeometry() => Assert.Equal("Geometry", new GeometryColumnReader().TypeName);

    [Fact]
    public void ClrType_IsGeometry() => Assert.Equal(typeof(Geometry), new GeometryColumnReader().ClrType);

    [Fact]
    public void Registry_ResolvesGeometryReader()
    {
        var reader = ColumnReaderRegistry.Default.GetReader("Geometry");
        Assert.IsType<GeometryColumnReader>(reader);
    }

    [Fact]
    public void ReadTypedColumn_BasicMode_MixedArms_RoundTripsThroughWriter()
    {
        var rows = new[]
        {
            Geometry.From(new Point(1, 2)),
            Geometry.FromRing(new Point[] { new(0, 0), new(1, 0), new(1, 1), new(0, 1) }),
            Geometry.FromLineString(new Point[] { new(10, 10), new(20, 20) }),
            Geometry.FromPolygon(new Point[][] { new Point[] { new(0, 0), new(1, 0), new(1, 1) } }),
            Geometry.FromMultiLineString(new Point[][] { new Point[] { new(5, 5) } }),
            Geometry.FromMultiPolygon(new Point[][][] { new Point[][] { new Point[] { new(0, 0), new(1, 0), new(1, 1), new(0, 1) } } }),
        };

        var roundTripped = RoundTrip(rows);

        Assert.Equal(rows.Length, roundTripped.Count);
        for (int i = 0; i < rows.Length; i++)
        {
            Assert.Equal(rows[i].Kind, roundTripped[i].Kind);
        }

        Assert.Equal(new Point(1, 2), roundTripped[0].AsPoint());
        Assert.Equal(rows[1].AsRing(), roundTripped[1].AsRing());
        Assert.Equal(rows[2].AsLineString(), roundTripped[2].AsLineString());
        Assert.Equal(rows[3].AsPolygon()[0], roundTripped[3].AsPolygon()[0]);
        Assert.Equal(rows[4].AsMultiLineString()[0], roundTripped[4].AsMultiLineString()[0]);
        Assert.Equal(rows[5].AsMultiPolygon()[0][0], roundTripped[5].AsMultiPolygon()[0][0]);
    }

    [Fact]
    public void ReadTypedColumn_BasicMode_IncludesNullRows()
    {
        var rows = new[]
        {
            Geometry.From(new Point(1, 2)),
            Geometry.Null,
            Geometry.FromRing(new Point[] { new(0, 0) }),
            Geometry.Null,
        };

        var rt = RoundTrip(rows);

        Assert.Equal(4, rt.Count);
        Assert.Equal(GeometryKind.Point, rt[0].Kind);
        Assert.True(rt[1].IsNull);
        Assert.Equal(GeometryKind.Ring, rt[2].Kind);
        Assert.True(rt[3].IsNull);
    }

    [Fact]
    public void ReadTypedColumn_BasicMode_SameArmRepeated_PreservesOrder()
    {
        var rows = new[]
        {
            Geometry.From(new Point(1, 1)),
            Geometry.From(new Point(2, 2)),
            Geometry.From(new Point(3, 3)),
        };

        var rt = RoundTrip(rows);

        for (int i = 0; i < rows.Length; i++)
        {
            Assert.Equal(rows[i].AsPoint(), rt[i].AsPoint());
        }
    }

    [Fact]
    public void ReadTypedColumn_BasicMode_AllNulls()
    {
        var rows = new[] { Geometry.Null, Geometry.Null, Geometry.Null };

        var rt = RoundTrip(rows);

        Assert.Equal(3, rt.Count);
        Assert.All(new[] { rt[0], rt[1], rt[2] }, g => Assert.True(g.IsNull));
    }

    [Fact]
    public void ReadTypedColumn_CompactMode_Plain_DecodesCorrectly()
    {
        // Manually construct bytes: mode=COMPACT(1); varint limit=3; format=PLAIN(0); 3 discriminators.
        var rows = new[]
        {
            Geometry.From(new Point(7, 8)),
            Geometry.Null,
            Geometry.FromRing(new Point[] { new(1, 2) }),
        };

        // Build arm data by calling writers individually (after reading mode+discriminators we expect arm payloads).
        // Simulate COMPACT+PLAIN prefix, then reuse the BASIC per-arm layout.
        var basicBuf = new ArrayBufferWriter<byte>();
        var basicWriter = new ProtocolWriter(basicBuf);
        new GeometryColumnWriter().WriteColumn(ref basicWriter, rows);
        // BASIC bytes are: 8-byte mode(0) + 3 disc + arm data. Swap the prefix to COMPACT+PLAIN form.
        var basic = basicBuf.WrittenSpan;
        var afterDiscriminators = 8 + rows.Length;
        var armData = basic.Slice(afterDiscriminators).ToArray();
        var discriminators = basic.Slice(8, rows.Length).ToArray();

        var compactBuf = new ArrayBufferWriter<byte>();
        var compactWriter = new ProtocolWriter(compactBuf);
        compactWriter.WriteUInt64(1);                       // mode = COMPACT
        compactWriter.WriteVarInt((ulong)rows.Length);      // granule limit
        compactWriter.WriteByte(0);                         // PLAIN format
        foreach (var d in discriminators) compactWriter.WriteByte(d);
        foreach (var b in armData) compactWriter.WriteByte(b);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(compactBuf.WrittenMemory));
        using var column = new GeometryColumnReader().ReadTypedColumn(ref reader, rows.Length);

        Assert.Equal(rows.Length, column.Count);
        Assert.Equal(new Point(7, 8), column[0].AsPoint());
        Assert.True(column[1].IsNull);
        Assert.Equal(new Point(1, 2), column[2].AsRing()[0]);
    }

    [Fact]
    public void ReadTypedColumn_CompactMode_SameDiscriminator_DecodesCorrectly()
    {
        // All rows are Points — granule format COMPACT (single discriminator for N rows).
        var rows = new[]
        {
            Geometry.From(new Point(1, 1)),
            Geometry.From(new Point(2, 2)),
            Geometry.From(new Point(3, 3)),
        };

        var basicBuf = new ArrayBufferWriter<byte>();
        var basicWriter = new ProtocolWriter(basicBuf);
        new GeometryColumnWriter().WriteColumn(ref basicWriter, rows);
        var armData = basicBuf.WrittenSpan.Slice(8 + rows.Length).ToArray();

        var compactBuf = new ArrayBufferWriter<byte>();
        var compactWriter = new ProtocolWriter(compactBuf);
        compactWriter.WriteUInt64(1);                    // COMPACT mode
        compactWriter.WriteVarInt((ulong)rows.Length);   // granule limit
        compactWriter.WriteByte(1);                      // COMPACT granule format
        compactWriter.WriteByte((byte)GeometryKind.Point); // single discriminator for all rows
        foreach (var b in armData) compactWriter.WriteByte(b);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(compactBuf.WrittenMemory));
        using var column = new GeometryColumnReader().ReadTypedColumn(ref reader, rows.Length);

        for (int i = 0; i < rows.Length; i++)
            Assert.Equal(rows[i].AsPoint(), column[i].AsPoint());
    }

    [Fact]
    public void ReadTypedColumn_EmptyRowCount_ReturnsEmpty()
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(Array.Empty<byte>()));
        using var column = new GeometryColumnReader().ReadTypedColumn(ref reader, 0);
        Assert.Equal(0, column.Count);
    }

    [Fact]
    public void ReadTypedColumn_UnknownMode_Throws()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);
        writer.WriteUInt64(42);
        var bytes = buf.WrittenMemory;

        Assert.Throws<InvalidDataException>(() => ReadWithGeometry(bytes));

        static void ReadWithGeometry(ReadOnlyMemory<byte> bytes)
        {
            var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
            using var _ = new GeometryColumnReader().ReadTypedColumn(ref reader, 1);
        }
    }

    [Fact]
    public void WriteColumn_WritesModeZeroPrefix()
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);
        new GeometryColumnWriter().WriteColumn(ref writer, new[] { Geometry.From(new Point(1, 2)) });

        // First 8 bytes are UInt64 mode = 0 (BASIC).
        var mode = BitConverter.ToUInt64(buf.WrittenSpan.Slice(0, 8));
        Assert.Equal(0UL, mode);
    }

    [Fact]
    public void WriteColumn_WritesDiscriminatorsAfterMode()
    {
        var rows = new[]
        {
            Geometry.From(new Point(1, 2)),
            Geometry.Null,
            Geometry.FromRing(new Point[] { new(0, 0) }),
        };

        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);
        new GeometryColumnWriter().WriteColumn(ref writer, rows);

        Assert.Equal((byte)GeometryKind.Point, buf.WrittenSpan[8]);
        Assert.Equal((byte)GeometryKind.Null, buf.WrittenSpan[9]);
        Assert.Equal((byte)GeometryKind.Ring, buf.WrittenSpan[10]);
    }

    private static TypedColumn<Geometry> RoundTrip(Geometry[] rows)
    {
        var buf = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buf);
        new GeometryColumnWriter().WriteColumn(ref writer, rows);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buf.WrittenMemory));
        return new GeometryColumnReader().ReadTypedColumn(ref reader, rows.Length);
    }
}
