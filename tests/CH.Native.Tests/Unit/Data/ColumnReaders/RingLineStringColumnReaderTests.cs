using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.Geo;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

public class RingLineStringColumnReaderTests
{
    [Fact]
    public void Ring_TypeName_IsRing() => Assert.Equal("Ring", new RingColumnReader().TypeName);

    [Fact]
    public void LineString_TypeName_IsLineString() => Assert.Equal("LineString", new LineStringColumnReader().TypeName);

    [Fact]
    public void Both_ClrType_IsPointArray()
    {
        Assert.Equal(typeof(Point[]), new RingColumnReader().ClrType);
        Assert.Equal(typeof(Point[]), new LineStringColumnReader().ClrType);
    }

    [Fact]
    public void Ring_ReadTypedColumn_HandlesMixedSizes()
    {
        // 2 rows: first ring has 2 points, second has 1 point
        // offsets: 2, 3 (UInt64 cumulative)
        // Tuple payload: 3 Xs then 3 Ys
        var bytes = BuildArrayOfPoints(
            new Point[] { new(1, 10), new(2, 20) },
            new Point[] { new(3, 30) });
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        using var column = new RingColumnReader().ReadTypedColumn(ref reader, 2);

        Assert.Equal(2, column.Count);
        Assert.Equal(new[] { new Point(1, 10), new Point(2, 20) }, column[0]);
        Assert.Equal(new[] { new Point(3, 30) }, column[1]);
    }

    [Fact]
    public void Ring_ReadTypedColumn_EmptyRing()
    {
        var bytes = BuildArrayOfPoints(Array.Empty<Point>());
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        using var column = new RingColumnReader().ReadTypedColumn(ref reader, 1);

        Assert.Equal(1, column.Count);
        Assert.Empty(column[0]);
    }

    [Fact]
    public void LineString_RingBytes_Match()
    {
        var sample = new Point[] { new(0, 0), new(1, 1), new(2, 0) };
        var bytes = BuildArrayOfPoints(sample);

        var reader1 = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        using var ring = new RingColumnReader().ReadTypedColumn(ref reader1, 1);

        var reader2 = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        using var line = new LineStringColumnReader().ReadTypedColumn(ref reader2, 1);

        Assert.Equal(ring[0], line[0]);
    }

    [Fact]
    public void Registry_ResolvesRingAndLineString()
    {
        Assert.IsType<RingColumnReader>(ColumnReaderRegistry.Default.GetReader("Ring"));
        Assert.IsType<LineStringColumnReader>(ColumnReaderRegistry.Default.GetReader("LineString"));
    }

    internal static byte[] BuildArrayOfPoints(params Point[][] rings)
    {
        // Array(Point) wire format: UInt64 offsets per row, then Tuple(Float64,Float64)
        // columnar: all Xs then all Ys.
        var totalPoints = rings.Sum(r => r.Length);
        var buffer = new byte[rings.Length * 8 + totalPoints * 16];
        var span = buffer.AsSpan();
        ulong cumulative = 0;
        int offset = 0;
        for (int i = 0; i < rings.Length; i++)
        {
            cumulative += (ulong)rings[i].Length;
            BitConverter.TryWriteBytes(span.Slice(offset, 8), cumulative);
            offset += 8;
        }
        // All Xs
        for (int r = 0; r < rings.Length; r++)
        {
            for (int p = 0; p < rings[r].Length; p++)
            {
                BitConverter.TryWriteBytes(span.Slice(offset, 8), rings[r][p].X);
                offset += 8;
            }
        }
        // All Ys
        for (int r = 0; r < rings.Length; r++)
        {
            for (int p = 0; p < rings[r].Length; p++)
            {
                BitConverter.TryWriteBytes(span.Slice(offset, 8), rings[r][p].Y);
                offset += 8;
            }
        }
        return buffer;
    }
}
