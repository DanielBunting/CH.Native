using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.Geo;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

public class PointColumnReaderTests
{
    [Fact]
    public void TypeName_IsPoint()
    {
        Assert.Equal("Point", new PointColumnReader().TypeName);
    }

    [Fact]
    public void ClrType_IsPoint()
    {
        Assert.Equal(typeof(Point), new PointColumnReader().ClrType);
    }

    [Fact]
    public void ReadValue_ReadsTwoLittleEndianDoubles()
    {
        var bytes = new byte[16];
        BitConverter.TryWriteBytes(bytes.AsSpan(0, 8), 1.5);
        BitConverter.TryWriteBytes(bytes.AsSpan(8, 8), -2.25);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        var result = new PointColumnReader().ReadValue(ref reader);

        Assert.Equal(new Point(1.5, -2.25), result);
    }

    [Fact]
    public void ReadTypedColumn_UsesColumnarLayout()
    {
        // 3 rows: all Xs first, then all Ys
        double[] xs = [1.0, 2.0, 3.0];
        double[] ys = [10.0, 20.0, 30.0];
        var bytes = new byte[6 * 8];
        for (int i = 0; i < 3; i++)
            BitConverter.TryWriteBytes(bytes.AsSpan(i * 8, 8), xs[i]);
        for (int i = 0; i < 3; i++)
            BitConverter.TryWriteBytes(bytes.AsSpan((3 + i) * 8, 8), ys[i]);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        using var column = new PointColumnReader().ReadTypedColumn(ref reader, 3);

        Assert.Equal(3, column.Count);
        Assert.Equal(new Point(1.0, 10.0), column[0]);
        Assert.Equal(new Point(2.0, 20.0), column[1]);
        Assert.Equal(new Point(3.0, 30.0), column[2]);
    }

    [Fact]
    public void ReadTypedColumn_Zero_ReturnsEmpty()
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(Array.Empty<byte>()));
        using var column = new PointColumnReader().ReadTypedColumn(ref reader, 0);
        Assert.Equal(0, column.Count);
    }

    [Fact]
    public void Registry_ResolvesPointReader()
    {
        var reader = ColumnReaderRegistry.Default.GetReader("Point");
        Assert.IsType<PointColumnReader>(reader);
    }

    [Fact]
    public void Registry_ResolvesNullablePoint_ViaFactory()
    {
        var reader = ColumnReaderRegistry.Default.GetReader("Nullable(Point)");
        Assert.IsType<NullableColumnReader<Point>>(reader);
        Assert.Equal(typeof(Point?), reader.ClrType);
    }
}
