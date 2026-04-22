using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Data.Geo;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

public class PointColumnWriterTests
{
    [Fact]
    public void TypeName_IsPoint() => Assert.Equal("Point", new PointColumnWriter().TypeName);

    [Fact]
    public void ClrType_IsPoint() => Assert.Equal(typeof(Point), new PointColumnWriter().ClrType);

    [Fact]
    public void WriteValue_EmitsTwoLittleEndianDoubles()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new PointColumnWriter().WriteValue(ref writer, new Point(1.5, -2.25));

        Assert.Equal(16, buffer.WrittenCount);
        Assert.Equal(1.5, BitConverter.ToDouble(buffer.WrittenSpan.Slice(0, 8)));
        Assert.Equal(-2.25, BitConverter.ToDouble(buffer.WrittenSpan.Slice(8, 8)));
    }

    [Fact]
    public void WriteColumn_ProducesColumnarLayout()
    {
        var points = new[] { new Point(1, 10), new Point(2, 20), new Point(3, 30) };
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new PointColumnWriter().WriteColumn(ref writer, points);

        Assert.Equal(48, buffer.WrittenCount);
        // All Xs
        Assert.Equal(1.0, BitConverter.ToDouble(buffer.WrittenSpan.Slice(0, 8)));
        Assert.Equal(2.0, BitConverter.ToDouble(buffer.WrittenSpan.Slice(8, 8)));
        Assert.Equal(3.0, BitConverter.ToDouble(buffer.WrittenSpan.Slice(16, 8)));
        // Then all Ys
        Assert.Equal(10.0, BitConverter.ToDouble(buffer.WrittenSpan.Slice(24, 8)));
        Assert.Equal(20.0, BitConverter.ToDouble(buffer.WrittenSpan.Slice(32, 8)));
        Assert.Equal(30.0, BitConverter.ToDouble(buffer.WrittenSpan.Slice(40, 8)));
    }

    [Fact]
    public void WriteColumn_ReadBack_RoundTrips()
    {
        var points = new[] { new Point(1, 10), new Point(2, 20), new Point(3, 30) };
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new PointColumnWriter().WriteColumn(ref writer, points);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new PointColumnReader().ReadTypedColumn(ref reader, points.Length);

        Assert.Equal(points.Length, column.Count);
        for (int i = 0; i < points.Length; i++)
            Assert.Equal(points[i], column[i]);
    }

    [Theory]
    [MemberData(nameof(CoercibleValues))]
    public void WriteValue_ViaNonGenericInterface_AcceptsCompatibleShapes(object? input, Point expected)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        ((IColumnWriter)new PointColumnWriter()).WriteValue(ref writer, input);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var result = new PointColumnReader().ReadValue(ref reader);
        Assert.Equal(expected, result);
    }

    public static IEnumerable<object?[]> CoercibleValues()
    {
        yield return new object?[] { new Point(3, 4), new Point(3, 4) };
        yield return new object?[] { (3.0, 4.0), new Point(3, 4) };
        yield return new object?[] { Tuple.Create(3.0, 4.0), new Point(3, 4) };
        yield return new object?[] { null, Point.Zero };
    }

    [Fact]
    public void Registry_ResolvesPointWriter()
    {
        var writer = ColumnWriterRegistry.Default.GetWriter("Point");
        Assert.IsType<PointColumnWriter>(writer);
    }
}
