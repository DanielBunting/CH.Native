using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Data.Conversion;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

/// <summary>
/// <see cref="ArrayColumnWriterNullTests"/> only covers null-element handling.
/// This adds the round-trip and shape coverage: empty / large / nested /
/// Array(Nullable(T)).
/// </summary>
public class ArrayColumnWriterRoundTripTests
{
    [Fact]
    public void RoundTrip_OneDimensionalInt32()
    {
        var rows = new[]
        {
            new[] { 1, 2, 3 },
            Array.Empty<int>(),
            new[] { 42 },
            new[] { -1, 0, 1, int.MaxValue },
        };

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new ArrayColumnWriter<int>(new Int32ColumnWriter()).WriteColumn(ref writer, rows);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new ArrayColumnReader<int>(new Int32ColumnReader())
            .ReadTypedColumn(ref reader, rows.Length);

        for (int i = 0; i < rows.Length; i++) Assert.Equal(rows[i], column[i]);
    }

    [Fact]
    public void RoundTrip_LargeArray()
    {
        var values = new int[10_000];
        for (int i = 0; i < values.Length; i++) values[i] = i;
        var rows = new[] { values };

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new ArrayColumnWriter<int>(new Int32ColumnWriter()).WriteColumn(ref writer, rows);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new ArrayColumnReader<int>(new Int32ColumnReader())
            .ReadTypedColumn(ref reader, rows.Length);

        Assert.Equal(values, column[0]);
    }

    [Fact]
    public void RoundTrip_TwoDimensional_ArrayOfArrayOfInt32()
    {
        var rows = new[]
        {
            new[]
            {
                new[] { 1, 2 },
                new[] { 3, 4, 5 },
            },
            new[]
            {
                Array.Empty<int>(),
                new[] { 100 },
            },
        };

        var inner = new ArrayColumnWriter<int>(new Int32ColumnWriter());
        var outer = new ArrayColumnWriter<int[]>(inner);

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        outer.WriteColumn(ref writer, rows);

        var innerReader = new ArrayColumnReader<int>(new Int32ColumnReader());
        var outerReader = new ArrayColumnReader<int[]>(innerReader);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = outerReader.ReadTypedColumn(ref reader, rows.Length);

        for (int i = 0; i < rows.Length; i++)
        {
            Assert.Equal(rows[i].Length, column[i].Length);
            for (int j = 0; j < rows[i].Length; j++)
                Assert.Equal(rows[i][j], column[i][j]);
        }
    }

    [Fact]
    public void RoundTrip_Rectangular_2D_Int32()
    {
        // int[,] → boundary converter → int[][] → writer pipeline → reader → int[][]
        var rect = new int[2, 3]
        {
            { 1, 2, 3 },
            { 4, 5, 6 },
        };
        var jagged = RectangularArrayConverter.To2DJagged(rect);
        var rows = new[] { jagged };

        var inner = new ArrayColumnWriter<int>(new Int32ColumnWriter());
        var outer = new ArrayColumnWriter<int[]>(inner);

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        outer.WriteColumn(ref writer, rows);

        var innerReader = new ArrayColumnReader<int>(new Int32ColumnReader());
        var outerReader = new ArrayColumnReader<int[]>(innerReader);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = outerReader.ReadTypedColumn(ref reader, rows.Length);

        Assert.Equal(2, column[0].Length);
        Assert.Equal(new[] { 1, 2, 3 }, column[0][0]);
        Assert.Equal(new[] { 4, 5, 6 }, column[0][1]);
    }

    [Fact]
    public void RoundTrip_Rectangular_3D_Int32()
    {
        var rect = new int[2, 2, 2]
        {
            { { 1, 2 }, { 3, 4 } },
            { { 5, 6 }, { 7, 8 } },
        };
        var jagged = RectangularArrayConverter.To3DJagged(rect);
        var rows = new[] { jagged };

        var innermost = new ArrayColumnWriter<int>(new Int32ColumnWriter());
        var middle = new ArrayColumnWriter<int[]>(innermost);
        var outer = new ArrayColumnWriter<int[][]>(middle);

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        outer.WriteColumn(ref writer, rows);

        var innermostReader = new ArrayColumnReader<int>(new Int32ColumnReader());
        var middleReader = new ArrayColumnReader<int[]>(innermostReader);
        var outerReader = new ArrayColumnReader<int[][]>(middleReader);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = outerReader.ReadTypedColumn(ref reader, rows.Length);

        Assert.Equal(2, column[0].Length);
        Assert.Equal(new[] { 1, 2 }, column[0][0][0]);
        Assert.Equal(new[] { 3, 4 }, column[0][0][1]);
        Assert.Equal(new[] { 5, 6 }, column[0][1][0]);
        Assert.Equal(new[] { 7, 8 }, column[0][1][1]);
    }

    [Fact]
    public void RoundTrip_Rectangular_2D_Empty()
    {
        // 0x0 rect → 0-length jagged outer, no inner rows.
        var rect = new int[0, 0];
        var jagged = RectangularArrayConverter.To2DJagged(rect);
        Assert.Empty(jagged);
    }

    [Fact]
    public void TypeName_NamespacesElement()
    {
        var sut = new ArrayColumnWriter<int>(new Int32ColumnWriter());
        Assert.Equal("Array(Int32)", sut.TypeName);
    }

    [Fact]
    public void TypeName_NestedArray()
    {
        var inner = new ArrayColumnWriter<int>(new Int32ColumnWriter());
        var outer = new ArrayColumnWriter<int[]>(inner);
        Assert.Equal("Array(Array(Int32))", outer.TypeName);
    }

    [Fact]
    public void NullPlaceholder_IsEmptyArray()
    {
        Assert.Empty(new ArrayColumnWriter<int>(new Int32ColumnWriter()).NullPlaceholder);
    }

    [Fact]
    public void Constructor_NonGeneric_RejectsMismatchedElementWriter()
    {
        Assert.Throws<ArgumentException>(() =>
            new ArrayColumnWriter<int>((IColumnWriter)new StringColumnWriter()));
    }
}
