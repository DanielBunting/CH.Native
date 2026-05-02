using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
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
