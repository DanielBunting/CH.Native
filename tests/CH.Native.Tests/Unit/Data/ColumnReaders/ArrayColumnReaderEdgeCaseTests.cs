using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

/// <summary>
/// <see cref="ArrayColumnWriterRoundTripTests"/> covers the writer side and a
/// reader-via-symmetry pass. This pins reader-only edge cases: large blocks,
/// deep nesting, Array(Nullable(T)), and the empty-row fast path.
/// </summary>
public class ArrayColumnReaderEdgeCaseTests
{
    [Fact]
    public void TypeName_NamespacesElement()
    {
        Assert.Equal("Array(Int32)",
            new ArrayColumnReader<int>(new Int32ColumnReader()).TypeName);
    }

    [Fact]
    public void TypeName_DeepNesting()
    {
        var inner = new ArrayColumnReader<int>(new Int32ColumnReader());
        var middle = new ArrayColumnReader<int[]>(inner);
        var outer = new ArrayColumnReader<int[][]>(middle);
        Assert.Equal("Array(Array(Array(Int32)))", outer.TypeName);
    }

    [Fact]
    public void ClrType_IsTArray()
    {
        Assert.Equal(typeof(int[]),
            new ArrayColumnReader<int>(new Int32ColumnReader()).ClrType);
    }

    [Fact]
    public void ReadTypedColumn_ZeroRows_ReturnsEmpty()
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(Array.Empty<byte>()));
        using var column = new ArrayColumnReader<int>(new Int32ColumnReader())
            .ReadTypedColumn(ref reader, 0);

        Assert.Equal(0, column.Count);
    }

    [Fact]
    public void ReadTypedColumn_LargeArray_DecodesAllElements()
    {
        var values = new int[50_000];
        for (int i = 0; i < values.Length; i++) values[i] = i;
        var rows = new[] { values };

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new ArrayColumnWriter<int>(new Int32ColumnWriter()).WriteColumn(ref writer, rows);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new ArrayColumnReader<int>(new Int32ColumnReader())
            .ReadTypedColumn(ref reader, rows.Length);

        Assert.Equal(values.Length, column[0].Length);
        for (int i = 0; i < values.Length; i += 100)
            Assert.Equal(values[i], column[0][i]);
    }

    [Fact]
    public void ReadTypedColumn_DeeplyNested_ThreeLevels()
    {
        // Array(Array(Array(Int32))) — exercises three layers of offset
        // tables and one terminal element column.
        var rows = new[]
        {
            new[]
            {
                new[] { new[] { 1, 2 }, new[] { 3 } },
                new[] { Array.Empty<int>() },
            },
        };

        var l1Writer = new ArrayColumnWriter<int>(new Int32ColumnWriter());
        var l2Writer = new ArrayColumnWriter<int[]>(l1Writer);
        var l3Writer = new ArrayColumnWriter<int[][]>(l2Writer);

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        l3Writer.WriteColumn(ref writer, rows);

        var l1Reader = new ArrayColumnReader<int>(new Int32ColumnReader());
        var l2Reader = new ArrayColumnReader<int[]>(l1Reader);
        var l3Reader = new ArrayColumnReader<int[][]>(l2Reader);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = l3Reader.ReadTypedColumn(ref reader, rows.Length);

        Assert.Equal(rows[0].Length, column[0].Length);
        Assert.Equal(rows[0][0].Length, column[0][0].Length);
        Assert.Equal(new[] { 1, 2 }, column[0][0][0]);
        Assert.Equal(new[] { 3 }, column[0][0][1]);
        Assert.Empty(column[0][1][0]);
    }

    [Fact]
    public void ReadTypedColumn_ArrayOfNullable_PreservesNullsPerElement()
    {
        // Array(Nullable(Int32)) — outer offsets, then a Nullable column
        // (bitmap + values) for the flattened element list.
        var rows = new[]
        {
            new int?[] { 1, null, 2 },
            new int?[] { null, null },
            Array.Empty<int?>(),
        };

        var elementWriter = new NullableColumnWriter<int>(new Int32ColumnWriter());
        var arrayWriter = new ArrayColumnWriter<int?>(elementWriter);

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        arrayWriter.WriteColumn(ref writer, rows);

        var elementReader = new NullableColumnReader<int>(new Int32ColumnReader());
        var arrayReader = new ArrayColumnReader<int?>(elementReader);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = arrayReader.ReadTypedColumn(ref reader, rows.Length);

        for (int i = 0; i < rows.Length; i++)
        {
            Assert.Equal(rows[i].Length, column[i].Length);
            for (int j = 0; j < rows[i].Length; j++)
                Assert.Equal(rows[i][j], column[i][j]);
        }
    }

    [Fact]
    public void Constructor_NonGeneric_RejectsMismatchedElementReader()
    {
        Assert.Throws<ArgumentException>(() =>
            new ArrayColumnReader<int>((IColumnReader)new StringColumnReader()));
    }
}
