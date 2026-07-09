using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

public class TupleColumnReaderCoverageTests
{
    private static IColumnReader[] TwoInts() => new IColumnReader[] { new Int32ColumnReader(), new Int32ColumnReader() };

    [Fact]
    public void Ctor_NoElements_Throws()
    {
        Assert.Throws<ArgumentException>(() => new TupleColumnReader(Array.Empty<IColumnReader>()));
        Assert.Throws<ArgumentException>(() => new TupleColumnReader(null!));
    }

    [Fact]
    public void Ctor_FieldNameCountMismatch_Throws() =>
        Assert.Throws<ArgumentException>(() => new TupleColumnReader(TwoInts(), new[] { "onlyOne" }));

    [Fact]
    public void TypeName_Positional_And_Named()
    {
        Assert.Equal("Tuple(Int32, Int32)", new TupleColumnReader(TwoInts()).TypeName);
        Assert.Equal("Tuple(a Int32, b Int32)", new TupleColumnReader(TwoInts(), new[] { "a", "b" }).TypeName);
    }

    [Fact]
    public void Accessors_And_GetFieldIndex()
    {
        var positional = new TupleColumnReader(TwoInts());
        Assert.Equal(typeof(object), positional.ClrType);
        Assert.Equal(2, positional.Arity);
        Assert.Null(positional.FieldNames);
        Assert.False(positional.HasFieldNames);
        Assert.Equal(-1, positional.GetFieldIndex("a"));   // positional -> always -1

        var named = new TupleColumnReader(TwoInts(), new[] { "a", "b" });
        Assert.True(named.HasFieldNames);
        Assert.Equal(new[] { "a", "b" }, named.FieldNames);
        Assert.Equal(1, named.GetFieldIndex("b"));
        Assert.Equal(-1, named.GetFieldIndex("missing"));
    }

    [Fact]
    public void ReadPrefix_DoesNotThrow()
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(Array.Empty<byte>()));
        new TupleColumnReader(TwoInts()).ReadPrefix(ref reader);
    }

    [Fact]
    public void ReadValue_ReadsElementsInOrder()
    {
        var bytes = new byte[8];
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(0), 42);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(4), 99);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        var value = (ITuple)new TupleColumnReader(TwoInts()).ReadValue(ref reader);
        Assert.Equal(2, value.Length);
        Assert.Equal(42, value[0]);
        Assert.Equal(99, value[1]);
    }

    [Fact]
    public void ReadTypedColumn_ColumnarLayout()
    {
        // Columnar: element-0 column [1,2] then element-1 column [3,4] -> rows (1,3),(2,4).
        var bytes = new byte[16];
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(0), 1);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(4), 2);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(8), 3);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(12), 4);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        using var col = new TupleColumnReader(TwoInts()).ReadTypedColumn(ref reader, 2);
        Assert.Equal(2, col.Count);
        var row0 = (ITuple)col.GetValue(0)!;
        Assert.Equal(1, row0[0]);
        Assert.Equal(3, row0[1]);
    }

    [Fact]
    public void ReadTypedColumn_ZeroRows_Empty()
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(Array.Empty<byte>()));
        using var col = new TupleColumnReader(TwoInts()).ReadTypedColumn(ref reader, 0);
        Assert.Equal(0, col.Count);
    }
}
