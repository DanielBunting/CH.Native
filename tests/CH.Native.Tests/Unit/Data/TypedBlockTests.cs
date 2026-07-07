using CH.Native.Data;
using Xunit;

namespace CH.Native.Tests.Unit.Data;

public class TypedBlockTests
{
    private static TypedBlock Make() => new()
    {
        TableName = "t",
        ColumnNames = new[] { "a", "b" },
        ColumnTypes = new[] { "Int32", "String" },
        Columns = new ITypedColumn[]
        {
            new TypedColumn<int>(new[] { 1, 2, 3 }),
            new TypedColumn<string>(new[] { "x", "y", "z" }),
        },
    };

    [Fact]
    public void Shape_And_Access()
    {
        var block = Make();
        Assert.Equal(2, block.ColumnCount);
        Assert.Equal(3, block.RowCount);
        Assert.Equal(1, block.GetValue(0, 0));
        Assert.Equal("y", block.GetValue(1, 1));
        Assert.Same(block.Columns[0], block[0]);
        Assert.Equal(3, block.GetColumn<int>(0).Count);
    }

    [Fact]
    public void GetColumn_WrongType_Throws()
    {
        var block = Make();
        Assert.Throws<InvalidCastException>(() => block.GetColumn<string>(0));
    }

    [Fact]
    public void GetColumnIndex_CaseInsensitive_And_Missing()
    {
        var block = Make();
        Assert.Equal(0, block.GetColumnIndex("A"));
        Assert.Equal(1, block.GetColumnIndex("b"));
        Assert.Equal(-1, block.GetColumnIndex("missing"));
    }

    [Fact]
    public void GetVariant_NonVariantColumn_Throws()
    {
        var block = Make();
        Assert.Throws<InvalidCastException>(() => block.GetVariant<int, int>(0, 0));
    }

    [Fact]
    public void GetVariant_VariantColumn_ReturnsTyped()
    {
        var variantCol = new VariantTypedColumn(
            discriminators: new byte[] { 0 },
            rowCount: 1,
            armColumns: new ITypedColumn[] { new TypedColumn<int>(new[] { 42 }), new TypedColumn<string>(new[] { "x" }) },
            rowToArmOffset: new[] { 0 });
        var block = new TypedBlock
        {
            TableName = "t",
            ColumnNames = new[] { "v" },
            ColumnTypes = new[] { "Variant(Int32, String)" },
            Columns = new ITypedColumn[] { variantCol },
        };
        var v = block.GetVariant<int, string>(0, 0);
        Assert.Equal(0, v.Discriminator);
        Assert.Equal(42, v.Arm0);
    }

    [Fact]
    public void RowCount_EmptyBlock_IsZero()
    {
        var block = new TypedBlock
        {
            TableName = "t",
            ColumnNames = Array.Empty<string>(),
            ColumnTypes = Array.Empty<string>(),
            Columns = Array.Empty<ITypedColumn>(),
        };
        Assert.Equal(0, block.RowCount);
        Assert.Equal(0, block.ColumnCount);
    }

    [Fact]
    public void Dispose_DisposesColumns()
    {
        var block = Make();
        block.Dispose();   // must not throw
    }
}
