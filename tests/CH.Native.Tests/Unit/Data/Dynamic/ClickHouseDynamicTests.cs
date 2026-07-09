using CH.Native.Data.Dynamic;
using Xunit;

namespace CH.Native.Tests.Unit.Data.Dynamic;

public class ClickHouseDynamicTests
{
    [Fact]
    public void Ctor_And_Properties()
    {
        var d = new ClickHouseDynamic(0, 42, "Int32");
        Assert.Equal(0, d.Discriminator);
        Assert.Equal(42, d.Value);
        Assert.Equal("Int32", d.DeclaredTypeName);
        Assert.False(d.IsNull);
    }

    [Fact]
    public void Null_IsNull()
    {
        Assert.True(ClickHouseDynamic.Null.IsNull);
        Assert.Null(ClickHouseDynamic.Null.Value);
        Assert.Null(ClickHouseDynamic.Null.DeclaredTypeName);
    }

    [Fact]
    public void TryGetAs_Match_Mismatch_Null()
    {
        var d = new ClickHouseDynamic(0, 42, "Int32");
        Assert.True(d.TryGetAs<int>(out var i));
        Assert.Equal(42, i);
        Assert.False(d.TryGetAs<string>(out _));
        Assert.False(ClickHouseDynamic.Null.TryGetAs<int>(out _));
    }

    [Fact]
    public void Equals_HashCode_Operators()
    {
        var a = new ClickHouseDynamic(0, 1, "Int32");
        var b = new ClickHouseDynamic(0, 1, "Int32");
        var diffDisc = new ClickHouseDynamic(1, 1, "Int32");
        var diffType = new ClickHouseDynamic(0, 1, "Int64");
        var diffVal = new ClickHouseDynamic(0, 2, "Int32");

        Assert.True(a.Equals(b));
        Assert.True(a == b);
        Assert.False(a != b);
        Assert.False(a.Equals(diffDisc));
        Assert.False(a.Equals(diffType));
        Assert.False(a.Equals(diffVal));
        Assert.True(a.Equals((object)b));
        Assert.False(a.Equals("x"));
        Assert.False(new ClickHouseDynamic(0, null, "Int32").Equals(new ClickHouseDynamic(0, 1, "Int32")));
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    [Fact]
    public void ToString_NullAndValue()
    {
        Assert.Equal("NULL", ClickHouseDynamic.Null.ToString());
        Assert.Equal("Dynamic[Int32]=5", new ClickHouseDynamic(0, 5, "Int32").ToString());
    }
}
