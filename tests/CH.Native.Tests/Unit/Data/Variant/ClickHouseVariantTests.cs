using CH.Native.Data.Variant;
using Xunit;

namespace CH.Native.Tests.Unit.Data.Variant;

public class ClickHouseVariantTests
{
    [Fact]
    public void Of_And_Properties()
    {
        var v = ClickHouseVariant.Of(1, "hi");
        Assert.Equal(1, v.Discriminator);
        Assert.Equal("hi", v.Value);
        Assert.False(v.IsNull);
    }

    [Fact]
    public void Null_IsNull()
    {
        Assert.True(ClickHouseVariant.Null.IsNull);
        Assert.Null(ClickHouseVariant.Null.Value);
    }

    [Fact]
    public void TryGetAs_Match_Mismatch_Null()
    {
        var v = new ClickHouseVariant(0, 42);
        Assert.True(v.TryGetAs<int>(out var i));
        Assert.Equal(42, i);
        Assert.False(v.TryGetAs<string>(out _));       // type mismatch
        Assert.False(ClickHouseVariant.Null.TryGetAs<int>(out _)); // null
    }

    [Fact]
    public void Equals_HashCode_Operators()
    {
        var a = new ClickHouseVariant(0, 1);
        var b = new ClickHouseVariant(0, 1);
        var c = new ClickHouseVariant(1, 1);   // different discriminator
        var d = new ClickHouseVariant(0, 2);   // different value

        Assert.True(a.Equals(b));
        Assert.True(a == b);
        Assert.False(a != b);
        Assert.False(a.Equals(c));
        Assert.False(a.Equals(d));
        Assert.True(a.Equals((object)b));
        Assert.False(a.Equals("x"));
        Assert.True(ClickHouseVariant.Null.Equals(new ClickHouseVariant(255, null)));
        Assert.False(new ClickHouseVariant(0, null).Equals(new ClickHouseVariant(0, 1)));
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    [Fact]
    public void ToString_NullAndValue()
    {
        Assert.Equal("NULL", ClickHouseVariant.Null.ToString());
        Assert.Equal("Variant[0]=5", new ClickHouseVariant(0, 5).ToString());
    }
}
