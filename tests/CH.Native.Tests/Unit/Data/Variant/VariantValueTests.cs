using CH.Native.Data.Variant;
using Xunit;

namespace CH.Native.Tests.Unit.Data.Variant;

public class VariantValueTests
{
    [Fact]
    public void FromArm0_And_Accessors()
    {
        var v = VariantValue<int, string>.FromArm0(42);
        Assert.Equal(0, v.Discriminator);
        Assert.False(v.IsNull);
        Assert.Equal(42, v.Arm0);
        Assert.True(v.TryGetArm0(out var a0));
        Assert.Equal(42, a0);
        Assert.False(v.TryGetArm1(out _));
        Assert.Throws<InvalidOperationException>(() => v.Arm1);
    }

    [Fact]
    public void FromArm1_And_Accessors()
    {
        var v = VariantValue<int, string>.FromArm1("hi");
        Assert.Equal(1, v.Discriminator);
        Assert.Equal("hi", v.Arm1);
        Assert.True(v.TryGetArm1(out var a1));
        Assert.Equal("hi", a1);
        Assert.False(v.TryGetArm0(out _));
        Assert.Throws<InvalidOperationException>(() => v.Arm0);
    }

    [Fact]
    public void Null_IsNull()
    {
        var v = VariantValue<int, string>.Null;
        Assert.True(v.IsNull);
        Assert.Equal(VariantValue<int, string>.NullDiscriminator, v.Discriminator);
    }

    [Fact]
    public void ToBoxed_EachArm_And_Null_And_Invalid()
    {
        Assert.Equal(0, VariantValue<int, string>.FromArm0(7).ToBoxed().Discriminator);
        Assert.Equal(7, VariantValue<int, string>.FromArm0(7).ToBoxed().Value);
        Assert.Equal("z", VariantValue<int, string>.FromArm1("z").ToBoxed().Value);
        Assert.True(VariantValue<int, string>.Null.ToBoxed().IsNull);
        Assert.Throws<InvalidOperationException>(() => new VariantValue<int, string>(5).ToBoxed());
    }

    [Fact]
    public void Equals_And_HashCode()
    {
        Assert.True(VariantValue<int, string>.FromArm0(1).Equals(VariantValue<int, string>.FromArm0(1)));
        Assert.False(VariantValue<int, string>.FromArm0(1).Equals(VariantValue<int, string>.FromArm0(2)));
        Assert.False(VariantValue<int, string>.FromArm0(1).Equals(VariantValue<int, string>.FromArm1("1")));
        Assert.True(VariantValue<int, string>.FromArm1("x").Equals(VariantValue<int, string>.FromArm1("x")));
        Assert.True(VariantValue<int, string>.Null.Equals(VariantValue<int, string>.Null));

        Assert.True(VariantValue<int, string>.FromArm0(1).Equals((object)VariantValue<int, string>.FromArm0(1)));
        Assert.False(VariantValue<int, string>.FromArm0(1).Equals("not a variant"));

        Assert.Equal(
            VariantValue<int, string>.FromArm0(5).GetHashCode(),
            VariantValue<int, string>.FromArm0(5).GetHashCode());
        Assert.Equal(0, VariantValue<int, string>.Null.GetHashCode());
        _ = VariantValue<int, string>.FromArm1("y").GetHashCode();
    }
}
