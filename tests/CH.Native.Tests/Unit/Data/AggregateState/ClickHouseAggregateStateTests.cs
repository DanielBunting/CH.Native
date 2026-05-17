using CH.Native.Data.AggregateState;
using Xunit;

namespace CH.Native.Tests.Unit.Data.AggregateState;

public class ClickHouseAggregateStateTests
{
    [Fact]
    public void Empty_HasEmptyBytesAndName()
    {
        Assert.Empty(ClickHouseAggregateState.Empty.State);
        Assert.Equal(string.Empty, ClickHouseAggregateState.Empty.FunctionName);
    }

    [Fact]
    public void Equality_IdenticalBytesAndName_AreEqual()
    {
        var a = new ClickHouseAggregateState(new byte[] { 1, 2, 3 }, "sum");
        var b = new ClickHouseAggregateState(new byte[] { 1, 2, 3 }, "sum");

        Assert.Equal(a, b);
        Assert.True(a.Equals(b));
        Assert.True(a.Equals((object)b));
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    [Fact]
    public void Equality_DifferentBytes_AreNotEqual()
    {
        var a = new ClickHouseAggregateState(new byte[] { 1, 2, 3 }, "sum");
        var b = new ClickHouseAggregateState(new byte[] { 1, 2, 4 }, "sum");

        Assert.NotEqual(a, b);
    }

    [Fact]
    public void Equality_DifferentFunctionName_AreNotEqual()
    {
        var a = new ClickHouseAggregateState(new byte[] { 1, 2, 3 }, "sum");
        var b = new ClickHouseAggregateState(new byte[] { 1, 2, 3 }, "min");

        Assert.NotEqual(a, b);
    }

    [Fact]
    public void Equality_DifferentLength_AreNotEqual()
    {
        var a = new ClickHouseAggregateState(new byte[] { 1, 2, 3 }, "sum");
        var b = new ClickHouseAggregateState(new byte[] { 1, 2, 3, 0 }, "sum");

        Assert.NotEqual(a, b);
    }

    [Fact]
    public void Equality_AgainstNull_IsFalse()
    {
        var a = new ClickHouseAggregateState(new byte[] { 1 }, "sum");
        Assert.False(a.Equals(null));
        Assert.False(a.Equals((object?)null));
    }

    [Fact]
    public void Ctor_RejectsNullState()
    {
        Assert.Throws<ArgumentNullException>(() => new ClickHouseAggregateState(null!, "sum"));
    }

    [Fact]
    public void Ctor_RejectsNullFunctionName()
    {
        Assert.Throws<ArgumentNullException>(() => new ClickHouseAggregateState(Array.Empty<byte>(), null!));
    }

    [Fact]
    public void HashCode_DependsOnBytesAndName()
    {
        // Different bytes → different hash (in practice; not a strict requirement, but byte-wise hashing should make this collisions rare).
        var a = new ClickHouseAggregateState(new byte[] { 1, 2, 3 }, "sum");
        var b = new ClickHouseAggregateState(new byte[] { 4, 5, 6 }, "sum");
        Assert.NotEqual(a.GetHashCode(), b.GetHashCode());
    }
}
