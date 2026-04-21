using CH.Native.Ado;
using Xunit;

namespace CH.Native.Tests.Unit.Ado;

public class ClickHouseDbCommandQueryIdTests
{
    [Fact]
    public void QueryId_DefaultsToNull()
    {
        var cmd = new ClickHouseDbCommand();
        Assert.Null(cmd.QueryId);
    }

    [Fact]
    public void QueryId_SetAndGet_RoundTrips()
    {
        var cmd = new ClickHouseDbCommand { QueryId = "my-id" };
        Assert.Equal("my-id", cmd.QueryId);
    }

    [Fact]
    public void QueryId_CanBeAssignedEmpty()
    {
        var cmd = new ClickHouseDbCommand { QueryId = "" };
        Assert.Equal("", cmd.QueryId);
    }
}
