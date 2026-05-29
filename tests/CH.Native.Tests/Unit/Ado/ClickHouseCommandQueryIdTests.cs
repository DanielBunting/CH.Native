using CH.Native.Ado;
using CH.Native.Connection;
using CH.Native.Commands;
using CH.Native.Results;
using Xunit;

namespace CH.Native.Tests.Unit.Ado;

public class ClickHouseCommandQueryIdTests
{
    [Fact]
    public void QueryId_DefaultsToNull()
    {
        var cmd = new ClickHouseCommand();
        Assert.Null(cmd.QueryId);
    }

    [Fact]
    public void QueryId_SetAndGet_RoundTrips()
    {
        var cmd = new ClickHouseCommand { QueryId = "my-id" };
        Assert.Equal("my-id", cmd.QueryId);
    }

    [Fact]
    public void QueryId_CanBeAssignedEmpty()
    {
        var cmd = new ClickHouseCommand { QueryId = "" };
        Assert.Equal("", cmd.QueryId);
    }
}
