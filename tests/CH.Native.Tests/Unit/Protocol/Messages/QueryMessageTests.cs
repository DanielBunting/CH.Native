using System.Text.RegularExpressions;
using CH.Native.Protocol.Messages;
using Xunit;

namespace CH.Native.Tests.Unit.Protocol.Messages;

public class QueryMessageTests
{
    private const int ProtocolRevision = 54467;

    [Fact]
    public void Create_WithCallerSuppliedId_UsesThatId()
    {
        var msg = QueryMessage.Create(
            "SELECT 1",
            "TestClient",
            "default",
            ProtocolRevision,
            queryId: "custom-id-42");

        Assert.Equal("custom-id-42", msg.QueryId);
    }

    [Fact]
    public void Create_WithNullId_GeneratesGuidInDFormat()
    {
        var msg = QueryMessage.Create(
            "SELECT 1",
            "TestClient",
            "default",
            ProtocolRevision,
            queryId: null);

        Assert.Matches(
            new Regex(@"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"),
            msg.QueryId);
    }

    [Fact]
    public void Create_WithEmptyString_TreatsAsNullAndGenerates()
    {
        var msg = QueryMessage.Create(
            "SELECT 1",
            "TestClient",
            "default",
            ProtocolRevision,
            queryId: "");

        Assert.NotEmpty(msg.QueryId);
        Assert.Matches(
            new Regex(@"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"),
            msg.QueryId);
    }

    [Fact]
    public void Create_UnchangedWhenQueryIdOmitted_StillGenerates()
    {
        var msg = QueryMessage.Create(
            "SELECT 1",
            "TestClient",
            "default",
            ProtocolRevision);

        Assert.Matches(
            new Regex(@"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"),
            msg.QueryId);
    }

    [Fact]
    public void Create_ClientInfoQueryIdMatchesOuter()
    {
        var msg = QueryMessage.Create(
            "SELECT 1",
            "TestClient",
            "default",
            ProtocolRevision,
            queryId: "same-everywhere");

        Assert.Equal(msg.QueryId, msg.ClientInfo.InitialQueryId);
    }
}
