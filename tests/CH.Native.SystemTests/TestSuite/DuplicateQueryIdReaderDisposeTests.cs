using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.TestSuite;

/// <summary>
/// Repro for wire-state-machine review finding #4: <c>ExitBusyResolve</c>'s owner
/// gate is query-id STRING equality. Caller-supplied query ids may legally be
/// reused across sequential queries (<c>ResolveQueryId</c> only length-checks), so
/// a completed-but-undisposed reader's dispose safety-net matches the SUCCESSOR
/// query's owner id: it releases the successor's busy slot mid-flight and convicts
/// its in-flight evidence (<c>_conversationWrote &amp;&amp; !_boundaryProven</c>),
/// poisoning a healthy connection.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public sealed class DuplicateQueryIdReaderDisposeTests
{
    private readonly SingleNodeFixture _fixture;

    public DuplicateQueryIdReaderDisposeTests(SingleNodeFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task DisposingCompletedReader_WithReusedQueryId_DoesNotPoisonSuccessor()
    {
        const string reusedId = "duplicate-query-id-repro";

        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        // Query 1: read to natural completion — EOS releases the busy slot via
        // MarkCompleted — but do NOT dispose yet (legal: the await-using scope
        // is still open while the app starts its next query).
        var r1 = await conn.ExecuteReaderAsync("SELECT 1", queryId: reusedId);
        while (await r1.ReadAsync()) { }

        // Query 2: same caller-supplied id, in flight (nothing consumed yet, so
        // no response terminator has been read for this conversation).
        var r2 = await conn.ExecuteReaderAsync(
            "SELECT number FROM numbers(1000)", queryId: reusedId);

        // Disposing the COMPLETED first reader must be a no-op for the
        // successor's conversation.
        await r1.DisposeAsync();

        long rows = 0;
        while (await r2.ReadAsync())
            rows++;
        await r2.DisposeAsync();
        Assert.Equal(1000, rows);

        // The connection must still be healthy: r1's dispose must not have
        // released r2's busy slot or convicted its mid-flight evidence.
        Assert.Equal(4242, await conn.ExecuteScalarAsync<int>("SELECT 4242"));
    }
}
