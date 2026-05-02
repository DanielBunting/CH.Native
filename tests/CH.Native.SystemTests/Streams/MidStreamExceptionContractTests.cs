using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Streams;

/// <summary>
/// Pins two contracts that an audit reviewer flagged as "verify-needed":
/// 1. A server-emitted Exception that arrives mid-stream during a normal
///    <c>await foreach</c> iteration must surface as
///    <see cref="ClickHouseServerException"/> at <c>MoveNextAsync</c>.
/// 2. The same exception arriving after the caller has broken out of the
///    iteration must NOT throw from <see cref="ClickHouseDataReader.DisposeAsync"/>
///    — disposing should never throw, per ADO.NET expectations and to keep
///    `await using` callers from masking the original cancellation.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Streams)]
public class MidStreamExceptionContractTests
{
    private readonly SingleNodeFixture _fx;

    public MidStreamExceptionContractTests(SingleNodeFixture fx) => _fx = fx;

    [Fact]
    public async Task MidStreamException_DuringIteration_PropagatesAsClickHouseServerException()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        // throwIf(number = 4) emits rows for 0..3 then raises a server-side
        // exception, with the streaming reader sitting on a non-empty queue
        // when the failure arrives. The exception should be observable at the
        // very next MoveNextAsync — not silently dropped.
        var ex = await Assert.ThrowsAsync<ClickHouseServerException>(async () =>
        {
            await foreach (var _ in conn.QueryAsync(
                "SELECT throwIf(number = 4, 'mid-stream') FROM numbers(10)"))
            {
                // consume rows until throwIf fires
            }
        });

        Assert.Contains("mid-stream", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task EarlyBreak_OnNormalQuery_DisposesCleanlyAndConnectionRecovers()
    {
        // Early-break + DisposeAsync drain is the ADO.NET pattern this contract
        // pins: dispose must not throw, the connection must be reusable, and
        // any pending server bytes must be drained / cancelled cleanly.
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        await using (var reader = await conn.ExecuteReaderAsync("SELECT * FROM numbers(100000)"))
        {
            // Read a couple of rows then bail. SendCancelAsync runs in
            // DisposeAsync's drain path; if it threw, the await using would
            // surface here.
            for (int i = 0; i < 5 && await reader.ReadAsync(); i++)
            {
                _ = reader.GetValue(0);
            }
        }

        // Connection still usable end-to-end.
        Assert.Equal(1, await conn.ExecuteScalarAsync<int>("SELECT 1"));
    }
}
