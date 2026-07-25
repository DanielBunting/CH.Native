using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using CH.Native.SystemTests.Helpers;
using Xunit;

namespace CH.Native.SystemTests.Streams;

/// <summary>
/// Repro for wire-state-machine review finding #1: the count-vs-Remaining guard
/// in <c>Block</c> throws <c>ClickHouseProtocolException</c> inside the compressed
/// multi-chunk accumulate loop, whose "need more decompressed data" retry contract
/// is <c>catch (InvalidOperationException)</c>. On the first accumulate pass
/// <c>reader.Remaining</c> is only the decompressed-so-far bytes (one ~1MB chunk),
/// so a legitimate block whose minimum footprint (rowCount × columnCount bytes)
/// exceeds one chunk is rejected as "malformed or hostile" and the connection is
/// condemned via the protocol-fatal path.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Streams)]
public sealed class CompressedMultiChunkBlockTests
{
    private readonly SingleNodeFixture _fixture;

    public CompressedMultiChunkBlockTests(SingleNodeFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task CompressedBlock_SpanningMultipleChunks_IsReadable()
    {
        await using var conn = new ClickHouseConnection(
            _fixture.BuildSettings(b => b.WithCompression()));
        await conn.OpenAsync();

        // One 4M-row single-column block: 32 MB raw, sent as ~32 compressed
        // chunks of ≤1MB decompressed each. First parse attempt sees
        // Remaining ≈ 1MB while rowCount × minBytesPerItem(=columnCount=1)
        // = 4,000,000 — a well-formed block must survive that pass and be
        // completed from the remaining chunks.
        long rows = 0;
        await using (var reader = await conn.ExecuteReaderAsync(
            "SELECT number FROM numbers(4000000) SETTINGS max_block_size = 4000000"))
        {
            while (await reader.ReadAsync())
                rows++;
        }

        Assert.Equal(4_000_000, rows);
        WireAssertions.AssertWireIdle(conn);
    }
}
