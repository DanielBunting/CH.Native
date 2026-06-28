using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Cluster: composite column SKIPPING over the wire. The skipper scan pass
/// (<c>Block.TrySkipBlockColumns</c>) only runs for <b>uncompressed</b> data messages,
/// so these tests use a <c>Compress=false</c> connection to force every composite
/// column through its <c>IColumnSkipper</c> before the reader parses it. A misaligned
/// skipper throws a protocol exception and poisons the connection — the exact failure
/// mode of the Nested skipper bug, on a path that compressed tests never reach.
/// </summary>
[Collection("ClickHouse")]
public class CompositeColumnSkipIntegrationTests
{
    private readonly ClickHouseFixture _fixture;

    public CompositeColumnSkipIntegrationTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    // Force the uncompressed read path so the column-skipper scan pass actually runs.
    private string Uncompressed => _fixture.ConnectionString + ";Compress=false";

    private async Task SkipRoundTrip(
        string columnDdl,
        string insertValues,
        string selectExpr,
        int[] expectedTails,
        string? sessionSetup = null)
    {
        var table = $"skip_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(Uncompressed);
        await conn.OpenAsync();
        if (sessionSetup is not null)
            await conn.ExecuteNonQueryAsync(sessionSetup);

        await conn.ExecuteNonQueryAsync($"CREATE TABLE {table} (id Int32, {columnDdl}, tail Int32) ENGINE = Memory");
        try
        {
            await conn.ExecuteNonQueryAsync($"INSERT INTO {table} VALUES {insertValues}");

            // Reading the composite forces the scan-pass skipper over it (uncompressed),
            // then the reader parses it. The trailing scalar verifies stream alignment.
            var tails = new List<int>();
            await foreach (var row in conn.QueryStreamAsync(
                $"SELECT id, {selectExpr}, tail FROM {table} ORDER BY id"))
            {
                tails.Add(row.GetFieldValue<int>("tail"));
            }
            Assert.Equal(expectedTails, tails);

            // A misaligned skipper would have poisoned the connection.
            Assert.Equal(42, await conn.ExecuteScalarAsync<int>("SELECT 42"));
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public Task Skip_NestedColumn_Uncompressed() => SkipRoundTrip(
        "n Nested(key String, value Int32)",
        "(1, [('a',10),('b',20)], 7), (2, [], 8)",
        "n", new[] { 7, 8 }, sessionSetup: "SET flatten_nested = 0");

    [Fact]
    public Task Skip_MapColumn_Uncompressed() => SkipRoundTrip(
        "m Map(String, Int32)",
        "(1, {'a':1,'b':2}, 7), (2, {}, 8)",
        "m", new[] { 7, 8 });

    [Fact]
    public Task Skip_TupleColumn_Uncompressed() => SkipRoundTrip(
        "t Tuple(Int32, String)",
        "(1, (10, 'x'), 7), (2, (20, 'y'), 8)",
        "t", new[] { 7, 8 });

    [Fact]
    public Task Skip_ArrayOfTupleColumn_Uncompressed() => SkipRoundTrip(
        "a Array(Tuple(Int32, String))",
        "(1, [(1,'a'),(2,'b')], 7), (2, [], 8)",
        "a", new[] { 7, 8 });

    [Fact]
    public Task Skip_ArrayOfArrayColumn_Uncompressed() => SkipRoundTrip(
        "a Array(Array(Int32))",
        "(1, [[1,2],[3]], 7), (2, [], 8)",
        "a", new[] { 7, 8 });

    [Fact]
    public Task Skip_LowCardinalityColumn_Uncompressed() => SkipRoundTrip(
        "lc LowCardinality(String)",
        "(1, 'red', 7), (2, 'blue', 8)",
        "lc", new[] { 7, 8 });

    [Fact]
    public Task Skip_MapOfArrayColumn_Uncompressed() => SkipRoundTrip(
        "m Map(String, Array(Int32))",
        "(1, {'a':[1,2],'b':[3]}, 7), (2, {}, 8)",
        "m", new[] { 7, 8 });
}
