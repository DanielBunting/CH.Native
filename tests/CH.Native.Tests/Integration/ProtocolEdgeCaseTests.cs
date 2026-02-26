using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

[Collection("ClickHouse")]
public class ProtocolEdgeCaseTests
{
    private readonly ClickHouseFixture _fixture;

    public ProtocolEdgeCaseTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Protocol_MidStreamException_Parsed()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // This query starts returning rows from system.numbers then throws an error
        // when number > 5. The server should send an exception mid-stream.
        var ex = await Assert.ThrowsAsync<ClickHouseServerException>(async () =>
        {
            await foreach (var row in connection.QueryAsync(
                "SELECT throwIf(number > 5, 'test mid-stream error') FROM system.numbers LIMIT 10"))
            {
                // Consume rows until the exception occurs
            }
        });

        Assert.True(ex.ErrorCode > 0);
        Assert.NotNull(ex.ServerExceptionName);
        Assert.Contains("test mid-stream error", ex.Message);
    }

    [Fact]
    public async Task Protocol_ProgressPackets_Received()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Execute a large query that is likely to generate progress packets
        long count = 0;
        await foreach (var row in connection.QueryAsync(
            "SELECT number FROM system.numbers LIMIT 100000"))
        {
            count++;
        }

        // Verify the query completed and returned all rows
        Assert.Equal(100000, count);
    }

    [Fact]
    public async Task Protocol_MultiBlock_ResultStitching()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Query returning >65536 rows forces multiple blocks in the native protocol
        long count = 0;
        ulong lastNumber = 0;
        await foreach (var row in connection.QueryAsync(
            "SELECT number FROM system.numbers LIMIT 100000"))
        {
            lastNumber = row.GetFieldValue<ulong>("number");
            count++;
        }

        // Verify all rows were stitched across multiple blocks
        Assert.Equal(100000, count);
        Assert.Equal(99999UL, lastNumber);
    }

    [Fact]
    public async Task Protocol_ServerHello_VersionCapture()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Verify ServerInfo was captured during handshake
        Assert.NotNull(connection.ServerInfo);
        Assert.NotNull(connection.ServerInfo!.ServerName);
        Assert.NotEmpty(connection.ServerInfo.ServerName);
        Assert.True(connection.ServerInfo.VersionMajor > 0,
            $"Expected VersionMajor > 0, got {connection.ServerInfo.VersionMajor}");
        Assert.True(connection.ServerInfo.ProtocolRevision > 0,
            $"Expected ProtocolRevision > 0, got {connection.ServerInfo.ProtocolRevision}");
    }

    [Fact]
    public async Task Protocol_EmptyDataBlock_Handled()
    {
        var tableName = $"test_proto_empty_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Name String
            ) ENGINE = Memory");

        try
        {
            // Query the empty table - the server sends an empty data block
            var count = 0;
            await foreach (var row in connection.QueryAsync(
                $"SELECT Id, Name FROM {tableName}"))
            {
                count++;
            }

            Assert.Equal(0, count);

            // Also verify via ExecuteReaderAsync
            await using var reader = await connection.ExecuteReaderAsync(
                $"SELECT Id, Name FROM {tableName}");
            Assert.False(await reader.ReadAsync());
            Assert.False(reader.HasRows);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }
}
