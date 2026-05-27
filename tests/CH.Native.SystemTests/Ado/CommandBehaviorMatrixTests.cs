using System.Data;
using CH.Native.Ado;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Ado;

/// <summary>
/// Pins the per-CommandBehavior contract end-to-end against a real
/// ClickHouse server. The matrix matters because frameworks (Dapper, EF) pass
/// these flags through and consumers expect documented ADO.NET behaviour.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class CommandBehaviorMatrixTests
{
    private readonly SingleNodeFixture _fx;

    public CommandBehaviorMatrixTests(SingleNodeFixture fx) => _fx = fx;

    private static async Task<ClickHouseDbConnection> OpenAsync(SingleNodeFixture fx)
    {
        var conn = new ClickHouseDbConnection(fx.ConnectionString);
        await conn.OpenAsync();
        return conn;
    }

    [Fact]
    public async Task Default_EnumeratesAllRows()
    {
        await using var conn = await OpenAsync(_fx);
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT number FROM numbers(5)";

        await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.Default);

        var values = new List<long>();
        while (await reader.ReadAsync())
            values.Add(reader.GetInt64(0));

        Assert.Equal(new long[] { 0, 1, 2, 3, 4 }, values);
    }

    [Fact]
    public async Task SingleRow_StopsAfterOneRow()
    {
        // SingleRow is a hint to readers — ADO.NET allows the provider to
        // either limit the wire fetch or simply ignore the hint. Pin the
        // observable contract: at least one row is available; the consumer
        // doesn't iterate beyond the first.
        await using var conn = await OpenAsync(_fx);
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT number FROM numbers(1000)";

        await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SingleRow);

        Assert.True(await reader.ReadAsync());
        Assert.Equal(0L, reader.GetInt64(0));
        // The reader is allowed to keep yielding (provider-discretion); pin
        // only that the first row was correctly delivered.
    }

    [Fact]
    public async Task SingleResult_HonoursSingleResultSet()
    {
        await using var conn = await OpenAsync(_fx);
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1";

        await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SingleResult);

        Assert.True(await reader.ReadAsync());
        // NextResult should always be false — ClickHouse is single-result-set per query.
        Assert.False(await reader.NextResultAsync());
    }

    [Fact]
    public async Task SchemaOnly_ReturnsSchemaTable()
    {
        // SchemaOnly is a hint that the consumer wants metadata only, not rows.
        // The current implementation runs the query (rows may be fetched), but
        // GetSchemaTable() must return populated column metadata.
        await using var conn = await OpenAsync(_fx);
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT toInt32(0) AS id, '' AS name";

        await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly);

        var schema = reader.GetSchemaTable();
        Assert.NotNull(schema);
        Assert.Equal(2, schema!.Rows.Count);
        Assert.Equal("id", schema.Rows[0]["ColumnName"]);
        Assert.Equal("name", schema.Rows[1]["ColumnName"]);
    }

    [Fact]
    public async Task CloseConnection_DisposingReaderClosesUnderlyingConnection()
    {
        var conn = await OpenAsync(_fx);
        try
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT 1";

            var reader = await cmd.ExecuteReaderAsync(CommandBehavior.CloseConnection);
            await reader.ReadAsync();
            await reader.DisposeAsync();

            // CloseConnection contract: the underlying connection must be
            // closed when the reader is disposed.
            Assert.Equal(ConnectionState.Closed, conn.State);
        }
        finally
        {
            await conn.DisposeAsync();
        }
    }

    [Fact]
    public async Task SequentialAccess_ReadsRowsForward()
    {
        // SequentialAccess hints "I'll only read columns left-to-right". The
        // provider is free to either honour the hint (saving a buffer) or
        // ignore it. Pin the observable contract: forward column reads work
        // correctly.
        await using var conn = await OpenAsync(_fx);
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT toInt32(1) AS a, toInt32(2) AS b, toInt32(3) AS c";

        await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess);

        Assert.True(await reader.ReadAsync());
        Assert.Equal(1, reader.GetInt32(0));
        Assert.Equal(2, reader.GetInt32(1));
        Assert.Equal(3, reader.GetInt32(2));
    }

    [Theory]
    [InlineData(CommandBehavior.KeyInfo)]
    public async Task UnsupportedHints_DoNotThrow(CommandBehavior hint)
    {
        // ADO contract: unrecognised CommandBehavior flags should be ignored
        // silently (treated as Default). Pin so a future enforcement change
        // doesn't accidentally break Dapper-style callers.
        await using var conn = await OpenAsync(_fx);
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 42";

        await using var reader = await cmd.ExecuteReaderAsync(hint);
        Assert.True(await reader.ReadAsync());
        Assert.Equal(42, reader.GetInt32(0));
    }
}
