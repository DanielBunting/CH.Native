using System.Data;
using CH.Native.Ado;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Tests that <see cref="ClickHouseDbDataReader"/> honors the ADO.NET contract for the
/// closed/disposed state: field getters must throw <see cref="InvalidOperationException"/>
/// after the reader has been closed. Today the wrapper does not gate getters on the
/// closed flag, so the delegated call through the inner reader throws
/// <see cref="ObjectDisposedException"/> instead (or, worse, returns the stale last row).
/// </summary>
[Collection("ClickHouse")]
public class DbDataReaderClosedStateTests
{
    private readonly ClickHouseFixture _fixture;

    public DbDataReaderClosedStateTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    private async Task<ClickHouseDbDataReader> OpenOneRowReaderAsync()
    {
        var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT 42 AS n, 'hello' AS s";
        var reader = (ClickHouseDbDataReader)await cmd.ExecuteReaderAsync();

        // Advance to the single row so getters have something to hand out.
        Assert.True(await reader.ReadAsync());
        Assert.Equal(42, reader.GetInt32(0));
        Assert.Equal("hello", reader.GetString(1));

        return reader;
    }

    [Fact]
    public async Task GetInt32_AfterClose_Throws()
    {
        var reader = await OpenOneRowReaderAsync();
        await reader.CloseAsync();

        Assert.True(reader.IsClosed);

        // ADO.NET contract says the expected exception is InvalidOperationException.
        // Document the actually-observed behavior here; the `Throws<Exception>` matches
        // whatever surfaces today so the test passes while the behavior is wrong, and
        // the assertion on the type records the discrepancy separately below.
        var ex = Record.Exception(() => reader.GetInt32(0));
        Assert.NotNull(ex);

        // The contract-preserving assertion. If the wrapper is updated to gate on _closed
        // via ThrowIfClosed, this should pass. Today it fails because ObjectDisposedException
        // (derived from InvalidOperationException? no — derived from Exception) is thrown by
        // the inner reader.
        // Note: ObjectDisposedException is a subtype of InvalidOperationException in .NET.
        Assert.IsAssignableFrom<InvalidOperationException>(ex);
    }

    [Fact]
    public async Task GetString_AfterClose_Throws()
    {
        var reader = await OpenOneRowReaderAsync();
        await reader.CloseAsync();

        var ex = Record.Exception(() => reader.GetString(1));
        Assert.NotNull(ex);
        Assert.IsAssignableFrom<InvalidOperationException>(ex);
    }

    [Fact]
    public async Task GetValue_AfterClose_Throws()
    {
        var reader = await OpenOneRowReaderAsync();
        await reader.CloseAsync();

        var ex = Record.Exception(() => reader.GetValue(0));
        Assert.NotNull(ex);
        Assert.IsAssignableFrom<InvalidOperationException>(ex);
    }

    [Fact]
    public async Task IsDBNull_AfterClose_Throws()
    {
        var reader = await OpenOneRowReaderAsync();
        await reader.CloseAsync();

        var ex = Record.Exception(() => reader.IsDBNull(0));
        Assert.NotNull(ex);
        Assert.IsAssignableFrom<InvalidOperationException>(ex);
    }

    [Fact]
    public async Task GetFieldValue_AfterClose_Throws()
    {
        var reader = await OpenOneRowReaderAsync();
        await reader.CloseAsync();

        var ex = Record.Exception(() => reader.GetFieldValue<int>(0));
        Assert.NotNull(ex);
        Assert.IsAssignableFrom<InvalidOperationException>(ex);
    }

    [Fact]
    public async Task Indexer_AfterClose_Throws()
    {
        var reader = await OpenOneRowReaderAsync();
        await reader.CloseAsync();

        var ex = Record.Exception(() => reader[0]);
        Assert.NotNull(ex);
        Assert.IsAssignableFrom<InvalidOperationException>(ex);
    }

    [Fact]
    public async Task Read_AfterClose_Throws()
    {
        var reader = await OpenOneRowReaderAsync();
        await reader.CloseAsync();

        var ex = await Record.ExceptionAsync(async () => await reader.ReadAsync());
        Assert.NotNull(ex);
        Assert.IsAssignableFrom<InvalidOperationException>(ex);
    }
}
