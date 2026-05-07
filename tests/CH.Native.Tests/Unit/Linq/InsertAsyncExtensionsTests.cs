using CH.Native.Linq;
using Xunit;

namespace CH.Native.Tests.Unit.Linq;

/// <summary>
/// Argument-validation tests for <see cref="ClickHouseQueryableExtensions.InsertAsync{T}"/>.
/// End-to-end round-trip behaviour is exercised by the integration suite
/// (<c>QueryableInsertTests</c>); these only cover the synchronous-throw paths
/// that don't require a server.
/// </summary>
public class InsertAsyncExtensionsTests
{
    [Fact]
    public async Task InsertAsync_NullRow_Throws()
    {
        var queryable = NonExecutingQueryable.Create<TestRow>();
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => queryable.InsertAsync((TestRow)null!));
    }

    [Fact]
    public async Task InsertAsync_NullEnumerable_Throws()
    {
        var queryable = NonExecutingQueryable.Create<TestRow>();
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => queryable.InsertAsync((IEnumerable<TestRow>)null!));
    }

    [Fact]
    public async Task InsertAsync_NullAsyncEnumerable_Throws()
    {
        var queryable = NonExecutingQueryable.Create<TestRow>();
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => queryable.InsertAsync((IAsyncEnumerable<TestRow>)null!));
    }

    [Fact]
    public async Task InsertAsync_NonClickHouseQueryable_Throws()
    {
        // A vanilla in-memory IQueryable<T> — its provider is not ClickHouseQueryProvider,
        // so InsertAsync must reject loudly rather than silently no-op.
        var foreign = new[] { new TestRow() }.AsQueryable();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => foreign.InsertAsync(new TestRow()));
        Assert.Contains("ClickHouse", ex.Message);
    }

    [Fact]
    public async Task InsertAsync_QueryableWithoutConnectionOrDataSource_Throws()
    {
        // Synthesise a SQL-generation-only context (Connection=null, DataSource=null) —
        // attempting to execute an Insert on it should fail with a clear message
        // rather than NRE inside BulkInsertAsync.
        var queryable = NonExecutingQueryable.Create<TestRow>();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => queryable.InsertAsync(new TestRow()));
        Assert.Contains("Connection or DataSource", ex.Message);
    }

    private sealed class TestRow
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private static class NonExecutingQueryable
    {
        public static IQueryable<T> Create<T>() where T : class
        {
            // Connection-less context — only valid for ToSql / argument-validation paths.
            var context = new ClickHouseQueryContext(connection: null, "test_table", typeof(T), columnNames: null);
            return new ClickHouseQueryable<T>(context);
        }
    }
}
