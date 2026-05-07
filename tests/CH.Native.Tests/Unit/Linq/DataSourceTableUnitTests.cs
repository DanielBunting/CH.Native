using CH.Native.Connection;
using CH.Native.Linq;
using Xunit;

namespace CH.Native.Tests.Unit.Linq;

/// <summary>
/// Argument-validation and disposal-state tests for
/// <see cref="ClickHouseDataSource.Table{T}()"/> and the explicit-name overload.
/// These don't open any wire connections — they only exercise the entry-point
/// guards that fire synchronously before any rent.
/// </summary>
public class DataSourceTableUnitTests
{
    private const string AnyConnectionString = "Host=localhost;Port=9000";

    [Fact]
    public async Task Table_ExplicitName_NullTableName_Throws()
    {
        await using var dataSource = new ClickHouseDataSource(AnyConnectionString);
        Assert.Throws<ArgumentNullException>(() => dataSource.Table<TestRow>(null!));
    }

    [Fact]
    public async Task Table_ExplicitName_EmptyTableName_Throws()
    {
        await using var dataSource = new ClickHouseDataSource(AnyConnectionString);
        Assert.Throws<ArgumentException>(() => dataSource.Table<TestRow>(string.Empty));
    }

    [Fact]
    public async Task Table_ExplicitName_WhitespaceTableName_Throws()
    {
        await using var dataSource = new ClickHouseDataSource(AnyConnectionString);
        Assert.Throws<ArgumentException>(() => dataSource.Table<TestRow>("   "));
    }

    [Fact]
    public async Task Table_AfterDispose_Throws()
    {
        var dataSource = new ClickHouseDataSource(AnyConnectionString);
        await dataSource.DisposeAsync();

        Assert.Throws<ObjectDisposedException>(() => dataSource.Table<TestRow>());
        Assert.Throws<ObjectDisposedException>(() => dataSource.Table<TestRow>("any"));
    }

    [Fact]
    public async Task Table_ExplicitName_ReturnsQueryableBoundToDataSource()
    {
        // Pin that the queryable carries the data-source binding through its
        // provider context — this is the load-bearing wiring that makes the
        // rent-per-enumeration semantics work later. Connection must be null
        // (the rent happens at execution time, not construction time).
        await using var dataSource = new ClickHouseDataSource(AnyConnectionString);
        var queryable = dataSource.Table<TestRow>("some_table");

        Assert.NotNull(queryable);
        var provider = Assert.IsType<ClickHouseQueryProvider>(queryable.Provider);
        Assert.Null(provider.Context.Connection);
        Assert.Same(dataSource, provider.Context.DataSource);
        Assert.Equal("some_table", provider.Context.TableName);
    }

    [Fact]
    public async Task Table_ResolvedName_ResolvesViaTableNameResolver()
    {
        // The parameterless overload uses TableNameResolver.Resolve<T>(), which
        // snake_cases the type name. Pin the contract so a future resolver
        // change (e.g. an attribute-aware resolver) is a deliberate decision.
        await using var dataSource = new ClickHouseDataSource(AnyConnectionString);
        var queryable = dataSource.Table<TestRow>();

        var provider = Assert.IsType<ClickHouseQueryProvider>(queryable.Provider);
        Assert.Equal("test_row", provider.Context.TableName);
    }

    private sealed class TestRow
    {
        public int Id { get; set; }
    }
}
