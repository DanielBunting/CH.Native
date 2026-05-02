using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Linq;

/// <summary>
/// Pins the snake_case inference rule used by <see cref="ClickHouseQueryableExtensions.Table{T}(ClickHouseConnection)"/>
/// when no explicit table name is provided. Every <c>samples/</c> example uses
/// the explicit-name overload, so a regression in the inference path would be
/// invisible without dedicated coverage. The acronym/digit edge cases lock in
/// the current resolver output as the contract.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Linq)]
public class TableNameInferenceTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public TableNameInferenceTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public void Resolve_SinglePascalWord_LowercasesToOneToken()
    {
        Assert.Equal("product", TableNameResolver.Resolve<Product>());
    }

    [Fact]
    public void Resolve_TwoPascalWords_JoinsWithUnderscore()
    {
        Assert.Equal("log_entry", TableNameResolver.Resolve<LogEntry>());
        Assert.Equal("user_account", TableNameResolver.Resolve<UserAccount>());
    }

    [Theory]
    // Locks in current behaviour: a digit boundary does NOT trigger an
    // underscore (because the resolver only checks IsLower / IsUpper, and
    // a digit is neither). This is unintuitive — pin it explicitly.
    [InlineData("IPv4Address", "i_pv4address")]
    [InlineData("XMLParser", "xml_parser")]
    [InlineData("HTTPRequest", "http_request")]
    [InlineData("ID", "id")]
    [InlineData("OrderID", "order_id")]
    [InlineData("OrderId", "order_id")]
    public void Resolve_AcronymEdgeCases_LocksInCurrentBehaviour(string typeName, string expected)
    {
        // The resolver is a pure ToSnakeCase over the type name. Lock the
        // outputs the current implementation produces; if the rule changes,
        // every caller that relied on a specific inferred name will break,
        // and these test names will surface the new outputs.
        var actual = TableNameResolver.ToSnakeCase(typeName);
        _output.WriteLine($"{typeName} → {actual}");
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task ConnectionTableT_WithExplicitName_OverridesInference()
    {
        // The explicit-name overload bypasses the resolver. Even with an
        // entity type whose inferred name doesn't exist as a table, the
        // explicit-name path should query the explicit name verbatim.
        var explicitName = $"product_explicit_{Guid.NewGuid():N}";

        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {explicitName} (id Int32, name String) ENGINE = MergeTree ORDER BY id");
        try
        {
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {explicitName} VALUES (1, 'a'), (2, 'b')");

            var rows = await conn.Table<Product>(explicitName).ToListAsync();
            Assert.Equal(2, rows.Count);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {explicitName}");
        }
    }

    [Fact]
    public async Task ConnectionTableT_NoExplicitName_RoutesToSnakeCasedTable()
    {
        // End-to-end probe that the inferred name flows all the way through
        // to the actual SQL. We create the table at the inferred name,
        // insert rows, and query via Table<T>() with no override.
        // We isolate within a temporary database to avoid colliding with
        // any other test that might also infer the same name.
        var sideDb = $"infer_db_{Guid.NewGuid():N}";

        await using var setup = new ClickHouseConnection(_fx.BuildSettings());
        await setup.OpenAsync();
        await setup.ExecuteNonQueryAsync($"CREATE DATABASE {sideDb}");
        try
        {
            await setup.ExecuteNonQueryAsync(
                $"CREATE TABLE {sideDb}.product (id Int32, name String) ENGINE = MergeTree ORDER BY id");
            await setup.ExecuteNonQueryAsync(
                $"INSERT INTO {sideDb}.product VALUES (1, 'a'), (2, 'b'), (3, 'c')");

            await using var conn = new ClickHouseConnection(
                _fx.BuildSettings(b => b.WithDatabase(sideDb)));
            await conn.OpenAsync();

            var rows = await conn.Table<Product>().ToListAsync();
            _output.WriteLine($"Inferred table fetched {rows.Count} rows.");
            Assert.Equal(3, rows.Count);
        }
        finally
        {
            await setup.ExecuteNonQueryAsync($"DROP DATABASE IF EXISTS {sideDb}");
        }
    }

    internal sealed class Product
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    internal sealed class LogEntry
    {
        public int Id { get; set; }
    }

    internal sealed class UserAccount
    {
        public int Id { get; set; }
    }
}
