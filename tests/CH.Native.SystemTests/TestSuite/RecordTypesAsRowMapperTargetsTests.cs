using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.TestSuite;

/// <summary>
/// Probes whether modern C# record types work as row-mapper targets in the
/// LINQ + ADO + raw QueryAsync paths. Records (positional) compile to
/// classes with a primary constructor that takes all properties as args —
/// they have NO parameterless constructor. The H12 finding showed the
/// LINQ mapper rejects anonymous types for the same reason; records would
/// follow.
///
/// <para>
/// This test pins today's behavior across:
/// </para>
/// <list type="bullet">
/// <item><description>Positional <c>record</c> with <c>[ClickHouseColumn]</c> applied to ctor params.</description></item>
/// <item><description>Read-only POCO with init-only setters (<c>{ get; init; }</c>).</description></item>
/// <item><description>Class with parameterless ctor + decorated properties — known good baseline.</description></item>
/// </list>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class RecordTypesAsRowMapperTargetsTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public RecordTypesAsRowMapperTargetsTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    // Baseline — known-good shape.
    public sealed class ClassRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "name", Order = 1)] public string Name { get; set; } = "";
    }

    // Positional record — ctor takes args; no parameterless ctor.
    public sealed record PositionalRecord(
        [property: ClickHouseColumn(Name = "id", Order = 0)] int Id,
        [property: ClickHouseColumn(Name = "name", Order = 1)] string Name);

    // Class with init-only setters. HAS a parameterless ctor (compiler
    // generates one when no explicit ctor is declared).
    public sealed class InitOnlyClass
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; init; }
        [ClickHouseColumn(Name = "name", Order = 1)] public string Name { get; init; } = "";
    }

    private async Task SeedAsync(string table)
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, name String) ENGINE = Memory");
        await conn.ExecuteNonQueryAsync(
            $"INSERT INTO {table} VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    }

    [Fact]
    public async Task ClassRow_RawQueryAsync_Works_BaselineGood()
    {
        var table = $"row_class_{Guid.NewGuid():N}";
        await SeedAsync(table);
        try
        {
            await using var conn = new ClickHouseConnection(_fx.BuildSettings());
            await conn.OpenAsync();

            var rows = new List<ClassRow>();
            await foreach (var r in conn.QueryAsync<ClassRow>($"SELECT id, name FROM {table} ORDER BY id"))
                rows.Add(r);

            Assert.Equal(3, rows.Count);
            Assert.Equal("a", rows[0].Name);
        }
        finally
        {
            await using var c = new ClickHouseConnection(_fx.BuildSettings());
            await c.OpenAsync();
            await c.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task PositionalRecord_RawQueryAsync_RoundTrips()
    {
        // The constraint on QueryAsync<T> was relaxed from `where T : new()`
        // to `where T : class`. TypeMapper<T> picks the parameterless ctor
        // when present (existing POCO path) or falls back to the args-ctor
        // path for records and anonymous types. Verify a positional record
        // round-trips end-to-end.
        var table = $"row_record_{Guid.NewGuid():N}";
        await SeedAsync(table);
        try
        {
            await using var conn = new ClickHouseConnection(_fx.BuildSettings());
            await conn.OpenAsync();

            var rows = new List<PositionalRecord>();
            await foreach (var r in conn.QueryAsync<PositionalRecord>(
                $"SELECT id, name FROM {table} ORDER BY id"))
                rows.Add(r);

            Assert.Equal(3, rows.Count);
            Assert.Equal(1, rows[0].Id);
            Assert.Equal("a", rows[0].Name);
            Assert.Equal(3, rows[2].Id);
            Assert.Equal("c", rows[2].Name);
        }
        finally
        {
            await using var c = new ClickHouseConnection(_fx.BuildSettings());
            await c.OpenAsync();
            await c.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public void QueryAsyncT_HasNoConstraint()
    {
        // Lock-in: the QueryAsync<T> generic parameter has no constraint —
        // it accepts records, anon types, value types (int for scalar
        // pseudo-projections), and standard POCOs. TypeMapper internally
        // selects between the parameterless-ctor path and the args-ctor
        // path based on T's reflection.
        var queryAsyncMethod = typeof(CH.Native.Connection.ClickHouseConnection)
            .GetMethods()
            .First(m => m.Name == "QueryAsync"
                && m.IsGenericMethod
                && m.GetGenericArguments().Length == 1);

        var typeParam = queryAsyncMethod.GetGenericArguments()[0];
        var attrs = typeParam.GenericParameterAttributes;

        _output.WriteLine($"QueryAsync<T> generic-parameter attributes: {attrs}");
        Assert.False(
            (attrs & System.Reflection.GenericParameterAttributes.DefaultConstructorConstraint) != 0,
            "QueryAsync<T> should not require parameterless ctor.");
    }

    [Fact]
    public async Task InitOnlyClass_RawQueryAsync_Works_LikeBaseline()
    {
        var table = $"row_initonly_{Guid.NewGuid():N}";
        await SeedAsync(table);
        try
        {
            await using var conn = new ClickHouseConnection(_fx.BuildSettings());
            await conn.OpenAsync();

            var rows = new List<InitOnlyClass>();
            await foreach (var r in conn.QueryAsync<InitOnlyClass>(
                $"SELECT id, name FROM {table} ORDER BY id"))
                rows.Add(r);

            // Init-only setters with default ctor: should work like the baseline.
            Assert.Equal(3, rows.Count);
            Assert.Equal("a", rows[0].Name);
        }
        finally
        {
            await using var c = new ClickHouseConnection(_fx.BuildSettings());
            await c.OpenAsync();
            await c.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }
}
