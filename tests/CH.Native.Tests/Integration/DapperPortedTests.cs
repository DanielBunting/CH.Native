using System.Data;
using System.Runtime.CompilerServices;
using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Dapper;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Dapper coverage ported from the driver's DapperTests: ITuple materialization, same-prefix parameter
/// names, DynamicParameters, IN via array parameter (the driver's <c>IN @Ids</c> list-expansion is N/A —
/// CH.Native's array handler binds <c>int[]</c> as a single <c>Array(T)</c>), and <c>* EXCEPT</c> insert.
/// </summary>
[Collection("ClickHouse")]
public class DapperPortedTests
{
    private readonly ClickHouseFixture _fixture;

    static DapperPortedTests() => SqlMapper.AddTypeHandler(new ITupleHandler());

    public DapperPortedTests(ClickHouseFixture fixture) => _fixture = fixture;

    private sealed class ITupleHandler : SqlMapper.TypeHandler<ITuple>
    {
        public override ITuple Parse(object value) => (ITuple)value;
        public override void SetValue(IDbDataParameter parameter, ITuple? value) => throw new NotSupportedException();
    }

    private sealed class NumberRow
    {
        public long Value { get; set; }
    }

    [Fact]
    public async Task Query_ReturningTuple_MaterializesITuple()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var tuple = (await connection.QueryAsync<ITuple>("SELECT tuple(toInt32(42), 'hello')")).Single();

        Assert.Equal(2, tuple.Length);
        Assert.Equal(42, Convert.ToInt32(tuple[0]));
        Assert.Equal("hello", tuple[1]);
    }

    [Fact]
    public async Task SameParameterPrefix_DoesNotCollide()
    {
        var table = $"prefix_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteAsync(
            $"CREATE TABLE {table} (testField Int32, testFieldWithSuffix Int32) ENGINE = Memory");
        try
        {
            await connection.ExecuteAsync(
                $"INSERT INTO {table} SELECT @testField, @testFieldWithSuffix",
                new { testField = 1, testFieldWithSuffix = 2 });

            var a = await connection.ExecuteScalarAsync<int>($"SELECT testField FROM {table}");
            var b = await connection.ExecuteScalarAsync<int>($"SELECT testFieldWithSuffix FROM {table}");
            Assert.Equal(1, a);
            Assert.Equal(2, b);
        }
        finally
        {
            await connection.ExecuteAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task DynamicParameters_FromDictionaryAndAnonymous()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var fromDict = new DynamicParameters();
        fromDict.Add("min", 3);
        var a = (await connection.QueryAsync<NumberRow>(
            "SELECT toInt64(number) AS Value FROM numbers(10) WHERE number >= @min", fromDict)).ToList();
        Assert.Equal(7, a.Count);

        var fromAnon = new DynamicParameters(new { min = 8 });
        var b = (await connection.QueryAsync<NumberRow>(
            "SELECT toInt64(number) AS Value FROM numbers(10) WHERE number >= @min", fromAnon)).ToList();
        Assert.Equal(2, b.Count);
    }

    [Fact]
    public async Task WhereIn_ViaArrayParameter()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var matches = (await connection.QueryAsync<NumberRow>(
            "SELECT toInt64(number) AS Value FROM numbers(10) WHERE has(@ids, toInt32(number))",
            new { ids = new[] { 1, 3, 7 } })).Select(r => r.Value).OrderBy(x => x).ToList();

        Assert.Equal(new long[] { 1, 3, 7 }, matches);
    }

    [Fact]
    public async Task InsertWithExceptSyntax_PopulatesDefaults()
    {
        var table = $"except_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteAsync(
            $"CREATE TABLE {table} (id Int32, name String, created DateTime DEFAULT now()) ENGINE = Memory");
        try
        {
            await connection.ExecuteAsync(
                $"INSERT INTO {table} (* EXCEPT (created)) SELECT @id, @name",
                new { id = 1, name = "x" });

            var createdIsRecent = await connection.ExecuteScalarAsync<byte>(
                $"SELECT created > (now() - INTERVAL 1 MINUTE) FROM {table}");
            Assert.Equal(1, createdIsRecent);
        }
        finally
        {
            await connection.ExecuteAsync($"DROP TABLE IF EXISTS {table}");
        }
    }
}
