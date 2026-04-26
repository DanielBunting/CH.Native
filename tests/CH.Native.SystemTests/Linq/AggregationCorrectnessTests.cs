using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.SystemTests.Fixtures;
using CH.Native.SystemTests.Linq.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Linq;

[Collection("LinqFacts")]
[Trait(Categories.Name, Categories.Linq)]
public class AggregationCorrectnessTests : IAsyncLifetime
{
    private readonly SingleNodeFixture _node;
    private readonly LinqFactTableFixture _facts;
    private ClickHouseConnection _conn = null!;

    public AggregationCorrectnessTests(SingleNodeFixture node, LinqFactTableFixture facts)
    {
        _node = node;
        _facts = facts;
    }

    public async Task InitializeAsync()
    {
        _conn = new ClickHouseConnection(_node.BuildSettings());
        await _conn.OpenAsync();
    }

    public Task DisposeAsync() => _conn.DisposeAsync().AsTask();

    [Fact]
    public async Task Where_NumericComparison_FiltersCorrectly()
    {
        int linqCount = await _conn.Table<LinqFactRow>(_facts.TableName)
            .Where(x => x.Amount > 100).CountAsync();

        var raw = await LinqAssertions.ExecuteScalarAsync<long>(
            _conn, $"SELECT count() FROM {_facts.TableName} WHERE amount > 100");

        Assert.Equal((int)raw, linqCount);
    }

    [Fact]
    public async Task GroupBy_SingleColumn_Sum_MatchesRawSql()
    {
        // LINQ: group by country, project key + sum(amount).
        var linq = await _conn.Table<LinqFactRow>(_facts.TableName)
            .GroupBy(x => x.Country)
            .Select(g => new CountryTotal
            {
                Country = g.Key,
                Total = g.Sum(x => x.Amount),
            })
            .ToListAsync();

        // Raw oracle.
        var oracle = new List<CountryTotal>();
        await foreach (var row in _conn.QueryAsync(
            $"SELECT country, sum(amount) AS total FROM {_facts.TableName} GROUP BY country"))
        {
            oracle.Add(new CountryTotal
            {
                Country = (string)row[0]!,
                Total = Convert.ToDouble(row[1]!),
            });
        }

        Assert.Equal(oracle.Count, linq.Count);
        var linqByCountry = linq.ToDictionary(r => r.Country, r => r.Total);
        foreach (var o in oracle)
        {
            Assert.True(linqByCountry.TryGetValue(o.Country, out double total),
                $"Missing country in LINQ result: {o.Country}");
            Assert.Equal(o.Total, total, precision: 2);
        }
    }

    [Fact]
    public async Task GroupBy_NullableKey_NullGroupPresent()
    {
        // ClickHouse treats NULL as its own group for GROUP BY on nullable columns.
        // The fact table seeds NULL for every 7th row.
        var linq = await _conn.Table<LinqFactRow>(_facts.TableName)
            .GroupBy(x => x.OptionalCode)
            .Select(g => new NullableGroup { Key = g.Key, Count = g.Count() })
            .ToListAsync();

        long rawNullCount = await LinqAssertions.ExecuteScalarAsync<long>(
            _conn,
            $"SELECT count() FROM {_facts.TableName} WHERE optional_code IS NULL");
        Assert.True(rawNullCount > 0, "Fixture should seed at least one NULL optional_code row");

        var nullGroup = linq.SingleOrDefault(g => g.Key == null);
        Assert.NotNull(nullGroup);
        Assert.Equal((int)rawNullCount, nullGroup!.Count);
    }

    [Theory]
    [InlineData("Sum")]
    [InlineData("Min")]
    [InlineData("Max")]
    [InlineData("Avg")]
    [InlineData("Count")]
    public async Task Aggregate_OverEmptyTable_DefaultValues(string op)
    {
        var query = _conn.Table<LinqFactRow>(_facts.TableName).Where(x => x.Id < 0);

        switch (op)
        {
            case "Sum":
                {
                    double linq = await query.SumAsync(x => x.Amount);
                    double raw = await LinqAssertions.ExecuteScalarAsync<double>(
                        _conn, $"SELECT sum(amount) FROM {_facts.TableName} WHERE id < 0");
                    Assert.Equal(raw, linq, precision: 6);
                    break;
                }
            case "Min":
                {
                    double linq = await query.MinAsync(x => x.Amount);
                    // ClickHouse min() over empty set returns NULL — pin observed behaviour:
                    // ExecuteScalarAsync<double> coalesces null to default(double) == 0.
                    Assert.Equal(0.0, linq);
                    break;
                }
            case "Max":
                {
                    double linq = await query.MaxAsync(x => x.Amount);
                    Assert.Equal(0.0, linq);
                    break;
                }
            case "Avg":
                {
                    double linq = await query.AverageAsync(x => x.Amount);
                    // avg() over empty set is NaN on ClickHouse (server returns NULL,
                    // client coalesces to 0). Pin whichever the lib does today.
                    Assert.True(linq is 0.0 or double.NaN,
                        $"avg over empty set should pin to 0 or NaN; got {linq}");
                    break;
                }
            case "Count":
                {
                    int linq = await query.CountAsync();
                    Assert.Equal(0, linq);
                    break;
                }
        }
    }

    [Fact]
    public async Task Aggregate_TypeCoercion_Int32SumAsInt64()
    {
        // The fixture has 1..50 cycle so total fits Int32, but ClickHouse returns
        // sum(Int32) as Int64. Verify the LINQ path's sum matches the raw oracle.
        long oracleSum = await LinqAssertions.ExecuteScalarAsync<long>(
            _conn, $"SELECT sum(quantity) FROM {_facts.TableName}");

        int linqSum = await _conn.Table<LinqFactRow>(_facts.TableName)
            .SumAsync(x => x.Quantity);

        // If sum overflows Int32, the LINQ path will throw or wrap — that's a real
        // bug surfaced by this test. Asserting equality forces the question.
        Assert.Equal(oracleSum, linqSum);
    }

    private sealed class CountryTotal
    {
        public string Country { get; set; } = string.Empty;
        public double Total { get; set; }
    }

    private sealed class NullableGroup
    {
        public int? Key { get; set; }
        public int Count { get; set; }
    }
}
