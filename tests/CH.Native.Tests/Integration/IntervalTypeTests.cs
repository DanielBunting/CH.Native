using CH.Native.Connection;
using CH.Native.Data;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

// Interval values surface as ClickHouseInterval (count + unit) rather than TimeSpan:
// Month/Quarter/Year are calendar units with no fixed duration. All eleven server
// interval types (system.data_type_families WHERE name LIKE 'Interval%') are covered.
[Collection("ClickHouse")]
public class IntervalTypeTests
{
    private readonly ClickHouseFixture _fixture;

    public IntervalTypeTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    private async Task<List<object?[]>> QueryAsync(string sql)
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var rows = new List<object?[]>();
        await foreach (var row in connection.QueryStreamAsync(sql))
        {
            var values = new object?[row.FieldCount];
            for (int i = 0; i < row.FieldCount; i++)
            {
                values[i] = row[i];
            }
            rows.Add(values);
        }
        return rows;
    }

    public static IEnumerable<object[]> AllUnits() => new[]
    {
        new object[] { "NANOSECOND", IntervalUnit.Nanosecond },
        new object[] { "MICROSECOND", IntervalUnit.Microsecond },
        new object[] { "MILLISECOND", IntervalUnit.Millisecond },
        new object[] { "SECOND", IntervalUnit.Second },
        new object[] { "MINUTE", IntervalUnit.Minute },
        new object[] { "HOUR", IntervalUnit.Hour },
        new object[] { "DAY", IntervalUnit.Day },
        new object[] { "WEEK", IntervalUnit.Week },
        new object[] { "MONTH", IntervalUnit.Month },
        new object[] { "QUARTER", IntervalUnit.Quarter },
        new object[] { "YEAR", IntervalUnit.Year },
    };

    [Theory]
    [MemberData(nameof(AllUnits))]
    public async Task SelectIntervalLiteral_AllUnits_RoundTrip(string sqlUnit, IntervalUnit expectedUnit)
    {
        var rows = await QueryAsync($"SELECT INTERVAL 3 {sqlUnit}");

        var value = Assert.Single(Assert.Single(rows));
        Assert.Equal(new ClickHouseInterval(3, expectedUnit), value);
    }

    [Fact]
    public async Task ToIntervalFunctions_NegativeAndZero()
    {
        var rows = await QueryAsync("SELECT toIntervalDay(-5), toIntervalMonth(0)");

        var row = Assert.Single(rows);
        Assert.Equal(new ClickHouseInterval(-5, IntervalUnit.Day), row[0]);
        Assert.Equal(new ClickHouseInterval(0, IntervalUnit.Month), row[1]);
    }

    [Fact]
    public async Task IntervalArithmetic_MultiRow()
    {
        var rows = await QueryAsync(
            "SELECT toIntervalDay(number) FROM numbers(3) ORDER BY number");

        Assert.Equal(3, rows.Count);
        for (int i = 0; i < 3; i++)
        {
            Assert.Equal(new ClickHouseInterval(i, IntervalUnit.Day), rows[i][0]);
        }
    }

    [Fact]
    public async Task NullableInterval_NullAndValue()
    {
        var rows = await QueryAsync(
            "SELECT number = 0 ? INTERVAL 3 DAY : NULL FROM numbers(2) ORDER BY number");

        Assert.Equal(2, rows.Count);
        Assert.Equal(new ClickHouseInterval(3, IntervalUnit.Day), rows[0][0]);
        Assert.Null(rows[1][0]);
    }

    [Fact]
    public async Task ArrayOfIntervals_Decodes()
    {
        var rows = await QueryAsync("SELECT [INTERVAL 1 HOUR, INTERVAL 2 HOUR]");

        var array = Assert.IsType<ClickHouseInterval[]>(Assert.Single(Assert.Single(rows)));
        Assert.Equal(new ClickHouseInterval(1, IntervalUnit.Hour), array[0]);
        Assert.Equal(new ClickHouseInterval(2, IntervalUnit.Hour), array[1]);
    }

    [Fact]
    public async Task ConnectionRemainsUsable_AfterIntervalQueries()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await foreach (var _ in connection.QueryStreamAsync("SELECT INTERVAL 3 DAY")) { }
        var result = await connection.ExecuteScalarAsync<int>("SELECT 42");

        Assert.Equal(42, result);
    }
}
