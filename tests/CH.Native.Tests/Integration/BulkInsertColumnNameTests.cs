using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Special-character, backticked, and reserved-word column names on the bulk path (ported from the
/// driver's ShouldExecuteBulkInsertWithComplexColumnName / ShouldExecuteInsertWithBacktickedColumns /
/// WithReservedWordColumns). CH.Native quotes column identifiers via <c>ClickHouseIdentifier.Quote</c>
/// (backtick-doubling); special-char names go through the runtime-column-name <c>DynamicBulkInserter</c>
/// path because <c>[ClickHouseColumn(Name=...)]</c> requires a compile-time constant.
/// </summary>
[Collection("ClickHouse")]
public class BulkInsertColumnNameTests
{
    private readonly ClickHouseFixture _fixture;

    public BulkInsertColumnNameTests(ClickHouseFixture fixture) => _fixture = fixture;

    private static string Quote(string name) => "`" + name.Replace("`", "``") + "`";

    [Theory]
    [InlineData("with.dot")]
    [InlineData("with'quote")]
    [InlineData("double\"quote")]
    [InlineData("with space")]
    [InlineData("with`backtick")]
    [InlineData("with:colon")]
    [InlineData("with,comma")]
    [InlineData("with^caret")]
    [InlineData("with&ampersand")]
    [InlineData("with(round)brackets")]
    [InlineData("with*star")]
    [InlineData("with?question")]
    [InlineData("with!exclamation")]
    public async Task SpecialCharacterColumnName_RoundTrips(string columnName)
    {
        var table = $"colname_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync($"CREATE TABLE {table} ({Quote(columnName)} Int32) ENGINE = Memory");
        try
        {
            await connection.BulkInsertAsync(table, new[] { columnName },
                new[] { new object?[] { 123 } });

            var value = await connection.ExecuteScalarAsync<int>($"SELECT {Quote(columnName)} FROM {table}");
            Assert.Equal(123, value);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    private sealed class ReservedWordRow
    {
        [ClickHouseColumn(Name = "index")]
        public int Index { get; set; }

        [ClickHouseColumn(Name = "key")]
        public string Key { get; set; } = "";

        [ClickHouseColumn(Name = "order")]
        public int Order { get; set; }
    }

    [Fact]
    public async Task ReservedWordColumns_RoundTripViaAttribute()
    {
        var table = $"reserved_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (`index` Int32, `key` String, `order` Int32) ENGINE = Memory");
        try
        {
            await using var inserter = connection.CreateBulkInserter<ReservedWordRow>(table);
            await inserter.InitAsync();
            await inserter.AddAsync(new ReservedWordRow { Index = 7, Key = "k", Order = 3 });
            await inserter.CompleteAsync();

            await using var reader = await connection.ExecuteReaderAsync(
                $"SELECT `index`, `key`, `order` FROM {table}");
            Assert.True(await reader.ReadAsync());
            Assert.Equal(7, reader.GetFieldValue<int>(0));
            Assert.Equal("k", reader.GetFieldValue<string>(1));
            Assert.Equal(3, reader.GetFieldValue<int>(2));
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }
}
