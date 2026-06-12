using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

// DateTime64(8/9) values exceed System.DateTime's 100ns tick resolution; the DateTime
// view truncates (long-standing default), and GetFieldValue<long> /
// ExecuteScalarAsync<long> return the exact Int64 wire value (the unit count since
// epoch — toUnixTimestamp64Nano for precision 9).
[Collection("ClickHouse")]
public class DateTime64RawValueTests
{
    private const long Nanos = 1_704_067_200_123_456_789; // 2024-01-01 00:00:00.123456789 UTC

    private readonly ClickHouseFixture _fixture;

    public DateTime64RawValueTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    private async Task RunWithValueAsync(string columnType, string literal, Func<ClickHouseConnection, string, Task> body)
    {
        var table = $"test_dt64raw_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync($"CREATE TABLE {table} (val {columnType}) ENGINE = Memory");
        try
        {
            await connection.ExecuteNonQueryAsync($"INSERT INTO {table} VALUES ('{literal}')");
            await body(connection, table);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public Task Precision9_GetFieldValueLong_ReturnsExactNanoseconds() =>
        RunWithValueAsync("DateTime64(9, 'UTC')", "2024-01-01 00:00:00.123456789", async (connection, table) =>
        {
            await foreach (var row in connection.QueryStreamAsync($"SELECT val FROM {table}"))
            {
                Assert.Equal(Nanos, row.GetFieldValue<long>(0));

                // The DateTime view of the same value stays truncated to 100ns ticks.
                var dt = row.GetFieldValue<DateTime>(0);
                Assert.Equal(new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddTicks(1234567), dt);
            }

            // Cross-check against the server's own nanosecond accessor.
            var serverNanos = await connection.ExecuteScalarAsync<long>(
                $"SELECT toUnixTimestamp64Nano(val) FROM {table}");
            Assert.Equal(Nanos, serverNanos);
        });

    [Fact]
    public Task Precision9_ExecuteScalarLong_ReturnsExactNanoseconds() =>
        RunWithValueAsync("DateTime64(9, 'UTC')", "2024-01-01 00:00:00.123456789", async (connection, table) =>
        {
            var raw = await connection.ExecuteScalarAsync<long>($"SELECT val FROM {table}");
            Assert.Equal(Nanos, raw);
        });

    // The parameterized scalar path (ClickHouseCommand.ExecuteScalarAsync<T> →
    // ExecuteScalarWithParametersAsync) carries the same long escape hatch.
    [Fact]
    public Task Precision9_ParameterizedScalarLong_ReturnsExactNanoseconds() =>
        RunWithValueAsync("DateTime64(9, 'UTC')", "2024-01-01 00:00:00.123456789", async (connection, table) =>
        {
            await using var command = connection.CreateCommand($"SELECT val FROM {table} WHERE 1 = @one");
            command.Parameters.Add("one", 1);

            var raw = await command.ExecuteScalarAsync<long>();
            Assert.Equal(Nanos, raw);
        });

    [Fact]
    public Task Precision8_GetFieldValueLong_ReturnsExactUnits() =>
        RunWithValueAsync("DateTime64(8, 'UTC')", "2024-01-01 00:00:00.12345678", async (connection, table) =>
        {
            await foreach (var row in connection.QueryStreamAsync($"SELECT val FROM {table}"))
            {
                Assert.Equal(170_406_720_012_345_678L, row.GetFieldValue<long>(0));
            }
        });

    // Typed row mapping compiles to GetFieldValue<TProp>, so a long property captures
    // the raw value the same way.
    private class RawRow
    {
        public long Val { get; set; }
    }

    [Fact]
    public Task Precision9_TypedRowMapping_LongProperty_GetsRawValue() =>
        RunWithValueAsync("DateTime64(9, 'UTC')", "2024-01-01 00:00:00.123456789", async (connection, table) =>
        {
            await foreach (var row in connection.QueryStreamAsync<RawRow>($"SELECT val FROM {table}"))
            {
                Assert.Equal(Nanos, row.Val);
            }
        });

    // Tick-exact precisions keep the typed DateTime storage; the long escape hatch is
    // not wired for them (no data is lost — convert the DateTime instead).
    [Fact]
    public Task Precision3_DateTimeView_Lossless() =>
        RunWithValueAsync("DateTime64(3, 'UTC')", "2024-01-01 00:00:00.123", async (connection, table) =>
        {
            await foreach (var row in connection.QueryStreamAsync($"SELECT val FROM {table}"))
            {
                Assert.Equal(
                    new DateTime(2024, 1, 1, 0, 0, 0, 123, DateTimeKind.Utc),
                    row.GetFieldValue<DateTime>(0));
            }
        });

    [Fact]
    public Task Precision9_MultiRow_OrderedRawValues() =>
        RunWithValueAsync("DateTime64(9, 'UTC')", "2024-01-01 00:00:00.000000001", async (connection, table) =>
        {
            await connection.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES ('2024-01-01 00:00:00.999999999')");

            var raws = new List<long>();
            await foreach (var row in connection.QueryStreamAsync($"SELECT val FROM {table} ORDER BY val"))
            {
                raws.Add(row.GetFieldValue<long>(0));
            }

            Assert.Equal(new[] { 1_704_067_200_000_000_001L, 1_704_067_200_999_999_999L }, raws);
        });
}
