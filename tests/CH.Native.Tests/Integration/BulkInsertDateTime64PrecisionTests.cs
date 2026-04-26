using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Integration coverage for every DateTime64 precision the bulk-insert path supports,
/// exercising DateTime64Extractor, NullableDateTime64Extractor,
/// DateTimeOffsetExtractor (DateTime64 branch) and NullableDateTimeOffsetExtractor.
/// Regression guard for the pre-fix double-precision nanosecond loss.
/// </summary>
[Collection("ClickHouse")]
public class BulkInsertDateTime64PrecisionTests
{
    private readonly ClickHouseFixture _fixture;

    public BulkInsertDateTime64PrecisionTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    private static readonly DateTime Aligned =
        new DateTime(2024, 1, 15, 12, 30, 45, DateTimeKind.Utc).AddTicks(1_234_567);

    [Theory]
    [InlineData(0)]
    [InlineData(3)]
    [InlineData(6)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(9)]
    public async Task BulkInsert_DateTime64_AllPrecisions_RoundTrip(int precision)
    {
        var tableName = $"test_dt64_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (Id Int32, Value DateTime64({precision}, 'UTC')) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<DateTimeRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new DateTimeRow { Id = 1, Value = Aligned });
            await inserter.CompleteAsync();

            var result = await connection.ExecuteScalarAsync<DateTime>(
                $"SELECT Value FROM {tableName} WHERE Id = 1");

            Assert.Equal(TruncateToPrecision(Aligned, precision), result);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Theory]
    [InlineData(3)]
    [InlineData(6)]
    [InlineData(8)]
    [InlineData(9)]
    public async Task BulkInsert_NullableDateTime64_RoundTrip_ValueAndNull(int precision)
    {
        var tableName = $"test_dt64_null_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (Id Int32, Value Nullable(DateTime64({precision}, 'UTC'))) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<NullableDateTimeRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new NullableDateTimeRow { Id = 1, Value = Aligned });
            await inserter.AddAsync(new NullableDateTimeRow { Id = 2, Value = null });
            await inserter.CompleteAsync();

            var row1 = await connection.ExecuteScalarAsync<DateTime>($"SELECT Value FROM {tableName} WHERE Id = 1");
            Assert.Equal(TruncateToPrecision(Aligned, precision), row1);

            var nullRowCount = await connection.ExecuteScalarAsync<long>(
                $"SELECT count() FROM {tableName} WHERE Id = 2 AND Value IS NULL");
            Assert.Equal(1, nullRowCount);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Theory]
    [InlineData(3)]
    [InlineData(6)]
    [InlineData(8)]
    [InlineData(9)]
    public async Task BulkInsert_DateTimeOffset_AsDateTime64_RoundTrip(int precision)
    {
        var tableName = $"test_dto_dt64_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (Id Int32, Value DateTime64({precision}, 'UTC')) ENGINE = Memory");

        try
        {
            var value = new DateTimeOffset(Aligned, TimeSpan.Zero);

            await using var inserter = connection.CreateBulkInserter<DateTimeOffsetRow>(tableName);
            await inserter.InitAsync();
            await inserter.AddAsync(new DateTimeOffsetRow { Id = 1, Value = value });
            await inserter.CompleteAsync();

            var result = await connection.ExecuteScalarAsync<DateTime>(
                $"SELECT Value FROM {tableName} WHERE Id = 1");
            Assert.Equal(TruncateToPrecision(Aligned, precision), result);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Theory]
    [InlineData(3)]
    [InlineData(9)]
    public async Task BulkInsert_NullableDateTimeOffset_AsDateTime64_RoundTrip(int precision)
    {
        var tableName = $"test_dto_null_dt64_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (Id Int32, Value Nullable(DateTime64({precision}, 'UTC'))) ENGINE = Memory");

        try
        {
            var value = new DateTimeOffset(Aligned, TimeSpan.Zero);

            await using var inserter = connection.CreateBulkInserter<NullableDateTimeOffsetRow>(tableName);
            await inserter.InitAsync();
            await inserter.AddAsync(new NullableDateTimeOffsetRow { Id = 1, Value = value });
            await inserter.AddAsync(new NullableDateTimeOffsetRow { Id = 2, Value = null });
            await inserter.CompleteAsync();

            var row1 = await connection.ExecuteScalarAsync<DateTime>($"SELECT Value FROM {tableName} WHERE Id = 1");
            Assert.Equal(TruncateToPrecision(Aligned, precision), row1);

            var nullRowCount = await connection.ExecuteScalarAsync<long>(
                $"SELECT count() FROM {tableName} WHERE Id = 2 AND Value IS NULL");
            Assert.Equal(1, nullRowCount);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_DateTime64_Precision9_NanosecondDigitsPreserved_Regression()
    {
        // Pre-fix the DateTime64 bulk extractor did totalSeconds(double) * 1e9, which for
        // recent timestamps overflows double precision and silently drops the last 2-3
        // nanosecond digits. Compare against fromUnixTimestamp64Nano on the server to
        // prove the exact 100ns-tick-aligned wire value round-trips.
        var tableName = $"test_dt64_ns_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (Id Int32, Value DateTime64(9, 'UTC')) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<DateTimeRow>(tableName);
            await inserter.InitAsync();
            await inserter.AddAsync(new DateTimeRow { Id = 1, Value = Aligned });
            await inserter.CompleteAsync();

            var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var expectedNs = (Aligned - epoch).Ticks * 100L;

            // toUnixTimestamp64Nano returns the wire-level Int64 — tests the exact bytes.
            var actualNs = await connection.ExecuteScalarAsync<long>(
                $"SELECT toUnixTimestamp64Nano(Value) FROM {tableName} WHERE Id = 1");
            Assert.Equal(expectedNs, actualNs);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    private static DateTime TruncateToPrecision(DateTime value, int precision)
    {
        // .NET DateTime has 100ns tick resolution. For precisions <= 7 we floor to that
        // precision's unit; for 8/9 the value is already at or above tick resolution.
        if (precision >= 7) return value;
        var utc = value.Kind == DateTimeKind.Utc ? value : value.ToUniversalTime();
        var ticksPerUnit = TimeSpan.TicksPerSecond / (long)Math.Pow(10, precision);
        var ticks = (utc - DateTime.UnixEpoch).Ticks;
        return DateTime.UnixEpoch.AddTicks(ticks / ticksPerUnit * ticksPerUnit);
    }

    private class DateTimeRow
    {
        public int Id { get; set; }
        public DateTime Value { get; set; }
    }

    private class NullableDateTimeRow
    {
        public int Id { get; set; }
        public DateTime? Value { get; set; }
    }

    private class DateTimeOffsetRow
    {
        public int Id { get; set; }
        public DateTimeOffset Value { get; set; }
    }

    private class NullableDateTimeOffsetRow
    {
        public int Id { get; set; }
        public DateTimeOffset? Value { get; set; }
    }
}
