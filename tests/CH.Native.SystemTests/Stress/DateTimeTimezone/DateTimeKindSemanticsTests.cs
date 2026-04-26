using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Stress.DateTimeTimezone;

/// <summary>
/// Pins the contract for <see cref="DateTimeKind"/> on both sides of the wire:
/// what the writer does with Utc/Local/Unspecified, and what the reader returns.
/// Disagreement here is the most common source of silent timezone drift.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Stress)]
public class DateTimeKindSemanticsTests
{
    private readonly SingleNodeFixture _fixture;

    public DateTimeKindSemanticsTests(SingleNodeFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Insert_KindUtc_NoConversion()
    {
        // 2024-06-15 12:00:00 UTC = unix milli 1718452800000
        var input = new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Utc);

        var table = $"tz_kind_utc_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (ts DateTime64(3, 'UTC')) ENGINE = Memory");

        try
        {
            await using (var inserter = conn.CreateBulkInserter<TsRow>(table))
            {
                await inserter.InitAsync();
                await inserter.AddAsync(new TsRow { Ts = input });
                await inserter.CompleteAsync();
            }

            var wireMillis = await conn.ExecuteScalarAsync<long>(
                $"SELECT toUnixTimestamp64Milli(ts) FROM {table}");
            Assert.Equal(1718452800000L, wireMillis);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    [Trait("Sensitivity", "HostTimezone")]
    public async Task Insert_KindLocal_ConvertedViaToUniversalTime()
    {
        // Construct a Local DateTime equivalent to 2024-06-15 12:00 UTC, given the
        // current host TZ. Whatever the host zone is, ToUniversalTime on a Local instant
        // built from "now-equivalent" UTC must round-trip back to that UTC.
        var utcReference = new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Utc);
        var localEquivalent = TimeZoneInfo.ConvertTimeFromUtc(utcReference, TimeZoneInfo.Local);
        var localKindInput = DateTime.SpecifyKind(localEquivalent, DateTimeKind.Local);

        var table = $"tz_kind_local_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (ts DateTime64(3, 'UTC')) ENGINE = Memory");

        try
        {
            await using (var inserter = conn.CreateBulkInserter<TsRow>(table))
            {
                await inserter.InitAsync();
                await inserter.AddAsync(new TsRow { Ts = localKindInput });
                await inserter.CompleteAsync();
            }

            var wireMillis = await conn.ExecuteScalarAsync<long>(
                $"SELECT toUnixTimestamp64Milli(ts) FROM {table}");
            Assert.Equal(1718452800000L, wireMillis);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    [Trait("Status", "PendingFix")]
    public async Task Insert_KindUnspecified_TreatedAsUtc_NotLocal()
    {
        // Pins the desired contract: Unspecified should be written verbatim as UTC,
        // matching the column writer at DateTime64ColumnWriter.cs:66. The bulk-insert
        // path appears to apply ToUniversalTime() regardless of Kind, producing a
        // host-zone-dependent off-by-N-hours wire value. This test currently fails on
        // any host whose local zone is not UTC — that's the bug surface to investigate.
        var input = new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Unspecified);

        var table = $"tz_kind_unspec_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (ts DateTime64(3, 'UTC')) ENGINE = Memory");

        try
        {
            await using (var inserter = conn.CreateBulkInserter<TsRow>(table))
            {
                await inserter.InitAsync();
                await inserter.AddAsync(new TsRow { Ts = input });
                await inserter.CompleteAsync();
            }

            var wireMillis = await conn.ExecuteScalarAsync<long>(
                $"SELECT toUnixTimestamp64Milli(ts) FROM {table}");
            Assert.Equal(1718452800000L, wireMillis);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Theory]
    [InlineData("DateTime")]
    [InlineData("DateTime64(3)")]
    [InlineData("DateTime64(3, 'UTC')")]
    [InlineData("DateTime64(3, 'America/New_York')")]
    [InlineData("DateTime64(6, 'Asia/Tokyo')")]
    public async Task Read_DateTime_AlwaysReturnsKindUtc(string columnType)
    {
        var table = $"tz_kind_read_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (ts {columnType}) ENGINE = Memory");

        try
        {
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES (toDateTime('2024-06-15 12:00:00', 'UTC'))");

            DateTime read = default;
            await foreach (var r in conn.QueryAsync($"SELECT ts FROM {table}"))
                read = r.GetFieldValue<DateTime>(0);

            Assert.Equal(DateTimeKind.Utc, read.Kind);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Theory]
    [InlineData("America/New_York", "2024-06-15 12:00:00", -4)] // EDT in summer
    [InlineData("America/New_York", "2024-12-15 12:00:00", -5)] // EST in winter
    [InlineData("Asia/Tokyo",       "2024-06-15 12:00:00",  9)]
    [InlineData("Australia/Sydney", "2024-12-15 12:00:00", 11)] // AEDT
    public async Task Read_DateTimeWithTz_ReturnsDateTimeOffset_OffsetMatchesTzAtInstant(
        string zone, string utcLiteral, int expectedHours)
    {
        var table = $"tz_dto_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (ts DateTime('{zone}')) ENGINE = Memory");

        try
        {
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES (toDateTime('{utcLiteral}', 'UTC'))");

            DateTimeOffset read = default;
            await foreach (var r in conn.QueryAsync($"SELECT ts FROM {table}"))
                read = r.GetFieldValue<DateTimeOffset>(0);

            Assert.Equal(TimeSpan.FromHours(expectedHours), read.Offset);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task RoundTripContract_KindUtcInUtcOut()
    {
        var input = new DateTime(2024, 7, 4, 16, 30, 0, DateTimeKind.Utc);

        var table = $"tz_kind_rt_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (ts DateTime64(3, 'UTC')) ENGINE = Memory");

        try
        {
            await using (var inserter = conn.CreateBulkInserter<TsRow>(table))
            {
                await inserter.InitAsync();
                await inserter.AddAsync(new TsRow { Ts = input });
                await inserter.CompleteAsync();
            }

            DateTime read = default;
            await foreach (var r in conn.QueryAsync($"SELECT ts FROM {table}"))
                read = r.GetFieldValue<DateTime>(0);

            Assert.Equal(DateTimeKind.Utc, read.Kind);
            Assert.Equal(input, read);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    private class TsRow
    {
        [ClickHouseColumn(Name = "ts", Order = 0)] public DateTime Ts { get; set; }
    }
}
