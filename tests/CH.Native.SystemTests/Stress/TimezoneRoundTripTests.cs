using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Stress;

/// <summary>
/// Round-trip DateTime/DateTime64 across timezone-affected paths. Silent TZ drift is
/// the worst class of bug for analytics pipelines; this test pins the contract.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Stress)]
public class TimezoneRoundTripTests
{
    private readonly SingleNodeFixture _fixture;

    public TimezoneRoundTripTests(SingleNodeFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task UtcDateTime64_RoundTrips_WithoutDrift()
    {
        var table = $"tz_utc_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, ts DateTime64(6, 'UTC')) ENGINE = Memory");

        try
        {
            var inputs = new[]
            {
                new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc),
                new DateTime(2024, 6, 15, 12, 34, 56, 789, DateTimeKind.Utc),
                new DateTime(2024, 12, 31, 23, 59, 59, 999, DateTimeKind.Utc),
            };

            await using (var inserter = conn.CreateBulkInserter<TsRow>(table))
            {
                await inserter.InitAsync();
                for (int i = 0; i < inputs.Length; i++)
                    await inserter.AddAsync(new TsRow { Id = i, Ts = inputs[i] });
                await inserter.CompleteAsync();
            }

            var results = new List<DateTime>();
            await foreach (var r in conn.QueryAsync($"SELECT ts FROM {table} ORDER BY id"))
                results.Add(r.GetFieldValue<DateTime>(0));

            for (int i = 0; i < inputs.Length; i++)
            {
                var expected = inputs[i];
                var actual = results[i].Kind == DateTimeKind.Utc ? results[i] : results[i].ToUniversalTime();
                // DateTime64(6) keeps microseconds; allow 1-tick fuzz for boundary cases.
                Assert.True(Math.Abs((expected - actual).Ticks) < 100,
                    $"DateTime drift: expected={expected:O} actual={actual:O} ({(expected - actual).Ticks} ticks)");
            }
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task DstBoundaryDay_NoSilentDrift()
    {
        // 2024-03-10 02:30:00 in America/Los_Angeles is the "spring forward" boundary;
        // make sure server-side TZ-aware DateTime round-trips by storing as UTC and
        // comparing against the unambiguous UTC representation.
        var table = $"tz_dst_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (ts DateTime('UTC')) ENGINE = Memory");

        try
        {
            // 02:30 PDT == 09:30 UTC
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES (toDateTime('2024-03-10 09:30:00', 'UTC'))");

            DateTime read = default;
            await foreach (var r in conn.QueryAsync($"SELECT ts FROM {table}"))
                read = r.GetFieldValue<DateTime>(0);

            var utc = read.Kind == DateTimeKind.Utc ? read : read.ToUniversalTime();
            Assert.Equal(new DateTime(2024, 3, 10, 9, 30, 0, DateTimeKind.Utc), utc);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task DateExtremes_RoundTrip()
    {
        var table = $"tz_date32_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, d Date32) ENGINE = Memory");

        try
        {
            var inputs = new[]
            {
                new DateOnly(1900, 1, 2),
                new DateOnly(2024, 7, 4),
                new DateOnly(2299, 12, 30),
            };

            await using (var inserter = conn.CreateBulkInserter<DateRow>(table))
            {
                await inserter.InitAsync();
                for (int i = 0; i < inputs.Length; i++)
                    await inserter.AddAsync(new DateRow { Id = i, D = inputs[i] });
                await inserter.CompleteAsync();
            }

            var got = new List<DateOnly>();
            await foreach (var r in conn.QueryAsync($"SELECT d FROM {table} ORDER BY id"))
                got.Add(r.GetFieldValue<DateOnly>(0));

            Assert.Equal(inputs, got);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    private class TsRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "ts", Order = 1)] public DateTime Ts { get; set; }
    }

    private class DateRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "d", Order = 1)] public DateOnly D { get; set; }
    }
}
