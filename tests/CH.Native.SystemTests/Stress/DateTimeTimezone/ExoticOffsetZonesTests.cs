using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using CH.Native.SystemTests.Stress.DateTimeTimezone.Helpers;
using Xunit;

namespace CH.Native.SystemTests.Stress.DateTimeTimezone;

/// <summary>
/// Exotic offsets: +14, +12:45, +5:45, POSIX-inverted Etc/GMT zones, and unknown
/// zone error handling. These are the shapes most likely to break on a TZ database
/// version mismatch or in code paths that assume whole-hour offsets.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Stress)]
public class ExoticOffsetZonesTests
{
    private readonly SingleNodeFixture _fixture;

    public ExoticOffsetZonesTests(SingleNodeFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Kiritimati_Utc14_RoundTripExact_AndLocalIs14HoursAhead()
    {
        const string Zone = "Pacific/Kiritimati";
        if (!TimezoneTestData.IsSupportedOnHost(Zone))
            return;

        // 2024-06-15 00:00 UTC → 2024-06-15 14:00 Kiritimati (UTC+14, no DST).
        var instant = new DateTime(2024, 6, 15, 0, 0, 0, DateTimeKind.Utc);
        var rendered = await RoundTripAndRender(instant, Zone, precision: 3);
        Assert.Equal("2024-06-15 14:00:00", rendered);
    }

    [Fact]
    public async Task Chatham_QuarterHourOffset_RoundTrip()
    {
        const string Zone = "Pacific/Chatham";
        if (!TimezoneTestData.IsSupportedOnHost(Zone))
            return;

        // Pacific/Chatham is +12:45 NZ standard, +13:45 in summer (NZ summer = December).
        // 2024-12-15 00:00 UTC → 13:45 local (summer).
        var instant = new DateTime(2024, 12, 15, 0, 0, 0, DateTimeKind.Utc);
        var rendered = await RoundTripAndRender(instant, Zone, precision: 3);
        Assert.Equal("2024-12-15 13:45:00", rendered);
    }

    [Fact]
    public async Task Kathmandu_QuarterHourOffset_RoundTrip()
    {
        const string Zone = "Asia/Kathmandu";
        if (!TimezoneTestData.IsSupportedOnHost(Zone))
            return;

        // Asia/Kathmandu is +5:45 year-round.
        // 2024-06-15 00:00 UTC → 05:45 local.
        var instant = new DateTime(2024, 6, 15, 0, 0, 0, DateTimeKind.Utc);
        var rendered = await RoundTripAndRender(instant, Zone, precision: 3);
        Assert.Equal("2024-06-15 05:45:00", rendered);
    }

    [Fact]
    public async Task EtcGmtPlus12_PosixInverted_LocalIs12HoursBehindUtc()
    {
        // Etc/GMT+12 is POSIX-style, where positive sign means *west* of Greenwich,
        // i.e. UTC-12. 2024-06-15 12:00 UTC therefore renders as 2024-06-15 00:00 local.
        const string Zone = "Etc/GMT+12";
        if (!TimezoneTestData.IsSupportedOnHost(Zone))
            return;

        var instant = new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Utc);
        var rendered = await RoundTripAndRender(instant, Zone, precision: 3);
        Assert.Equal("2024-06-15 00:00:00", rendered);
    }

    [Fact]
    public async Task UnknownTimezone_LibraryReaderOrServerRejects()
    {
        // ClickHouse's own behaviour for an unknown zone in the type signature is to
        // reject the CREATE TABLE. If a future server version becomes lenient, our
        // reader's TimeZoneInfo lookup would still throw — assert that *some* layer
        // rejects rather than silently producing wrong results.
        var table = $"tz_unknown_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        var rejectedAtCreate = false;
        var rejectedAtRead = false;

        try
        {
            try
            {
                await conn.ExecuteNonQueryAsync(
                    $"CREATE TABLE {table} (ts DateTime('NotAReal/Zone')) ENGINE = Memory");
            }
            catch (Exception)
            {
                rejectedAtCreate = true;
            }

            if (!rejectedAtCreate)
            {
                await conn.ExecuteNonQueryAsync(
                    $"INSERT INTO {table} VALUES (toDateTime('2024-06-15 12:00:00', 'UTC'))");

                try
                {
                    await foreach (var _ in conn.QueryAsync($"SELECT ts FROM {table}"))
                    {
                        // Materialising any row triggers the column reader's TZ lookup.
                    }
                }
                catch (TimeZoneNotFoundException)
                {
                    rejectedAtRead = true;
                }
                catch (Exception)
                {
                    rejectedAtRead = true;
                }
            }

            Assert.True(rejectedAtCreate || rejectedAtRead,
                "Expected an unknown timezone to be rejected by either the server or the column reader.");
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    private async Task<string?> RoundTripAndRender(DateTime utcInstant, string zone, int precision)
    {
        var table = $"tz_exotic_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (ts DateTime64({precision}, '{zone}')) ENGINE = Memory");

        try
        {
            await using (var inserter = conn.CreateBulkInserter<TsRow>(table))
            {
                await inserter.InitAsync();
                await inserter.AddAsync(new TsRow { Ts = utcInstant });
                await inserter.CompleteAsync();
            }

            DateTime read = default;
            await foreach (var r in conn.QueryAsync($"SELECT ts FROM {table}"))
                read = r.GetFieldValue<DateTime>(0);
            Assert.Equal(utcInstant, read);

            return await conn.ExecuteScalarAsync<string>(
                $"SELECT formatDateTime(ts, '%Y-%m-%d %H:%i:%S', '{zone}') FROM {table}");
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
