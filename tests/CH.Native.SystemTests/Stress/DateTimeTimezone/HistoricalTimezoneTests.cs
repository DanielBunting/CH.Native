using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using CH.Native.SystemTests.Stress.DateTimeTimezone.Helpers;
using Xunit;

namespace CH.Native.SystemTests.Stress.DateTimeTimezone;

/// <summary>
/// Historical timezone rule shifts. ClickHouse and .NET maintain independent timezone
/// databases (server: container's tzdata; client: ICU on macOS/Linux, registry on
/// Windows). When historical rules change between releases, the two diverge — this
/// test surfaces the disagreement loudly rather than silently corrupting analytics.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Stress)]
public class HistoricalTimezoneTests
{
    private readonly SingleNodeFixture _fixture;

    public HistoricalTimezoneTests(SingleNodeFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task MoscowPre2014_RoundTripExact_AndOracleAgrees()
    {
        // Russia abolished DST and shifted permanently to UTC+3 in 2014. Before then
        // Moscow was UTC+4 in summer. The instant 2013-06-01 12:00 UTC therefore renders
        // either as 16:00 Moscow (pre-2014 rule) or 15:00 (current rule), depending on
        // whose tzdata is consulted. We don't pin which value — we pin that client and
        // server agree.
        const string Zone = "Europe/Moscow";
        var instant = new DateTime(2013, 6, 1, 12, 0, 0, DateTimeKind.Utc);

        var table = $"tz_moscow_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (ts DateTime64(3, '{Zone}')) ENGINE = Memory");

        try
        {
            await using (var inserter = conn.CreateBulkInserter<TsRow>(table))
            {
                await inserter.InitAsync();
                await inserter.AddAsync(new TsRow { Ts = instant });
                await inserter.CompleteAsync();
            }

            DateTime read = default;
            await foreach (var r in conn.QueryAsync($"SELECT ts FROM {table}"))
                read = r.GetFieldValue<DateTime>(0);
            Assert.Equal(instant, read);

            var serverRendered = await conn.ExecuteScalarAsync<string>(
                $"SELECT formatDateTime(ts, '%H:%i', '{Zone}') FROM {table}");
            var oracleRendered = TimezoneOracle.LocalRendering(instant, Zone, "%H:%i");

            Assert.Equal(oracleRendered, serverRendered);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task LordHowe_HalfHourDst_ShiftIs30Minutes()
    {
        // Lord Howe Island uses 30-minute DST (+10:30 standard, +11:00 summer). The
        // southern-hemisphere transition out of DST happens on the first Sunday of
        // April at 02:00 local DST → 01:30 local standard. On 2026 that's April 5,
        // and the corresponding UTC instant is April 4 15:00. Pick two UTC instants
        // straddling that moment, 2 h apart, and show the local diff is 1.5 h — half
        // an hour shorter, because the offset shrunk by 30 min.
        const string Zone = "Australia/Lord_Howe";

        if (!TimezoneTestData.IsSupportedOnHost(Zone))
            return; // host platform skip; matrix test will already report the gap.

        var beforeBoundaryUtc = new DateTime(2026, 4, 4, 14, 0, 0, DateTimeKind.Utc); // 01:00 LHDT (+11:00)
        var afterBoundaryUtc = new DateTime(2026, 4, 4, 16, 0, 0, DateTimeKind.Utc);  // 02:30 LHST (+10:30)

        var table = $"tz_lord_howe_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, ts DateTime64(3, '{Zone}')) ENGINE = Memory");

        try
        {
            await using (var inserter = conn.CreateBulkInserter<TsRowWithId>(table))
            {
                await inserter.InitAsync();
                await inserter.AddAsync(new TsRowWithId { Id = 0, Ts = beforeBoundaryUtc });
                await inserter.AddAsync(new TsRowWithId { Id = 1, Ts = afterBoundaryUtc });
                await inserter.CompleteAsync();
            }

            // Asking the server for the local-time difference shows the half-hour shift.
            var beforeLocal = await conn.ExecuteScalarAsync<string>(
                $"SELECT formatDateTime(ts, '%Y-%m-%d %H:%i:%S', '{Zone}') FROM {table} WHERE id = 0");
            var afterLocal = await conn.ExecuteScalarAsync<string>(
                $"SELECT formatDateTime(ts, '%Y-%m-%d %H:%i:%S', '{Zone}') FROM {table} WHERE id = 1");

            var beforeParsed = DateTime.ParseExact(beforeLocal!, "yyyy-MM-dd HH:mm:ss",
                System.Globalization.CultureInfo.InvariantCulture);
            var afterParsed = DateTime.ParseExact(afterLocal!, "yyyy-MM-dd HH:mm:ss",
                System.Globalization.CultureInfo.InvariantCulture);

            // 2 h UTC apart, with a -0:30 DST shift in the middle, gives 1 h 30 min local apart.
            Assert.Equal(TimeSpan.FromMinutes(90), afterParsed - beforeParsed);
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

    private class TsRowWithId
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "ts", Order = 1)] public DateTime Ts { get; set; }
    }
}
