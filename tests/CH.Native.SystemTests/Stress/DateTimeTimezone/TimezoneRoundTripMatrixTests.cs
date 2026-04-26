using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using CH.Native.SystemTests.Stress.DateTimeTimezone.Helpers;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Stress.DateTimeTimezone;

/// <summary>
/// Cross-product matrix of curated IANA zones × curated UTC instants. For each cell:
/// round-trip a single row of <c>DateTime64(3, '{tz}')</c> and confirm that the wire
/// value (via <c>toUnixTimestamp64Milli</c>) and the .NET round-trip both match the
/// original UTC instant. The timezone parameter on <c>DateTime64</c> does not affect
/// the wire encoding — every instant is stored as a UTC tick — so a zone bug only
/// surfaces if the reader/writer accidentally applies a TZ shift.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Stress)]
public class TimezoneRoundTripMatrixTests
{
    private readonly SingleNodeFixture _fixture;
    private readonly ITestOutputHelper _output;

    public TimezoneRoundTripMatrixTests(SingleNodeFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Theory]
    [MemberData(nameof(TimezoneTestData.MatrixCases), MemberType = typeof(TimezoneTestData))]
    public async Task RoundTrip_DateTime64_3_AcrossZoneAndInstant(string zone, DateTime utcInstant, bool supported)
    {
        if (!supported)
        {
            _output.WriteLine($"SKIPPED: timezone '{zone}' is not resolvable via TimeZoneInfo on this host.");
            return;
        }

        var table = $"tz_matrix_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, ts DateTime64(3, '{zone}')) ENGINE = Memory");

        try
        {
            await using (var inserter = conn.CreateBulkInserter<TsRow>(table))
            {
                await inserter.InitAsync();
                await inserter.AddAsync(new TsRow { Id = 0, Ts = utcInstant });
                await inserter.CompleteAsync();
            }

            DateTime read = default;
            await foreach (var r in conn.QueryAsync($"SELECT ts FROM {table} ORDER BY id"))
                read = r.GetFieldValue<DateTime>(0);

            Assert.Equal(DateTimeKind.Utc, read.Kind);

            // DateTime64(3) keeps milliseconds; allow ≤1 ms fuzz (10_000 ticks).
            var deltaTicks = Math.Abs((utcInstant - read).Ticks);
            Assert.True(
                deltaTicks <= TimeSpan.TicksPerMillisecond,
                $"Drift in {zone} at {utcInstant:O}: read={read:O}, delta={deltaTicks} ticks");

            // Server oracle: wire-level millis equal client expectation.
            var serverMillis = await conn.ExecuteScalarAsync<long>(
                $"SELECT toUnixTimestamp64Milli(ts) FROM {table}");
            var expectedMillis = (long)(utcInstant - DateTime.UnixEpoch).TotalMilliseconds;
            Assert.Equal(expectedMillis, serverMillis);
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
}
