using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using CH.Native.SystemTests.Stress.DateTimeTimezone.Helpers;
using Xunit;

namespace CH.Native.SystemTests.Stress.DateTimeTimezone;

/// <summary>
/// Spring-forward DST transition in <c>America/New_York</c>: 2026-03-08 02:00 local
/// jumps directly to 03:00, so 02:30 local does not exist. Tests pin behaviour at the
/// boundary: instants 1 second before and after are distinguishable, and server-local
/// rendering matches the client-side <see cref="TimeZoneInfo"/> oracle.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Stress)]
public class DstSpringForwardTests
{
    private const string Zone = "America/New_York";
    private readonly SingleNodeFixture _fixture;

    public DstSpringForwardTests(SingleNodeFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public void NonExistentLocalTime_ConvertTimeToUtc_ThrowsArgumentException()
    {
        // 02:30 local on the spring-forward day does not exist; .NET's contract is to
        // throw. The library implicitly relies on this — any caller that builds a Local
        // DateTime around this value and passes it through ToUniversalTime() should fail
        // loud rather than silently produce an unspecified instant.
        var tz = TimeZoneInfo.FindSystemTimeZoneById(Zone);
        var nonExistent = new DateTime(2026, 3, 8, 2, 30, 0, DateTimeKind.Unspecified);

        Assert.Throws<ArgumentException>(() => TimeZoneInfo.ConvertTimeToUtc(nonExistent, tz));
    }

    [Fact]
    public async Task OneSecondBeforeAndAfterSpringForward_DistinctInstants_RoundTrip()
    {
        // 06:59:59Z = 01:59:59 EST (last second before jump)
        // 07:00:00Z = 03:00:00 EDT (first second after jump)
        var beforeUtc = new DateTime(2026, 3, 8, 6, 59, 59, DateTimeKind.Utc);
        var afterUtc = new DateTime(2026, 3, 8, 7, 0, 0, DateTimeKind.Utc);

        var table = $"tz_spring_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, ts DateTime64(3, '{Zone}')) ENGINE = Memory");

        try
        {
            await using (var inserter = conn.CreateBulkInserter<TsRow>(table))
            {
                await inserter.InitAsync();
                await inserter.AddAsync(new TsRow { Id = 0, Ts = beforeUtc });
                await inserter.AddAsync(new TsRow { Id = 1, Ts = afterUtc });
                await inserter.CompleteAsync();
            }

            var read = new List<DateTime>();
            await foreach (var r in conn.QueryAsync($"SELECT ts FROM {table} ORDER BY id"))
                read.Add(r.GetFieldValue<DateTime>(0));

            Assert.Equal(2, read.Count);
            Assert.Equal(DateTimeKind.Utc, read[0].Kind);
            Assert.Equal(DateTimeKind.Utc, read[1].Kind);
            Assert.Equal(1.0, (read[1] - read[0]).TotalSeconds, precision: 3);

            // Server-local rendering: 01:59:59 EST then 03:00:00 EDT — note hour 02 is skipped.
            var beforeLocalRender = await conn.ExecuteScalarAsync<string>(
                $"SELECT formatDateTime(ts, '%H:%i:%S', '{Zone}') FROM {table} WHERE id = 0");
            var afterLocalRender = await conn.ExecuteScalarAsync<string>(
                $"SELECT formatDateTime(ts, '%H:%i:%S', '{Zone}') FROM {table} WHERE id = 1");

            Assert.Equal("01:59:59", beforeLocalRender);
            Assert.Equal("03:00:00", afterLocalRender);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task DstSpringForward_ServerLocalRendering_AgreesWithOracle()
    {
        // Three instants spanning the transition: well before, at the boundary, well after.
        var instants = new[]
        {
            new DateTime(2026, 3, 8,  6,  0,  0, DateTimeKind.Utc), // 01:00 EST
            new DateTime(2026, 3, 8,  7,  0,  0, DateTimeKind.Utc), // 03:00 EDT (the new "first" hour)
            new DateTime(2026, 3, 8, 12,  0,  0, DateTimeKind.Utc), // 08:00 EDT
        };

        var table = $"tz_spring_oracle_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, ts DateTime64(3, '{Zone}')) ENGINE = Memory");

        try
        {
            await using (var inserter = conn.CreateBulkInserter<TsRow>(table))
            {
                await inserter.InitAsync();
                for (int i = 0; i < instants.Length; i++)
                    await inserter.AddAsync(new TsRow { Id = i, Ts = instants[i] });
                await inserter.CompleteAsync();
            }

            for (int i = 0; i < instants.Length; i++)
            {
                var serverRendered = await conn.ExecuteScalarAsync<string>(
                    $"SELECT formatDateTime(ts, '%Y-%m-%d %H:%i:%S', '{Zone}') FROM {table} WHERE id = {i}");
                var oracleRendered = TimezoneOracle.LocalRendering(instants[i], Zone, "%Y-%m-%d %H:%i:%S");

                Assert.Equal(oracleRendered, serverRendered);
            }
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
