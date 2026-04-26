using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Stress.DateTimeTimezone;

/// <summary>
/// Fall-back DST transition in <c>America/New_York</c>: 2026-11-01 02:00 local rolls
/// back to 01:00, so the wall-clock hour 01:00–02:00 occurs twice — once at UTC offset
/// -04:00 (EDT), once at -05:00 (EST). The two instants are distinct in UTC; the test
/// pins that the library preserves the distinction even though both render as 01:30 local.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Stress)]
public class DstFallBackTests
{
    private const string Zone = "America/New_York";
    private readonly SingleNodeFixture _fixture;

    public DstFallBackTests(SingleNodeFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task AmbiguousLocalTime_DistinctInstantsPreservedAsUtc()
    {
        // Both render as 01:30 local on 2026-11-01:
        //   05:30Z = 01:30 EDT (-04:00)  — first occurrence
        //   06:30Z = 01:30 EST (-05:00)  — second occurrence
        var firstUtc = new DateTime(2026, 11, 1, 5, 30, 0, DateTimeKind.Utc);
        var secondUtc = new DateTime(2026, 11, 1, 6, 30, 0, DateTimeKind.Utc);

        var table = $"tz_fall_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, ts DateTime64(3, '{Zone}')) ENGINE = Memory");

        try
        {
            await using (var inserter = conn.CreateBulkInserter<TsRow>(table))
            {
                await inserter.InitAsync();
                await inserter.AddAsync(new TsRow { Id = 0, Ts = firstUtc });
                await inserter.AddAsync(new TsRow { Id = 1, Ts = secondUtc });
                await inserter.CompleteAsync();
            }

            var read = new List<DateTime>();
            await foreach (var r in conn.QueryAsync($"SELECT ts FROM {table} ORDER BY id"))
                read.Add(r.GetFieldValue<DateTime>(0));

            Assert.Equal(firstUtc, read[0]);
            Assert.Equal(secondUtc, read[1]);
            Assert.Equal(1.0, (read[1] - read[0]).TotalHours, precision: 3);

            // Both must render as 01:30 local — proves the duplicate hour is real.
            var firstLocal = await conn.ExecuteScalarAsync<string>(
                $"SELECT formatDateTime(ts, '%H:%i:%S', '{Zone}') FROM {table} WHERE id = 0");
            var secondLocal = await conn.ExecuteScalarAsync<string>(
                $"SELECT formatDateTime(ts, '%H:%i:%S', '{Zone}') FROM {table} WHERE id = 1");

            Assert.Equal("01:30:00", firstLocal);
            Assert.Equal("01:30:00", secondLocal);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task QueryByLocalRange_ReturnsBothAmbiguousRows()
    {
        // Filtering on a local 01:00–02:00 range *should* return both occurrences of
        // 01:30 (EDT and EST), since both wall-clock-render inside that hour. ClickHouse's
        // toDateTime(., tz) collapses ambiguity to a single offset, so this query exposes
        // whether the server matches "all instants whose local rendering falls in the range"
        // or "all instants matching the canonical-offset interpretation of the range".
        // This test pins ClickHouse's actual behaviour — if it changes, we want to know.
        var firstUtc = new DateTime(2026, 11, 1, 5, 30, 0, DateTimeKind.Utc);  // 01:30 EDT
        var secondUtc = new DateTime(2026, 11, 1, 6, 30, 0, DateTimeKind.Utc); // 01:30 EST

        var table = $"tz_fall_q_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, ts DateTime64(3, '{Zone}')) ENGINE = Memory");

        try
        {
            await using (var inserter = conn.CreateBulkInserter<TsRow>(table))
            {
                await inserter.InitAsync();
                await inserter.AddAsync(new TsRow { Id = 0, Ts = firstUtc });
                await inserter.AddAsync(new TsRow { Id = 1, Ts = secondUtc });
                await inserter.CompleteAsync();
            }

            var matchedCount = await conn.ExecuteScalarAsync<ulong>(
                $@"SELECT count() FROM {table}
                   WHERE toDateTime(ts, '{Zone}') >= toDateTime('2026-11-01 01:00:00', '{Zone}')
                     AND toDateTime(ts, '{Zone}') <  toDateTime('2026-11-01 02:00:00', '{Zone}')");

            // Pin to whatever ClickHouse currently does. With current EDT-first canonicalisation
            // both rows match (each is in [05:00Z, 07:00Z)). If a future server changes the rule
            // this test fails and we revisit.
            Assert.Equal(2UL, matchedCount);
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
