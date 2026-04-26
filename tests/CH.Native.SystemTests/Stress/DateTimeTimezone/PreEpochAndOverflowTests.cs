using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Stress.DateTimeTimezone;

/// <summary>
/// Boundary cases at the edges of the wire encodings:
/// - <c>DateTime64</c> uses <see cref="long"/> wire and supports negative (pre-epoch) values.
/// - Legacy <c>DateTime</c> uses <see cref="uint"/> wire and is bounded to
///   1970-01-01 .. 2106-02-07 UTC. The current writer at
///   <c>DateTimeColumnWriter.cs:33</c> silently clamps values outside that range —
///   this is a latent correctness hazard. Two tests here pin the *desired* contract
///   (throw rather than clamp) and are expected to fail until the writer is fixed.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Stress)]
public class PreEpochAndOverflowTests
{
    private readonly SingleNodeFixture _fixture;

    public PreEpochAndOverflowTests(SingleNodeFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task DateTime64_PreEpoch_NegativeWire_RoundTrip()
    {
        var input = new DateTime(1900, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        var table = $"tz_pre_epoch_{Guid.NewGuid():N}";
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
            Assert.Equal(input, read);

            // toUnixTimestamp64Milli is signed — pre-epoch values come back negative.
            var wireMillis = await conn.ExecuteScalarAsync<long>(
                $"SELECT toUnixTimestamp64Milli(ts) FROM {table}");
            Assert.True(wireMillis < 0, $"Expected negative wire for pre-epoch instant, got {wireMillis}");

            var rendered = await conn.ExecuteScalarAsync<string>(
                $"SELECT formatDateTime(ts, '%Y-%m-%d %H:%i:%S', 'UTC') FROM {table}");
            Assert.Equal("1900-01-01 00:00:00", rendered);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    [Trait("Status", "PendingFix")]
    public async Task DateTime_LegacyPreEpoch_ShouldThrow_NotSilentlyClamp()
    {
        // Pinning the *desired* contract: pre-1970 instants on legacy DateTime should
        // throw rather than silently produce 1970-01-01. Today's writer at
        // DateTimeColumnWriter.cs:33 clamps via Math.Max(0, ...). When this test fails
        // it surfaces the silent clamp as a real bug.
        var input = new DateTime(1969, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        var table = $"tz_legacy_pre_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (ts DateTime) ENGINE = Memory");

        try
        {
            await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await using var inserter = conn.CreateBulkInserter<TsRow>(table);
                await inserter.InitAsync();
                await inserter.AddAsync(new TsRow { Ts = input });
                await inserter.CompleteAsync();
            });
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    [Trait("Status", "PendingFix")]
    public async Task DateTime_LegacyPostOverflow_ShouldThrow_NotSilentlyClamp()
    {
        // 2200-01-01 is past the UInt32 boundary (2106-02-07). Today the writer clamps
        // to uint.MaxValue. Desired: throw.
        var input = new DateTime(2200, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        var table = $"tz_legacy_post_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (ts DateTime) ENGINE = Memory");

        try
        {
            await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await using var inserter = conn.CreateBulkInserter<TsRow>(table);
                await inserter.InitAsync();
                await inserter.AddAsync(new TsRow { Ts = input });
                await inserter.CompleteAsync();
            });
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task DateTime64_Year9999_UpperBound_RoundTrip()
    {
        // CLR DateTime.MaxValue is 9999-12-31 23:59:59.9999999. Test a near-max value
        // round-trips through DateTime64(3, 'UTC'). ClickHouse's own DateTime64 max is
        // around 2299, so 9999 may overflow on the server — pin the actual behaviour.
        var input = new DateTime(2299, 12, 30, 23, 59, 59, 999, DateTimeKind.Utc);

        var table = $"tz_far_future_{Guid.NewGuid():N}";
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

            Assert.True(Math.Abs((input - read).Ticks) <= TimeSpan.TicksPerMillisecond);
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
