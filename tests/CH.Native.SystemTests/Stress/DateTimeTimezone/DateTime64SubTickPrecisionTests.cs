using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Stress.DateTimeTimezone;

/// <summary>
/// DateTime64 has precision 0–9 (seconds → nanoseconds). .NET's DateTime is 100 ns
/// per tick — precisions 8 and 9 cannot round-trip exactly through CLR. These tests
/// pin: the wire encoding (via <c>toUnixTimestamp64*</c>), the truncation rule for
/// sub-tick precisions, and behaviour at negative wire values.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Stress)]
public class DateTime64SubTickPrecisionTests
{
    private readonly SingleNodeFixture _fixture;

    public DateTime64SubTickPrecisionTests(SingleNodeFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Precision0_SecondsRoundTrip_Exact()
    {
        var input = new DateTime(2023, 11, 14, 22, 13, 20, DateTimeKind.Utc);

        var table = $"tz_p0_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (ts DateTime64(0, 'UTC')) ENGINE = Memory");

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
            // Wire is unix seconds at precision 0.
            var seconds = await conn.ExecuteScalarAsync<long>($"SELECT toInt64(ts) FROM {table}");
            Assert.Equal(1700000000L, seconds);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task Precision3_MillisecondsRoundTrip_ExactWireValue()
    {
        // 12:34:56.789
        var input = new DateTime(2024, 6, 15, 12, 34, 56, 789, DateTimeKind.Utc);

        var table = $"tz_p3_{Guid.NewGuid():N}";
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

            var wireMillis = await conn.ExecuteScalarAsync<long>(
                $"SELECT toUnixTimestamp64Milli(ts) FROM {table}");
            var expected = (long)(input - DateTime.UnixEpoch).TotalMilliseconds;
            Assert.Equal(expected, wireMillis);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task Precision6_MicrosecondsRoundTrip_WithinOneTick()
    {
        // 12:34:56.789012
        var ticks = new DateTime(2024, 6, 15, 12, 34, 56, DateTimeKind.Utc).Ticks
                  + 789012 * (TimeSpan.TicksPerMillisecond / 1000);
        var input = new DateTime(ticks, DateTimeKind.Utc);

        var table = $"tz_p6_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (ts DateTime64(6, 'UTC')) ENGINE = Memory");

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

            Assert.True(Math.Abs((input - read).Ticks) <= 1);

            var wireMicros = await conn.ExecuteScalarAsync<long>(
                $"SELECT toUnixTimestamp64Micro(ts) FROM {table}");
            var expectedMicros = (input - DateTime.UnixEpoch).Ticks / 10;
            Assert.Equal(expectedMicros, wireMicros);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task Precision7_HundredNanosecondsRoundTrip_Exact()
    {
        // .NET tick = 100 ns. Precision 7 corresponds 1:1 with ticks, so round-trip
        // is exact even at the smallest unit.
        var input = new DateTime(2024, 6, 15, 12, 34, 56, DateTimeKind.Utc).AddTicks(1234567);

        var table = $"tz_p7_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (ts DateTime64(7, 'UTC')) ENGINE = Memory");

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
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task Precision8_TenNanosecondGranularity_TruncatesToHundredNs()
    {
        // 0.34567891 s = 34567891 wire units at precision 8.
        // CLR ticks = 3456789 (the trailing "1" 10-ns bit is dropped on read).
        var input = new DateTime(2024, 6, 15, 12, 34, 56, DateTimeKind.Utc).AddTicks(3456789);

        var table = $"tz_p8_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (ts DateTime64(8, 'UTC')) ENGINE = Memory");

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

            // CLR ticks should match exactly — we wrote a tick-aligned input.
            Assert.Equal(input, read);

            // Server-reported nanoseconds-since-epoch should equal ticks-since-epoch * 100.
            var nanos = await conn.ExecuteScalarAsync<long>(
                $"SELECT toUnixTimestamp64Nano(ts) FROM {table}");
            var expected = (input - DateTime.UnixEpoch).Ticks * 100;
            Assert.Equal(expected, nanos);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task Precision9_NanosecondGranularity_TruncatesToHundredNsOnRead()
    {
        // .NET DateTime cannot represent finer than 100 ns. Insert via SQL with a
        // sub-tick value (8-nanosecond residual) to verify the reader truncates rather
        // than rounds, and that a CLR-constructed DateTime round-trips exact.
        var clrInput = new DateTime(2024, 6, 15, 12, 34, 56, DateTimeKind.Utc).AddTicks(1234567);

        var table = $"tz_p9_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, ts DateTime64(9, 'UTC')) ENGINE = Memory");

        try
        {
            // Row 0 — CLR-aligned input via bulk insert.
            await using (var inserter = conn.CreateBulkInserter<TsRowWithId>(table))
            {
                await inserter.InitAsync();
                await inserter.AddAsync(new TsRowWithId { Id = 0, Ts = clrInput });
                await inserter.CompleteAsync();
            }

            // Row 1 — server-side value with 8 ns of sub-tick residual that CLR cannot represent.
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES (1, fromUnixTimestamp64Nano(toInt64(1718454896123456798)))");

            // Bulk-inserted row should round-trip exactly.
            var clrRead = default(DateTime);
            await foreach (var r in conn.QueryAsync($"SELECT ts FROM {table} WHERE id = 0"))
                clrRead = r.GetFieldValue<DateTime>(0);
            Assert.Equal(clrInput, clrRead);

            // Server-inserted row's wire matches the inserted nano value exactly.
            var nanoWire = await conn.ExecuteScalarAsync<long>(
                $"SELECT toUnixTimestamp64Nano(ts) FROM {table} WHERE id = 1");
            Assert.Equal(1718454896123456798L, nanoWire);

            // Reader truncates the trailing "98" ns to nearest 100 ns boundary (".1234567s").
            var serverRead = default(DateTime);
            await foreach (var r in conn.QueryAsync($"SELECT ts FROM {table} WHERE id = 1"))
                serverRead = r.GetFieldValue<DateTime>(0);

            var expectedTicks = (1718454896123456798L / 100); // truncated nanos → ticks
            var expectedDt = DateTime.UnixEpoch.AddTicks(expectedTicks);
            Assert.Equal(DateTimeKind.Utc, serverRead.Kind);
            Assert.Equal(expectedDt, serverRead);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task Precision9_PreEpoch_NegativeWire_RoundTrip()
    {
        // 1900-01-01 UTC predates the unix epoch by ~70 years; the Int64 wire is negative.
        var input = new DateTime(1900, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        var table = $"tz_p9_neg_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (ts DateTime64(9, 'UTC')) ENGINE = Memory");

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

            // Server confirms the wire is negative and corresponds to 1900-01-01.
            var rendered = await conn.ExecuteScalarAsync<string>(
                $"SELECT formatDateTime(ts, '%Y-%m-%d %H:%i:%S', 'UTC') FROM {table}");
            Assert.Equal("1900-01-01 00:00:00", rendered);
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
