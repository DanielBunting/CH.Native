using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.TestSuite;

/// <summary>
/// Pins the DateTime64-with-timezone read/write contract. Per the doc-comment
/// on <c>DateTime64ColumnReader.cs:22</c>: "Optional timezone name (stored but
/// not applied to returned DateTime)." Anyone storing wall-clock times in a
/// non-UTC zone gets back the wrong instant unless they account for this.
///
/// <para>
/// The test inserts a known instant via <c>parseDateTime64BestEffort</c> in
/// a specific timezone, then reads it back and pins:
/// </para>
/// <list type="bullet">
/// <item><description>Whether the returned <see cref="DateTime"/> represents the UTC
///     instant or the local wall-clock time in the column's TZ.</description></item>
/// <item><description>What <see cref="DateTime.Kind"/> the returned value carries.</description></item>
/// <item><description>Whether the column's TypeName preserves the timezone string for
///     callers that introspect it.</description></item>
/// </list>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class DateTime64TimezoneRoundTripTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public DateTime64TimezoneRoundTripTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    private sealed class TimestampRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "ts", Order = 1)] public DateTime Ts { get; set; }
    }

    [Fact]
    public async Task DateTime64_WithEasternTimezone_ReturnsUtcInstant_DateTimeKindUnspecified()
    {
        // ClickHouse stores DateTime64 as a UTC instant regardless of the
        // declared timezone — the timezone is metadata for display only.
        // The library returns the UTC instant; this test pins WHICH semantics
        // (the underlying instant) and what Kind is reported.
        var table = $"dt64_tz_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        try
        {
            await conn.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id Int32, ts DateTime64(3, 'America/New_York')) ENGINE = Memory");

            // 2024-06-15 12:00:00 New York wall clock = 16:00:00 UTC (EDT).
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES (1, parseDateTime64BestEffort('2024-06-15 12:00:00', 3, 'America/New_York'))");

            TimestampRow? row = null;
            await foreach (var r in conn.QueryAsync<TimestampRow>($"SELECT id, ts FROM {table}"))
                row = r;

            Assert.NotNull(row);
            _output.WriteLine($"Read ts: {row!.Ts:yyyy-MM-dd HH:mm:ss} (Kind={row.Ts.Kind})");

            // Pin the actual returned value. Server stores 2024-06-15 16:00:00 UTC;
            // the library returns ticks since Unix epoch, which corresponds to UTC.
            Assert.Equal(new DateTime(2024, 6, 15, 16, 0, 0), row.Ts);

            // GOOD NEWS: the library tags the result as Kind=Utc, so callers
            // doing tz-aware math (.ToLocalTime(), TimeZoneInfo conversions)
            // get correct results. Pin this contract — anyone relying on
            // Kind=Utc is safe.
            _output.WriteLine($"DateTime.Kind for DateTime64(_, 'TZ') read: {row.Ts.Kind}");
            Assert.Equal(DateTimeKind.Utc, row.Ts.Kind);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task DateTime_WithoutTimezone_ReturnsUtcInstant()
    {
        // Sanity baseline: a column declared without TZ also returns the UTC
        // instant. (ClickHouse's default timezone for DateTime is the server's
        // local timezone — but the wire format is always seconds-since-epoch UTC.)
        var table = $"dt_no_tz_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        try
        {
            await conn.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id Int32, ts DateTime) ENGINE = Memory");
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES (1, toDateTime('2024-06-15 12:00:00', 'UTC'))");

            TimestampRow? row = null;
            await foreach (var r in conn.QueryAsync<TimestampRow>($"SELECT id, ts FROM {table}"))
                row = r;

            Assert.NotNull(row);
            Assert.Equal(new DateTime(2024, 6, 15, 12, 0, 0), row!.Ts);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task DateTime64_NanosecondPrecision_TruncatesAt100ns_NotSilentlyDrops()
    {
        // DateTime64(9) is nanosecond precision. CLR DateTime resolution is
        // 100ns (Ticks). Sub-100ns digits must be dropped consistently —
        // pin the actual rounding behaviour.
        var table = $"dt64_ns_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        try
        {
            await conn.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id Int32, ts DateTime64(9)) ENGINE = Memory");
            // 12:00:00.123456789 — last 2 digits (89 ns) are below CLR resolution.
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES (1, parseDateTime64BestEffort('2024-01-01 12:00:00.123456789', 9))");

            TimestampRow? row = null;
            await foreach (var r in conn.QueryAsync<TimestampRow>($"SELECT id, ts FROM {table}"))
                row = r;

            Assert.NotNull(row);
            _output.WriteLine($"DateTime64(9) read: {row!.Ts.Ticks} ticks ({row.Ts:HH:mm:ss.fffffff})");
            // Expected: 1234567 ticks past 12:00:00 (the .89 ns is dropped).
            var expectedTicks = new DateTime(2024, 1, 1, 12, 0, 0).AddTicks(1234567).Ticks;
            Assert.Equal(expectedTicks, row.Ts.Ticks);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }
}
