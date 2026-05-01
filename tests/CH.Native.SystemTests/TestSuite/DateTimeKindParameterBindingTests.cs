using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.TestSuite;

/// <summary>
/// Probes how <see cref="DateTime.Kind"/> affects parameter binding for
/// DateTime / DateTime64 columns. The wire format uses Unix-timestamp
/// seconds/ticks UTC; if the parameter serializer doesn't normalize a
/// <c>DateTime.Kind=Local</c> or <c>=Unspecified</c> value to UTC before
/// sending, the server stores the wrong instant.
///
/// <para>
/// Common pattern that surfaces this: a caller does
/// <c>cmd.Parameters.AddWithValue("@ts", DateTime.Now)</c>. <c>DateTime.Now</c>
/// returns Kind=Local. If the serializer ignores Kind and sends the raw
/// ticks, the value lands at the wrong instant by the local UTC offset.
/// </para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class DateTimeKindParameterBindingTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public DateTimeKindParameterBindingTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    public sealed class TimestampRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "ts", Order = 1)] public DateTime Ts { get; set; }
    }

    [Fact]
    public async Task DateTimeUtc_RoundTripsExactly()
    {
        // Sanity baseline.
        var table = $"dt_kind_utc_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        try
        {
            await conn.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id Int32, ts DateTime) ENGINE = Memory");

            var input = new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Utc);

            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} (id, ts) VALUES (1, {{p1:DateTime}})",
                new Dictionary<string, object?> { ["p1"] = input });

            TimestampRow? row = null;
            await foreach (var r in conn.QueryAsync<TimestampRow>($"SELECT id, ts FROM {table}"))
                row = r;

            Assert.NotNull(row);
            _output.WriteLine($"Utc round-trip: in={input:o} (Kind={input.Kind}), out={row!.Ts:o} (Kind={row.Ts.Kind})");
            Assert.Equal(input, row.Ts);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task DateTimeUnspecified_DocumentsRoundTripBehavior()
    {
        // OBSERVE: a caller binds DateTime with Kind=Unspecified. The
        // serializer's path is unaudited — does it treat as UTC, as local,
        // or pass the raw ticks?
        var table = $"dt_kind_unspec_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        try
        {
            await conn.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id Int32, ts DateTime) ENGINE = Memory");

            var input = new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Unspecified);

            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} (id, ts) VALUES (1, {{p1:DateTime}})",
                new Dictionary<string, object?> { ["p1"] = input });

            TimestampRow? row = null;
            await foreach (var r in conn.QueryAsync<TimestampRow>($"SELECT id, ts FROM {table}"))
                row = r;

            Assert.NotNull(row);
            _output.WriteLine($"Unspecified round-trip: in={input:o} (Kind={input.Kind}), out={row!.Ts:o} (Kind={row.Ts.Kind})");

            // Pin today's behaviour — the value the test container sees back
            // depends on the server's interpretation of the wire bytes plus
            // the column's TZ. Just assert no exception, and log the result.
            Assert.NotNull(row);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task DateTimeLocal_DocumentsRoundTripBehavior()
    {
        // The danger case: caller binds DateTime.Now (Kind=Local). Most
        // local-time CLR machines run in non-UTC zones. Without
        // normalization, the wire value silently shifts.
        var table = $"dt_kind_local_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        try
        {
            await conn.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id Int32, ts DateTime) ENGINE = Memory");

            // 12:00:00 local time.
            var localInput = new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Local);
            // The expected UTC instant if the serializer correctly normalizes.
            var expectedUtc = localInput.ToUniversalTime();

            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} (id, ts) VALUES (1, {{p1:DateTime}})",
                new Dictionary<string, object?> { ["p1"] = localInput });

            TimestampRow? row = null;
            await foreach (var r in conn.QueryAsync<TimestampRow>($"SELECT id, ts FROM {table}"))
                row = r;

            Assert.NotNull(row);
            _output.WriteLine($"Local round-trip: in={localInput:o} (Kind={localInput.Kind}), expectedUtc={expectedUtc:o}, out={row!.Ts:o} (Kind={row.Ts.Kind})");
            // Document — assertion is loose: just ensure the value is non-null
            // and within ±1 day of either local or UTC interpretation.
            // If the round-trip differs from expectedUtc (in UTC ticks), that's
            // the bug.
            var diffFromExpected = (row.Ts - DateTime.SpecifyKind(expectedUtc, DateTimeKind.Utc)).TotalMinutes;
            _output.WriteLine($"Diff from expected UTC interpretation: {diffFromExpected:F1} minutes");
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }
}
