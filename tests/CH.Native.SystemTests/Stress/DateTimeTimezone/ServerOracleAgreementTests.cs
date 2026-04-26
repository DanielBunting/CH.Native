using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using CH.Native.SystemTests.Stress.DateTimeTimezone.Helpers;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Stress.DateTimeTimezone;

/// <summary>
/// Cross-validates the library's <c>DateTime('tz')</c> reader (which returns
/// <see cref="DateTimeOffset"/> after a client-side <see cref="TimeZoneInfo"/>
/// conversion) against the server's <c>formatDateTime</c> oracle, for every curated
/// zone × every seasonal instant. Disagreement here would mean the client and the
/// server's tzdata have diverged for a real instant — a silent analytics-pipeline bug.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Stress)]
public class ServerOracleAgreementTests
{
    private static readonly DateTime[] SeasonalInstants =
    [
        new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc), // northern winter
        new DateTime(2024, 7, 15, 12, 0, 0, DateTimeKind.Utc), // northern summer
    ];

    private readonly SingleNodeFixture _fixture;
    private readonly ITestOutputHelper _output;

    public ServerOracleAgreementTests(SingleNodeFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Theory]
    [MemberData(nameof(TimezoneTestData.ZoneCases), MemberType = typeof(TimezoneTestData))]
    public async Task ClientReader_AgreesWith_ServerFormatDateTime(string zone, bool supported)
    {
        if (!supported)
        {
            _output.WriteLine($"SKIPPED: zone '{zone}' not resolvable on this host.");
            return;
        }

        // DateTime('tz') wire is UInt32 seconds — must stay in 1970..2106 range.
        var table = $"tz_oracle_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, ts DateTime('{zone}')) ENGINE = Memory");

        try
        {
            // Insert via SQL using server-side toDateTime so we don't round through DateTimeOffset
            // before the test runs — we want to see what the server stores natively.
            for (int i = 0; i < SeasonalInstants.Length; i++)
            {
                var lit = SeasonalInstants[i].ToString("yyyy-MM-dd HH:mm:ss",
                    System.Globalization.CultureInfo.InvariantCulture);
                await conn.ExecuteNonQueryAsync(
                    $"INSERT INTO {table} VALUES ({i}, toDateTime('{lit}', 'UTC'))");
            }

            for (int i = 0; i < SeasonalInstants.Length; i++)
            {
                DateTimeOffset clientRead = default;
                await foreach (var r in conn.QueryAsync($"SELECT ts FROM {table} WHERE id = {i}"))
                    clientRead = r.GetFieldValue<DateTimeOffset>(0);

                var serverRendering = await conn.ExecuteScalarAsync<string>(
                    $"SELECT formatDateTime(ts, '%Y-%m-%d %H:%i:%S', '{zone}') FROM {table} WHERE id = {i}");

                var clientRendering = clientRead.ToString("yyyy-MM-dd HH:mm:ss",
                    System.Globalization.CultureInfo.InvariantCulture);

                Assert.Equal(serverRendering, clientRendering);

                // Offset on the client should match the oracle's offset for the same UTC instant.
                Assert.Equal(TimezoneOracle.OffsetAt(SeasonalInstants[i], zone), clientRead.Offset);
            }
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }
}
