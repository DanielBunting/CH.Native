using System.Diagnostics;
using CH.Native.Connection;
using Xunit;

namespace CH.Native.Tests.Unit.Connection;

/// <summary>
/// <see cref="ClickHouseConnection.KillQueryAsync"/> requires the query id to
/// be a GUID (KILL QUERY doesn't accept parameters, so the id is inlined).
/// Pre-fix the GUID validation ran after a full TCP handshake on a freshly
/// opened side-channel connection, burning the round-trip on a hopeless call.
/// </summary>
public class KillQueryValidationOrderTests
{
    [Fact]
    public async Task InvalidGuid_ThrowsImmediately_NoConnectionAttempt()
    {
        // Point at a non-routable host. If validation runs after OpenAsync the
        // test would hang on the connect timeout (multi-second). After the fix
        // the ArgumentException must surface in microseconds.
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("192.0.2.1") // RFC 5737 non-routable
            .WithPort(9000)
            .Build();

        await using var conn = new ClickHouseConnection(settings);

        var sw = Stopwatch.StartNew();
        await Assert.ThrowsAsync<ArgumentException>(
            () => conn.KillQueryAsync("not-a-guid"));
        sw.Stop();

        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(1),
            $"Validation took {sw.Elapsed.TotalMilliseconds:F0}ms — fix should reject before opening any connection.");
    }
}
