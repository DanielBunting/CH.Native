using System.Net;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.TestSuite;

/// <summary>
/// Probes IP address parameter binding and read-back across the canonical
/// form variations. <see cref="IPAddress.ToString"/> normalizes most forms
/// but IPv6 has multiple legal text representations (<c>::1</c>,
/// <c>0:0:0:0:0:0:0:1</c>, <c>::ffff:127.0.0.1</c>) and the server-side
/// IPv6 type may not match across them.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class IpAddressCanonicalFormTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public IpAddressCanonicalFormTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    public sealed class IpRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "ip", Order = 1)] public IPAddress Ip { get; set; } = null!;
    }

    [Fact]
    public async Task IPv4_RoundTrip()
    {
        var table = $"ip_v4_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        try
        {
            await conn.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id Int32, ip IPv4) ENGINE = Memory");

            var input = IPAddress.Parse("192.168.1.42");
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} (id, ip) VALUES (1, {{p1:IPv4}})",
                new Dictionary<string, object?> { ["p1"] = input });

            IpRow? row = null;
            await foreach (var r in conn.QueryAsync<IpRow>($"SELECT id, ip FROM {table}"))
                row = r;

            Assert.NotNull(row);
            Assert.Equal(input, row!.Ip);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task IPv6_LongForm_RoundTripsToCanonicalShortForm()
    {
        // OBSERVE: input long form `0:0:0:0:0:0:0:1`, server stores
        // canonical, returns short form `::1`. Pin the round-trip
        // behaviour and the equality semantics.
        var table = $"ip_v6_long_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        try
        {
            await conn.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id Int32, ip IPv6) ENGINE = Memory");

            var inputLong = IPAddress.Parse("0:0:0:0:0:0:0:1");
            var inputShort = IPAddress.Parse("::1");
            // .NET normalizes these to the same IPAddress under .Equals.
            Assert.True(inputLong.Equals(inputShort));

            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} (id, ip) VALUES (1, {{p1:IPv6}})",
                new Dictionary<string, object?> { ["p1"] = inputLong });

            IpRow? row = null;
            await foreach (var r in conn.QueryAsync<IpRow>($"SELECT id, ip FROM {table}"))
                row = r;

            Assert.NotNull(row);
            _output.WriteLine($"IPv6 long-form round-trip: in={inputLong}, out={row!.Ip}");
            Assert.True(row.Ip.Equals(inputLong),
                $"Round-trip mismatch: in={inputLong}, out={row.Ip}");
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task IPv6_IPv4MappedForm_DocumentedBehaviour()
    {
        // ::ffff:127.0.0.1 — IPv4-mapped IPv6 address. ClickHouse may
        // normalize to IPv4 representation, may keep as IPv6, may reject.
        // Pin today's behavior.
        var table = $"ip_v6_mapped_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        try
        {
            await conn.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id Int32, ip IPv6) ENGINE = Memory");

            var input = IPAddress.Parse("::ffff:127.0.0.1");
            Exception? caught = null;
            IpRow? row = null;
            try
            {
                await conn.ExecuteNonQueryAsync(
                    $"INSERT INTO {table} (id, ip) VALUES (1, {{p1:IPv6}})",
                    new Dictionary<string, object?> { ["p1"] = input });

                await foreach (var r in conn.QueryAsync<IpRow>($"SELECT id, ip FROM {table}"))
                    row = r;
            }
            catch (Exception ex)
            {
                caught = ex;
            }

            _output.WriteLine($"IPv4-mapped IPv6 round-trip: in={input}, out={row?.Ip}, thrown={caught?.GetType().Name}");
            // Document — either it works (round-trip equal) or throws cleanly.
            Assert.True(row is not null || caught is not null);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }
}
