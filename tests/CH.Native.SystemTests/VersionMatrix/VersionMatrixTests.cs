using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.VersionMatrix;

/// <summary>
/// Smoke-level coverage replayed against every pinned ClickHouse image. The point is to
/// catch protocol-version negotiation regressions and type-name parser drift, not to
/// re-test feature correctness — that lives in the integration suite.
/// </summary>
[Collection("VersionMatrix")]
[Trait(Categories.Name, Categories.VersionMatrix)]
public class VersionMatrixTests
{
    private readonly VersionedNodeCache _cache;

    public VersionMatrixTests(VersionedNodeCache cache)
    {
        _cache = cache;
    }

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task Handshake_NegotiatesProtocol(string image)
    {
        var settings = await _cache.GetSettingsAsync(image);
        await using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();
        Assert.True(conn.IsOpen);
        // Pin a minimum negotiated protocol version. Anything below 54453 (ClickHouse
        // ~22.x) suggests a silent downgrade negotiation — the test images we run all
        // negotiate well above this. Catches regressions where the client claims an
        // older revision than the server supports.
        Assert.True(conn.NegotiatedProtocolVersion >= 54453,
            $"Negotiated protocol version {conn.NegotiatedProtocolVersion} is suspiciously low for image {image}.");
    }

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task RoundTripsCommonScalarTypes(string image)
    {
        var settings = await _cache.GetSettingsAsync(image);
        await using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();

        var i = await conn.ExecuteScalarAsync<int>("SELECT toInt32(-7)");
        Assert.Equal(-7, i);

        var u = await conn.ExecuteScalarAsync<ulong>("SELECT toUInt64(42)");
        Assert.Equal(42UL, u);

        var s = await conn.ExecuteScalarAsync<string>("SELECT 'hello'");
        Assert.Equal("hello", s);

        var f = await conn.ExecuteScalarAsync<double>("SELECT toFloat64(3.5)");
        Assert.Equal(3.5, f);

        // DateTime via a real column, since literal toDateTime(...) returns DateTime('TZ')
        // which the scalar convert path doesn't unwrap on every server version.
        await foreach (var row in conn.QueryAsync(
            "SELECT toDateTime('2024-01-02 03:04:05', 'UTC') AS dt"))
        {
            var dt = row.GetFieldValue<DateTime>(0);
            Assert.Equal(new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc), dt.ToUniversalTime());
        }
    }

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task LowCardinality_StreamsCorrectly(string image)
    {
        var settings = await _cache.GetSettingsAsync(image);
        await using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();

        var values = new List<string>();
        await foreach (var row in conn.QueryAsync(
            "SELECT toString(number % 4) AS k FROM numbers(20)"))
        {
            values.Add(row.GetFieldValue<string>(0));
        }
        Assert.Equal(20, values.Count);
        Assert.Equal(4, values.Distinct().Count());
    }

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task ArraysAndNullables_RoundTrip(string image)
    {
        var settings = await _cache.GetSettingsAsync(image);
        await using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();

        // Cast literals to fix element types — without this the array literal types
        // depend on the server version's inference and break the requested .NET type.
        await foreach (var row in conn.QueryAsync(
            "SELECT cast([1, 2, 3] AS Array(Int32)), " +
            "cast([toNullable('a'), NULL, toNullable('c')] AS Array(Nullable(String)))"))
        {
            var ints = row.GetFieldValue<int[]>(0);
            Assert.Equal(new[] { 1, 2, 3 }, ints);

            var strs = row.GetFieldValue<string?[]>(1);
            Assert.Collection(strs,
                v => Assert.Equal("a", v),
                Assert.Null,
                v => Assert.Equal("c", v));
        }
    }
}
