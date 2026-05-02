using System.Net;
using System.Numerics;
using CH.Native.Connection;
using CH.Native.Data.Dynamic;
using CH.Native.Data.Geo;
using CH.Native.Data.Variant;
using CH.Native.Mapping;
using CH.Native.Numerics;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.VersionMatrix;

/// <summary>
/// Cross-version round-trip coverage for the complex and "newer" wire types that
/// are most likely to drift between ClickHouse releases: <c>Map</c>, <c>Tuple</c>,
/// <c>IPv4/IPv6</c>, <c>UUID</c>, <c>Decimal128/256</c> via
/// <see cref="ClickHouseDecimal"/>, <c>DateTime64</c> with timezone, geometry,
/// and the experimental JSON / Dynamic / Variant types (gated to images that
/// support them). Existing version-matrix tests cover the scalar surface; this
/// class focuses on type-name and binary-format drift on composite types.
/// </summary>
[Collection("VersionMatrix")]
[Trait(Categories.Name, Categories.VersionMatrix)]
public class ComplexTypeMatrixTests
{
    private readonly VersionedNodeCache _cache;

    public ComplexTypeMatrixTests(VersionedNodeCache cache)
    {
        _cache = cache;
    }

    private async Task<ClickHouseConnection> OpenAsync(string image)
    {
        var settings = await _cache.GetSettingsAsync(image);
        var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();
        return conn;
    }

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task Map_RoundTrips(string image)
    {
        await using var conn = await OpenAsync(image);
        var table = $"map_matrix_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, m Map(String, Int32)) ENGINE = Memory");
        try
        {
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES " +
                "(1, map('a', 1, 'b', 2)), " +
                "(2, map()), " +
                "(3, map('only', 99))");

            var got = new Dictionary<int, Dictionary<string, int>>();
            await foreach (var r in conn.QueryAsync($"SELECT id, m FROM {table} ORDER BY id"))
            {
                got[r.GetFieldValue<int>(0)] = r.GetFieldValue<Dictionary<string, int>>(1);
            }
            Assert.Equal(2, got[1]["b"]);
            Assert.Empty(got[2]);
            Assert.Equal(99, got[3]["only"]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task Tuple_RoundTrips(string image)
    {
        await using var conn = await OpenAsync(image);
        var table = $"tuple_matrix_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, t Tuple(Int32, String, DateTime64(3, 'UTC'))) ENGINE = Memory");
        try
        {
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES " +
                "(1, (42, 'alpha', '2024-06-01 10:11:12.345')), " +
                "(2, (-7, '', '2030-12-31 23:59:59.999'))");

            var got = new List<object?[]>();
            await foreach (var r in conn.QueryAsync($"SELECT id, t.1, t.2, t.3 FROM {table} ORDER BY id"))
            {
                got.Add(new object?[]
                {
                    r.GetFieldValue<int>(0),
                    r.GetFieldValue<int>(1),
                    r.GetFieldValue<string>(2),
                    r.GetFieldValue<DateTime>(3),
                });
            }

            Assert.Equal(2, got.Count);
            Assert.Equal(42, got[0][1]);
            Assert.Equal("alpha", got[0][2]);
            Assert.Equal(-7, got[1][1]);
            Assert.Equal("", got[1][2]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task IPv4_And_IPv6_RoundTrip(string image)
    {
        await using var conn = await OpenAsync(image);
        var table = $"ip_matrix_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, v4 IPv4, v6 IPv6) ENGINE = Memory");
        try
        {
            var rows = new[]
            {
                new IpRow { Id = 1, V4 = IPAddress.Parse("10.0.0.1"),     V6 = IPAddress.Parse("::1") },
                new IpRow { Id = 2, V4 = IPAddress.Parse("192.168.1.42"), V6 = IPAddress.Parse("2001:db8::abcd") },
                new IpRow { Id = 3, V4 = IPAddress.Parse("0.0.0.0"),      V6 = IPAddress.IPv6Any },
            };

            await using (var ins = conn.CreateBulkInserter<IpRow>(table))
            {
                await ins.InitAsync();
                foreach (var r in rows) await ins.AddAsync(r);
                await ins.CompleteAsync();
            }

            var got = new List<IpRow>();
            await foreach (var r in conn.QueryAsync<IpRow>($"SELECT id, v4, v6 FROM {table} ORDER BY id"))
                got.Add(r);

            Assert.Equal(rows.Length, got.Count);
            for (int i = 0; i < rows.Length; i++)
            {
                Assert.Equal(rows[i].V4!, got[i].V4!);
                Assert.Equal(rows[i].V6!, got[i].V6!);
            }
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task Uuid_RoundTrips(string image)
    {
        await using var conn = await OpenAsync(image);
        var table = $"uuid_matrix_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, u UUID) ENGINE = Memory");
        try
        {
            var rows = Enumerable.Range(0, 16).Select(i => new UuidRow
            {
                Id = i,
                U = Guid.NewGuid(),
            }).ToList();

            await using (var ins = conn.CreateBulkInserter<UuidRow>(table))
            {
                await ins.InitAsync();
                foreach (var r in rows) await ins.AddAsync(r);
                await ins.CompleteAsync();
            }

            var got = new List<UuidRow>();
            await foreach (var r in conn.QueryAsync<UuidRow>($"SELECT id, u FROM {table} ORDER BY id"))
                got.Add(r);

            Assert.Equal(rows.Count, got.Count);
            for (int i = 0; i < rows.Count; i++)
                Assert.Equal(rows[i].U, got[i].U);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task Decimal128_BoundaryValues_RoundTrip(string image)
    {
        await using var conn = await OpenAsync(image);
        var table = $"d128_matrix_{Guid.NewGuid():N}";
        // Decimal128(P, S): P up to 38, S up to P. Use scale 10 for headroom.
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, v Decimal(38, 10)) ENGINE = Memory");
        try
        {
            // 38 nines / 10^10 = 9999999999999999999999999999.9999999999 (28 nines, decimal point, 10 nines)
            var maxMantissa = BigInteger.Parse(new string('9', 38));
            var values = new[]
            {
                ClickHouseDecimal.Zero,
                new ClickHouseDecimal(maxMantissa, 10),
                new ClickHouseDecimal(-maxMantissa, 10),
                new ClickHouseDecimal(BigInteger.Parse("12345678901234567890123456789"), 10),
            };
            for (int i = 0; i < values.Length; i++)
                await conn.ExecuteNonQueryAsync(
                    $"INSERT INTO {table} VALUES ({i}, '{values[i].ToString()}')");

            var got = new List<ClickHouseDecimal>();
            await foreach (var r in conn.QueryAsync($"SELECT v FROM {table} ORDER BY id"))
                got.Add(r.GetFieldValue<ClickHouseDecimal>(0));

            Assert.Equal(values.Length, got.Count);
            for (int i = 0; i < values.Length; i++)
                Assert.Equal(values[i], got[i]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task Decimal256_BeyondDotNetDecimalMax_RoundTrips(string image)
    {
        await using var conn = await OpenAsync(image);
        var table = $"d256_matrix_{Guid.NewGuid():N}";
        // Decimal256(76, 20): mantissa up to 10^76 - 1.
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, v Decimal(76, 20)) ENGINE = Memory");
        try
        {
            var maxMantissa = BigInteger.Parse(new string('9', 76));
            var values = new[]
            {
                new ClickHouseDecimal(maxMantissa, 20),
                new ClickHouseDecimal(-maxMantissa, 20),
                new ClickHouseDecimal(BigInteger.One, 20),
            };
            for (int i = 0; i < values.Length; i++)
                await conn.ExecuteNonQueryAsync(
                    $"INSERT INTO {table} VALUES ({i}, '{values[i].ToString()}')");

            var got = new List<ClickHouseDecimal>();
            await foreach (var r in conn.QueryAsync($"SELECT v FROM {table} ORDER BY id"))
                got.Add(r.GetFieldValue<ClickHouseDecimal>(0));

            Assert.Equal(values.Length, got.Count);
            for (int i = 0; i < values.Length; i++)
                Assert.Equal(values[i], got[i]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task DateTime64_WithTimezone_PreservesOffsetAcrossDstTransition(string image)
    {
        await using var conn = await OpenAsync(image);
        var table = $"dt64tz_matrix_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, " +
            "ts_utc DateTime64(3, 'UTC'), " +
            "ts_lon DateTime64(3, 'Europe/London'), " +
            "ts_la DateTime64(3, 'America/Los_Angeles')) ENGINE = Memory");
        try
        {
            // Three timestamps spanning a UK DST forward transition (2024-03-31 01:00 UTC).
            var moments = new DateTime[]
            {
                new(2024, 3, 31, 0, 30, 0, DateTimeKind.Utc),  // before BST starts
                new(2024, 3, 31, 1, 30, 0, DateTimeKind.Utc),  // inside the skipped local hour
                new(2024, 3, 31, 2, 30, 0, DateTimeKind.Utc),  // BST in effect
            };
            for (int i = 0; i < moments.Length; i++)
            {
                var iso = moments[i].ToString("yyyy-MM-dd HH:mm:ss.fff");
                await conn.ExecuteNonQueryAsync(
                    $"INSERT INTO {table} VALUES ({i}, " +
                    $"toDateTime64('{iso}', 3, 'UTC'), " +
                    $"toDateTime64('{iso}', 3, 'UTC'), " +
                    $"toDateTime64('{iso}', 3, 'UTC'))");
            }

            int idx = 0;
            await foreach (var r in conn.QueryAsync(
                $"SELECT id, ts_utc, ts_lon, ts_la FROM {table} ORDER BY id"))
            {
                var utc = r.GetFieldValue<DateTime>(1);
                var lon = r.GetFieldValue<DateTime>(2);
                var la = r.GetFieldValue<DateTime>(3);

                // Wall-clock differs between zones for the same UTC instant — but the
                // UTC representation must match the inserted moment exactly (sub-tick).
                Assert.Equal(moments[idx].Ticks, utc.ToUniversalTime().Ticks);
                Assert.Equal(moments[idx].Ticks, lon.ToUniversalTime().Ticks);
                Assert.Equal(moments[idx].Ticks, la.ToUniversalTime().Ticks);
                idx++;
            }
            Assert.Equal(moments.Length, idx);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task GeoPoint_RoundTrips(string image)
    {
        await using var conn = await OpenAsync(image);
        var table = $"geo_matrix_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, p Point) ENGINE = Memory");
        try
        {
            var pts = new[]
            {
                new Point(0.0, 0.0),
                new Point(-12.5, 7.25),
                new Point(180.0, -90.0),
            };
            for (int i = 0; i < pts.Length; i++)
                await conn.ExecuteNonQueryAsync(
                    $"INSERT INTO {table} VALUES ({i}, ({pts[i].X}, {pts[i].Y}))");

            var got = new List<Point>();
            await foreach (var r in conn.QueryAsync($"SELECT p FROM {table} ORDER BY id"))
                got.Add(r.GetFieldValue<Point>(0));

            Assert.Equal(pts, got);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Theory]
    [MemberData(nameof(SupportedImages.WithJsonType), MemberType = typeof(SupportedImages))]
    public async Task Json_RoundTrips_OnSupportedImages(string image)
    {
        await using var conn = await OpenAsync(image);
        await conn.ExecuteNonQueryAsync("SET allow_experimental_json_type = 1");
        var table = $"json_matrix_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, j JSON) ENGINE = Memory");
        try
        {
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES " +
                "(1, '{\"name\":\"alice\",\"age\":30}'), " +
                "(2, '{\"nested\":{\"k\":1}}')");

            var got = new List<string>();
            await foreach (var r in conn.QueryAsync(
                $"SELECT toString(j) FROM {table} ORDER BY id"))
            {
                got.Add(r.GetFieldValue<string>(0));
            }
            Assert.Equal(2, got.Count);
            Assert.Contains("alice", got[0]);
            Assert.Contains("nested", got[1]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Theory]
    [MemberData(nameof(SupportedImages.WithDynamicVariant), MemberType = typeof(SupportedImages))]
    public async Task Variant_RoundTrips_OnSupportedImages(string image)
    {
        await using var conn = await OpenAsync(image);
        await conn.ExecuteNonQueryAsync("SET allow_experimental_variant_type = 1");
        await conn.ExecuteNonQueryAsync("SET allow_suspicious_variant_types = 1");
        var table = $"variant_matrix_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, v Variant(Int64, String)) ENGINE = Memory");
        try
        {
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES " +
                "(1, 42), " +
                "(2, 'hello'), " +
                "(3, NULL)");

            var got = new List<string?>();
            await foreach (var r in conn.QueryAsync(
                $"SELECT if(isNull(v), NULL, toString(v)) FROM {table} ORDER BY id"))
            {
                got.Add(r.IsDBNull(0) ? null : r.GetFieldValue<string>(0));
            }
            Assert.Equal(3, got.Count);
            Assert.Equal("42", got[0]);
            Assert.Equal("hello", got[1]);
            Assert.Null(got[2]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Theory]
    [MemberData(nameof(SupportedImages.WithDynamicVariant), MemberType = typeof(SupportedImages))]
    public async Task Dynamic_RoundTrips_OnSupportedImages(string image)
    {
        await using var conn = await OpenAsync(image);
        await conn.ExecuteNonQueryAsync("SET allow_experimental_dynamic_type = 1");
        var table = $"dynamic_matrix_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, d Dynamic) ENGINE = Memory");
        try
        {
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES " +
                "(1, 42::Int64), " +
                "(2, 'hello'), " +
                "(3, NULL)");

            var got = new List<string?>();
            await foreach (var r in conn.QueryAsync(
                $"SELECT if(isNull(d), NULL, toString(d)) FROM {table} ORDER BY id"))
            {
                got.Add(r.IsDBNull(0) ? null : r.GetFieldValue<string>(0));
            }
            Assert.Equal(3, got.Count);
            Assert.Equal("42", got[0]);
            Assert.Equal("hello", got[1]);
            Assert.Null(got[2]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    private sealed class IpRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "v4", Order = 1)] public IPAddress? V4 { get; set; }
        [ClickHouseColumn(Name = "v6", Order = 2)] public IPAddress? V6 { get; set; }
    }

    private sealed class UuidRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "u", Order = 1)] public Guid U { get; set; }
    }
}
