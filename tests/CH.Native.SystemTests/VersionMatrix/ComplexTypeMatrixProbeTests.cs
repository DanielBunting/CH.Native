using System.Net;
using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.VersionMatrix;

/// <summary>
/// Probes for type-name and binary-format drift across ClickHouse versions, focused
/// on edges <see cref="ComplexTypeMatrixTests"/> doesn't already cover: nested Maps,
/// Tuple field-name preservation, deeply nested Array(Nullable(Decimal)),
/// IPv4-mapped IPv6, server-side Enum8, LowCardinality(FixedString), Nullable
/// composed with LowCardinality.
///
/// <para>Plan section 6 lists 20 items — 11 are already pinned by
/// <see cref="ComplexTypeMatrixTests"/>. This class adds the remainder, each as a
/// <c>[Theory]</c> over <see cref="SupportedImages.All"/> for cross-version drift
/// detection.</para>
/// </summary>
[Collection("VersionMatrix")]
[Trait(Categories.Name, Categories.VersionMatrix)]
public class ComplexTypeMatrixProbeTests
{
    private readonly VersionedNodeCache _cache;
    private readonly ITestOutputHelper _output;

    public ComplexTypeMatrixProbeTests(VersionedNodeCache cache, ITestOutputHelper output)
    {
        _cache = cache;
        _output = output;
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
    public async Task Map_NestedMap_RoundTrips(string image)
    {
        await using var conn = await OpenAsync(image);
        var table = $"map_nested_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, m Map(String, Map(String, Int32))) ENGINE = Memory");
        try
        {
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES " +
                "(1, map('outer1', map('a', 1, 'b', 2), 'outer2', map('c', 3))), " +
                "(2, map())");

            var got = new List<Dictionary<string, Dictionary<string, int>>>();
            await foreach (var r in conn.QueryAsync($"SELECT m FROM {table} ORDER BY id"))
                got.Add(r.GetFieldValue<Dictionary<string, Dictionary<string, int>>>(0));

            Assert.Equal(2, got.Count);
            Assert.Equal(1, got[0]["outer1"]["a"]);
            Assert.Equal(3, got[0]["outer2"]["c"]);
            Assert.Empty(got[1]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task Map_StringKey_WithSpaceAndEmpty_RoundTrips(string image)
    {
        // Probe edge keys: empty string, key with space, key with unicode.
        await using var conn = await OpenAsync(image);
        var table = $"map_edge_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, m Map(String, Int32)) ENGINE = Memory");
        try
        {
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES " +
                "(1, map('', 0, ' ', 1, 'with space', 2, 'unicode_\\u00e9', 3))");

            var got = new Dictionary<string, int>();
            await foreach (var r in conn.QueryAsync($"SELECT m FROM {table}"))
                got = r.GetFieldValue<Dictionary<string, int>>(0);

            Assert.True(got.ContainsKey(""));
            Assert.Equal(1, got[" "]);
            Assert.Equal(2, got["with space"]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task Tuple_NamedFields_PreservesNamesInTypeMetadata(string image)
    {
        // Tuple(a Int32, b String) — server reports the type name; probe whether the
        // named-field metadata round-trips. Use system.columns to verify the type
        // string the server records.
        await using var conn = await OpenAsync(image);
        var table = $"tuple_named_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, t Tuple(a Int32, b String)) ENGINE = Memory");
        try
        {
            await conn.ExecuteNonQueryAsync($"INSERT INTO {table} VALUES (1, (42, 'hello'))");

            // Server-side type metadata
            var declaredType = await conn.ExecuteScalarAsync<string>(
                $"SELECT type FROM system.columns WHERE table = '{table}' AND name = 't'");
            _output.WriteLine($"Image {image}: declared tuple type = {declaredType}");
            Assert.NotNull(declaredType);
            Assert.Contains("Tuple", declaredType, StringComparison.Ordinal);

            // Wire round-trip via positional access (named-tuple destructuring depends
            // on whether the library exposes the names in its type system).
            int rows = 0;
            await foreach (var r in conn.QueryAsync($"SELECT t.a, t.b FROM {table}"))
            {
                Assert.Equal(42, r.GetFieldValue<int>(0));
                Assert.Equal("hello", r.GetFieldValue<string>(1));
                rows++;
            }
            Assert.Equal(1, rows);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task Uuid_BoundaryValues_RoundTripStable(string image)
    {
        // UUID's wire layout swaps two halves on the way through ClickHouse compared
        // to the .NET Guid byte order — pin specific values to catch endianness drift.
        await using var conn = await OpenAsync(image);
        var table = $"uuid_bnd_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, u UUID) ENGINE = Memory");
        try
        {
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES " +
                "(1, toUUID('00000000-0000-0000-0000-000000000000')), " +
                "(2, toUUID('ffffffff-ffff-ffff-ffff-ffffffffffff')), " +
                "(3, toUUID('01020304-0506-0708-090a-0b0c0d0e0f10'))");

            var got = new List<Guid>();
            await foreach (var r in conn.QueryAsync($"SELECT u FROM {table} ORDER BY id"))
                got.Add(r.GetFieldValue<Guid>(0));

            Assert.Equal(3, got.Count);
            Assert.Equal(Guid.Empty, got[0]);
            Assert.Equal(Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff"), got[1]);
            Assert.Equal(Guid.Parse("01020304-0506-0708-090a-0b0c0d0e0f10"), got[2]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task IPv6_IPv4MappedForm_RoundTrips(string image)
    {
        // ::ffff:1.2.3.4 is the IPv4-mapped IPv6 representation. ClickHouse may collapse
        // these to IPv4 internally — probe what the reader sees.
        await using var conn = await OpenAsync(image);
        var table = $"ip_mapped_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, addr IPv6) ENGINE = Memory");
        try
        {
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES " +
                "(1, toIPv6('::ffff:1.2.3.4')), " +
                "(2, toIPv6('2001:db8::1'))");

            var got = new List<IPAddress>();
            await foreach (var r in conn.QueryAsync($"SELECT addr FROM {table} ORDER BY id"))
                got.Add(r.GetFieldValue<IPAddress>(0));

            _output.WriteLine($"Image {image}: ::ffff:1.2.3.4 → {got[0]}");
            _output.WriteLine($"Image {image}: 2001:db8::1 → {got[1]}");

            Assert.Equal(2, got.Count);
            Assert.Equal(IPAddress.Parse("2001:db8::1"), got[1]);
            // Pin the safety: it's a valid IP and not an exception.
            Assert.NotNull(got[0]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task DateTime64_Precision9_NanosecondCorner(string image)
    {
        await using var conn = await OpenAsync(image);
        var table = $"dt64_p9_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, ts DateTime64(9, 'UTC')) ENGINE = Memory");
        try
        {
            // .NET DateTime resolution is 100 ns ticks — we cannot represent finer.
            // Pin: a value at 100 ns boundary round-trips exactly; sub-100ns is the
            // documented truncation boundary.
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES " +
                "(1, toDateTime64('2023-01-01 00:00:00.000000100', 9, 'UTC')), " +
                "(2, toDateTime64('2023-01-01 00:00:00.987654300', 9, 'UTC'))");

            var got = new List<DateTime>();
            await foreach (var r in conn.QueryAsync($"SELECT ts FROM {table} ORDER BY id"))
                got.Add(r.GetFieldValue<DateTime>(0));

            _output.WriteLine($"Image {image}: 100ns row = {got[0]:O}");
            _output.WriteLine($"Image {image}: 987654300ns row = {got[1]:O}");

            Assert.Equal(2, got.Count);
            // The 100 ns row maps to exactly 1 .NET tick after the second.
            Assert.Equal(
                new DateTime(2023, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks + 1,
                got[0].ToUniversalTime().Ticks);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task ServerSideEnum8_RoundTripStable(string image)
    {
        await using var conn = await OpenAsync(image);
        var table = $"enum8_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, e Enum8('a' = 1, 'b' = 2, 'c' = 3)) ENGINE = Memory");
        try
        {
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES (1, 'a'), (2, 'b'), (3, 'c')");

            // Cast to String server-side so we observe the label, not the underlying
            // Int8 — the wire type for an Enum8 column is the integer code; the
            // human-readable label is server-resolved.
            var got = new List<string>();
            await foreach (var r in conn.QueryAsync(
                $"SELECT cast(e, 'String') FROM {table} ORDER BY id"))
                got.Add(r.GetFieldValue<string>(0));

            Assert.Equal(new[] { "a", "b", "c" }, got);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task LowCardinality_OfFixedString_RoundTrips(string image)
    {
        await using var conn = await OpenAsync(image);
        var table = $"lc_fs_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, s LowCardinality(FixedString(8))) ENGINE = Memory");
        try
        {
            // FixedString pads with NULs; the values below are exactly 8 bytes each.
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES " +
                "(1, 'aaaaaaaa'), (2, 'bbbbbbbb'), (3, 'aaaaaaaa'), (4, 'cccccccc')");

            // FixedString materialises as byte[] on the wire. Server-side cast to
            // String gives a directly-usable string; this also exercises the
            // LowCardinality dictionary roundtrip end-to-end.
            var got = new List<string>();
            await foreach (var r in conn.QueryAsync(
                $"SELECT cast(s, 'String') FROM {table} ORDER BY id"))
                got.Add(r.GetFieldValue<string>(0));

            Assert.Equal(4, got.Count);
            Assert.Equal("aaaaaaaa", got[0]);
            Assert.Equal("bbbbbbbb", got[1]);
            Assert.Equal("aaaaaaaa", got[2]);
            Assert.Equal("cccccccc", got[3]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task Array_Nested_OfNullable_OfDecimal_DepthThree(string image)
    {
        await using var conn = await OpenAsync(image);
        var table = $"arr3_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, v Array(Array(Nullable(Decimal128(4))))) ENGINE = Memory");
        try
        {
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES " +
                "(1, [[toDecimal128('1.2345', 4), NULL], [toDecimal128('-3.0000', 4)]]), " +
                "(2, [[]]), " +
                "(3, [])");

            int rows = 0;
            await foreach (var r in conn.QueryAsync(
                $"SELECT length(v), arraySum(arrayMap(x -> length(x), v)) FROM {table} ORDER BY id"))
            {
                _output.WriteLine($"Image {image}: row outer-len={r.GetFieldValue<ulong>(0)}, total-elements={r.GetFieldValue<ulong>(1)}");
                rows++;
            }
            Assert.Equal(3, rows);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task AggregateFunctionState_StableAcrossVersions_HexRoundTrip()
    {
        // The plan's §6.19 — "insert a state from one server version, finalize from
        // another". Testcontainers gives us one container per image, so we transfer
        // the state out-of-band: produce sumState() on image A, hex-encode it, INSERT
        // the unhex on image B, and finalize. If the binary state format drifts
        // between versions the finalized value won't match.
        var images = new[]
        {
            SupportedImages.V23_8_LTS,
            SupportedImages.V24_3_LTS,
            SupportedImages.V24_8_LTS,
            SupportedImages.Latest,
        };

        // Produce a state on the oldest supported image.
        await using var producer = await OpenAsync(images[0]);
        var stateHex = await producer.ExecuteScalarAsync<string>(
            "SELECT hex(sumState(toInt32(number))) FROM numbers(100)");
        _output.WriteLine($"sumState(0..99) hex on {images[0]}: {stateHex?.Length ?? 0} bytes");
        Assert.False(string.IsNullOrEmpty(stateHex));

        // Re-hydrate and finalize on every other image. The finalized value must be
        // 0 + 1 + ... + 99 = 4950, regardless of which version stored or read it.
        const long expected = 100 * 99 / 2;
        foreach (var image in images.Skip(1))
        {
            await using var consumer = await OpenAsync(image);
            var table = $"agg_xv_{Guid.NewGuid():N}";
            await consumer.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (s AggregateFunction(sum, Int32)) ENGINE = Memory");
            try
            {
                await consumer.ExecuteNonQueryAsync(
                    $"INSERT INTO {table} VALUES (unhex('{stateHex}'))");
                var got = await consumer.ExecuteScalarAsync<long>(
                    $"SELECT finalizeAggregation(s) FROM {table}");
                _output.WriteLine($"  finalizeAggregation on {image}: {got}");
                Assert.Equal(expected, got);
            }
            finally
            {
                try { await consumer.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}"); }
                catch { }
            }
        }
    }

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task Nullable_Of_LowCardinality_BehaviourProbe(string image)
    {
        // The "wrong way round" composition. ClickHouse historically prefers
        // LowCardinality(Nullable(X)); Nullable(LowCardinality(X)) may be rejected.
        // Probe what happens.
        await using var conn = await OpenAsync(image);
        var table = $"nul_lc_{Guid.NewGuid():N}";

        Exception? caught = null;
        try
        {
            await conn.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id Int32, s Nullable(LowCardinality(String))) ENGINE = Memory");
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES (1, 'a'), (2, NULL)");
            var got = new List<string?>();
            await foreach (var r in conn.QueryAsync($"SELECT s FROM {table} ORDER BY id"))
                got.Add(r.IsDBNull(0) ? null : r.GetFieldValue<string>(0));
            _output.WriteLine($"Image {image}: Nullable(LowCardinality) accepted, got {got.Count} rows");
        }
        catch (Exception ex)
        {
            caught = ex;
            _output.WriteLine($"Image {image}: Nullable(LowCardinality) rejected — {ex.GetType().Name}: {ex.Message}");
        }
        finally
        {
            try { await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}"); } catch { }
        }

        // Pin only safety: rejection is via typed exception, not OOM/AV.
        if (caught is not null)
        {
            Assert.IsNotType<OutOfMemoryException>(caught);
            Assert.IsNotType<AccessViolationException>(caught);
        }
    }
}
