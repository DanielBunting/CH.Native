using System.Net;
using System.Numerics;
using CH.Native.Connection;
using CH.Native.Data.Dynamic;
using CH.Native.Data.Geo;
using CH.Native.Data.Variant;
using CH.Native.Mapping;
using CH.Native.Numerics;
using CH.Native.SmokeTests.Fixtures;
using CH.Native.SmokeTests.Helpers;
using Xunit;

namespace CH.Native.SmokeTests.Query;

// Cross-client roundtrips with clickhouse-client (exec'd inside the server container) as the
// second client, mirroring the CrossClient*IT pattern from jvm-clickhouse-native. Each [Fact]
// runs both directions against ANCHORED expected values, so a failure identifies which client
// is wrong rather than just that the two disagree:
//
//   (a) clickhouse-client INSERT -> CH.Native reads: canonical (server-rendered toString())
//       asserted vs anchored strings, plus raw typed read asserted vs anchored CLR values.
//   (b) CH.Native insert (bulk inserter, or SQL over the native protocol for writer-gap
//       types) -> clickhouse-client reads canonical, asserted vs the same anchored strings.
//
// Canonical comparisons use the server's own toString() rendering on both legs, so float,
// DateTime, and NULL formatting are deterministic by construction and no client-side TSV
// type parsing is needed.
[Collection("SmokeTest")]
public class CliTypeRoundTripSmokeTests
{
    private readonly SmokeTestFixture _fixture;

    public CliTypeRoundTripSmokeTests(SmokeTestFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task HarnessSelfTest()
    {
        var rows = await CliQueryHelper.QueryCanonicalAsync(
            _fixture, "SELECT 1, 'a\\tb', NULL");

        var row = Assert.Single(rows);
        Assert.Equal(new string?[] { "1", "a\tb", null }, row);
    }

    private async Task RunCliCrossTest<T>(
        string columnType,
        string insertValues,
        string?[] expectedCanonical,
        Row<T>[]? bulkRows = null,
        object?[]? expectedClr = null,
        string canonicalExpr = "toString(val)",
        string[]? cliArgs = null,
        string[]? nativeSessionSettings = null)
    {
        var table = $"smoke_cli_{Guid.NewGuid():N}";
        try
        {
            await ExecuteNativeAsync(nativeSessionSettings,
                $"CREATE TABLE {table} (id Int32, val {columnType}) ENGINE = Memory");

            // --- (a) CLI insert -> CH.Native read ---
            await CliQueryHelper.ExecuteNonQueryAsync(_fixture,
                $"INSERT INTO {table} VALUES {insertValues}", cliArgs);

            var nativeCanonical = await QueryNativeAsync(nativeSessionSettings,
                $"SELECT {canonicalExpr} FROM {table} ORDER BY id");
            Assert.Equal(expectedCanonical,
                nativeCanonical.Select(r => (string?)r[0]).ToArray());

            if (expectedClr is not null)
            {
                var nativeRaw = await QueryNativeAsync(nativeSessionSettings,
                    $"SELECT val FROM {table} ORDER BY id");
                ResultComparer.AssertResultsEqual(
                    nativeRaw,
                    expectedClr.Select(v => new[] { v }).ToList(),
                    $"CLI insert -> native raw read: {columnType}");
            }

            // --- (b) CH.Native insert -> CLI read ---
            await ExecuteNativeAsync(nativeSessionSettings, $"TRUNCATE TABLE {table}");

            if (bulkRows is not null)
            {
                await using var conn = new ClickHouseConnection(_fixture.NativeConnectionString);
                await conn.OpenAsync();
                if (nativeSessionSettings is not null)
                {
                    foreach (var setting in nativeSessionSettings)
                    {
                        await conn.ExecuteNonQueryAsync(setting);
                    }
                }

                await using var inserter = conn.CreateBulkInserter<Row<T>>(table);
                await inserter.InitAsync();
                foreach (var r in bulkRows)
                {
                    await inserter.AddAsync(r);
                }
                await inserter.CompleteAsync();
            }
            else
            {
                // Writer-gap types: a SQL INSERT still travels the native protocol.
                await ExecuteNativeAsync(nativeSessionSettings,
                    $"INSERT INTO {table} VALUES {insertValues}");
            }

            var cliRead = await CliQueryHelper.QueryCanonicalAsync(_fixture,
                $"SELECT {canonicalExpr} FROM {table} ORDER BY id", cliArgs);
            Assert.Equal(expectedCanonical, cliRead.Select(r => r[0]).ToArray());
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    private async Task ExecuteNativeAsync(string[]? sessionSettings, string sql)
    {
        await using var conn = new ClickHouseConnection(_fixture.NativeConnectionString);
        await conn.OpenAsync();
        foreach (var setting in sessionSettings ?? Array.Empty<string>())
        {
            await conn.ExecuteNonQueryAsync(setting);
        }
        await conn.ExecuteNonQueryAsync(sql);
    }

    private async Task<List<object?[]>> QueryNativeAsync(string[]? sessionSettings, string sql)
    {
        await using var conn = new ClickHouseConnection(_fixture.NativeConnectionString);
        await conn.OpenAsync();
        foreach (var setting in sessionSettings ?? Array.Empty<string>())
        {
            await conn.ExecuteNonQueryAsync(setting);
        }

        var rows = new List<object?[]>();
        await foreach (var row in conn.QueryStreamAsync(sql))
        {
            var values = new object?[row.FieldCount];
            for (int i = 0; i < row.FieldCount; i++)
            {
                values[i] = row[i];
            }
            rows.Add(values);
        }
        return rows;
    }

    [Fact]
    public Task Int64() => RunCliCrossTest(
        "Int64",
        "(0, -9223372036854775808),(1, 0),(2, 9223372036854775807)",
        new string?[] { "-9223372036854775808", "0", "9223372036854775807" },
        bulkRows: new[]
        {
            new Row<long> { Id = 0, Val = long.MinValue },
            new Row<long> { Id = 1, Val = 0 },
            new Row<long> { Id = 2, Val = long.MaxValue },
        },
        expectedClr: new object?[] { long.MinValue, 0L, long.MaxValue });

    [Fact]
    public Task NullableString() => RunCliCrossTest(
        "Nullable(String)",
        "(0, 'hello'),(1, NULL),(2, '')",
        new string?[] { "hello", null, "" },
        bulkRows: new[]
        {
            new Row<string?> { Id = 0, Val = "hello" },
            new Row<string?> { Id = 1, Val = null },
            new Row<string?> { Id = 2, Val = "" },
        },
        expectedClr: new object?[] { "hello", null, "" });

    [Fact]
    public Task ArrayInt32() => RunCliCrossTest(
        "Array(Int32)",
        "(0, [1,2,3]),(1, []),(2, [-2147483648,2147483647])",
        new string?[] { "[1,2,3]", "[]", "[-2147483648,2147483647]" },
        bulkRows: new[]
        {
            new Row<int[]> { Id = 0, Val = new[] { 1, 2, 3 } },
            new Row<int[]> { Id = 1, Val = Array.Empty<int>() },
            new Row<int[]> { Id = 2, Val = new[] { int.MinValue, int.MaxValue } },
        },
        expectedClr: new object?[]
        {
            new[] { 1, 2, 3 },
            Array.Empty<int>(),
            new[] { int.MinValue, int.MaxValue },
        });

    #region Scalars

    [Fact]
    public Task Bool() => RunCliCrossTest(
        "Bool",
        "(0, false),(1, true)",
        new string?[] { "false", "true" },
        bulkRows: new[]
        {
            new Row<bool> { Id = 0, Val = false },
            new Row<bool> { Id = 1, Val = true },
        },
        expectedClr: new object?[] { false, true });

    [Fact]
    public Task Int8() => RunCliCrossTest(
        "Int8",
        "(0, -128),(1, 0),(2, 127)",
        new string?[] { "-128", "0", "127" },
        bulkRows: new[]
        {
            new Row<sbyte> { Id = 0, Val = sbyte.MinValue },
            new Row<sbyte> { Id = 1, Val = 0 },
            new Row<sbyte> { Id = 2, Val = sbyte.MaxValue },
        },
        expectedClr: new object?[] { sbyte.MinValue, (sbyte)0, sbyte.MaxValue });

    [Fact]
    public Task Int16() => RunCliCrossTest(
        "Int16",
        "(0, -32768),(1, 0),(2, 32767)",
        new string?[] { "-32768", "0", "32767" },
        bulkRows: new[]
        {
            new Row<short> { Id = 0, Val = short.MinValue },
            new Row<short> { Id = 1, Val = 0 },
            new Row<short> { Id = 2, Val = short.MaxValue },
        },
        expectedClr: new object?[] { short.MinValue, (short)0, short.MaxValue });

    [Fact]
    public Task Int32() => RunCliCrossTest(
        "Int32",
        "(0, -2147483648),(1, 0),(2, 2147483647)",
        new string?[] { "-2147483648", "0", "2147483647" },
        bulkRows: new[]
        {
            new Row<int> { Id = 0, Val = int.MinValue },
            new Row<int> { Id = 1, Val = 0 },
            new Row<int> { Id = 2, Val = int.MaxValue },
        },
        expectedClr: new object?[] { int.MinValue, 0, int.MaxValue });

    [Fact]
    public Task Int128_() => RunCliCrossTest(
        "Int128",
        "(0, -170141183460469231731687303715884105728),(1, 0),(2, 170141183460469231731687303715884105727)",
        new string?[]
        {
            "-170141183460469231731687303715884105728",
            "0",
            "170141183460469231731687303715884105727",
        },
        bulkRows: new[]
        {
            new Row<Int128> { Id = 0, Val = Int128.MinValue },
            new Row<Int128> { Id = 1, Val = Int128.Zero },
            new Row<Int128> { Id = 2, Val = Int128.MaxValue },
        },
        expectedClr: new object?[] { Int128.MinValue, Int128.Zero, Int128.MaxValue });

    [Fact]
    public Task Int256() => RunCliCrossTest(
        "Int256",
        "(0, -57896044618658097711785492504343953926634992332820282019728792003956564819968),(1, 0),(2, 57896044618658097711785492504343953926634992332820282019728792003956564819967)",
        new string?[]
        {
            "-57896044618658097711785492504343953926634992332820282019728792003956564819968",
            "0",
            "57896044618658097711785492504343953926634992332820282019728792003956564819967",
        },
        bulkRows: new[]
        {
            new Row<BigInteger> { Id = 0, Val = BigInteger.Parse("-57896044618658097711785492504343953926634992332820282019728792003956564819968") },
            new Row<BigInteger> { Id = 1, Val = BigInteger.Zero },
            new Row<BigInteger> { Id = 2, Val = BigInteger.Parse("57896044618658097711785492504343953926634992332820282019728792003956564819967") },
        },
        expectedClr: new object?[]
        {
            BigInteger.Parse("-57896044618658097711785492504343953926634992332820282019728792003956564819968"),
            BigInteger.Zero,
            BigInteger.Parse("57896044618658097711785492504343953926634992332820282019728792003956564819967"),
        });

    [Fact]
    public Task UInt8() => RunCliCrossTest(
        "UInt8",
        "(0, 0),(1, 128),(2, 255)",
        new string?[] { "0", "128", "255" },
        bulkRows: new[]
        {
            new Row<byte> { Id = 0, Val = 0 },
            new Row<byte> { Id = 1, Val = 128 },
            new Row<byte> { Id = 2, Val = byte.MaxValue },
        },
        expectedClr: new object?[] { (byte)0, (byte)128, byte.MaxValue });

    [Fact]
    public Task UInt16() => RunCliCrossTest(
        "UInt16",
        "(0, 0),(1, 65535)",
        new string?[] { "0", "65535" },
        bulkRows: new[]
        {
            new Row<ushort> { Id = 0, Val = 0 },
            new Row<ushort> { Id = 1, Val = ushort.MaxValue },
        },
        expectedClr: new object?[] { (ushort)0, ushort.MaxValue });

    [Fact]
    public Task UInt32() => RunCliCrossTest(
        "UInt32",
        "(0, 0),(1, 4294967295)",
        new string?[] { "0", "4294967295" },
        bulkRows: new[]
        {
            new Row<uint> { Id = 0, Val = 0 },
            new Row<uint> { Id = 1, Val = uint.MaxValue },
        },
        expectedClr: new object?[] { 0u, uint.MaxValue });

    [Fact]
    public Task UInt64() => RunCliCrossTest(
        "UInt64",
        "(0, 0),(1, 9223372036854775808),(2, 18446744073709551615)",
        new string?[] { "0", "9223372036854775808", "18446744073709551615" },
        bulkRows: new[]
        {
            new Row<ulong> { Id = 0, Val = 0 },
            new Row<ulong> { Id = 1, Val = (ulong)long.MaxValue + 1 },
            new Row<ulong> { Id = 2, Val = ulong.MaxValue },
        },
        expectedClr: new object?[] { 0ul, (ulong)long.MaxValue + 1, ulong.MaxValue });

    [Fact]
    public Task UInt128_() => RunCliCrossTest(
        "UInt128",
        "(0, 0),(1, 340282366920938463463374607431768211455)",
        new string?[] { "0", "340282366920938463463374607431768211455" },
        bulkRows: new[]
        {
            new Row<UInt128> { Id = 0, Val = UInt128.MinValue },
            new Row<UInt128> { Id = 1, Val = UInt128.MaxValue },
        },
        expectedClr: new object?[] { UInt128.MinValue, UInt128.MaxValue });

    [Fact]
    public Task UInt256() => RunCliCrossTest(
        "UInt256",
        "(0, 0),(1, 115792089237316195423570985008687907853269984665640564039457584007913129639935)",
        new string?[]
        {
            "0",
            "115792089237316195423570985008687907853269984665640564039457584007913129639935",
        },
        bulkRows: new[]
        {
            new Row<BigInteger> { Id = 0, Val = BigInteger.Zero },
            new Row<BigInteger> { Id = 1, Val = BigInteger.Parse("115792089237316195423570985008687907853269984665640564039457584007913129639935") },
        },
        expectedClr: new object?[]
        {
            BigInteger.Zero,
            BigInteger.Parse("115792089237316195423570985008687907853269984665640564039457584007913129639935"),
        });

    // Values chosen to be exactly representable in bfloat16 (8-bit mantissa).
    [Fact]
    public Task BFloat16() => RunCliCrossTest(
        "BFloat16",
        "(0, -0.5),(1, 1.5),(2, 2.5)",
        new string?[] { "-0.5", "1.5", "2.5" },
        bulkRows: new[]
        {
            new Row<float> { Id = 0, Val = -0.5f },
            new Row<float> { Id = 1, Val = 1.5f },
            new Row<float> { Id = 2, Val = 2.5f },
        },
        expectedClr: new object?[] { -0.5f, 1.5f, 2.5f });

    [Fact]
    public Task Float32() => RunCliCrossTest(
        "Float32",
        "(0, -1.5),(1, 0),(2, 1.5)",
        new string?[] { "-1.5", "0", "1.5" },
        bulkRows: new[]
        {
            new Row<float> { Id = 0, Val = -1.5f },
            new Row<float> { Id = 1, Val = 0f },
            new Row<float> { Id = 2, Val = 1.5f },
        },
        expectedClr: new object?[] { -1.5f, 0f, 1.5f });

    [Fact]
    public Task Float64() => RunCliCrossTest(
        "Float64",
        "(0, -2.71828),(1, 0),(2, 3.14159265358979)",
        new string?[] { "-2.71828", "0", "3.14159265358979" },
        bulkRows: new[]
        {
            new Row<double> { Id = 0, Val = -2.71828 },
            new Row<double> { Id = 1, Val = 0.0 },
            new Row<double> { Id = 2, Val = 3.14159265358979 },
        },
        expectedClr: new object?[] { -2.71828, 0.0, 3.14159265358979 });

    [Fact]
    public Task Float64_Specials() => RunCliCrossTest(
        "Float64",
        "(0, nan),(1, inf),(2, -inf)",
        new string?[] { "nan", "inf", "-inf" },
        bulkRows: new[]
        {
            new Row<double> { Id = 0, Val = double.NaN },
            new Row<double> { Id = 1, Val = double.PositiveInfinity },
            new Row<double> { Id = 2, Val = double.NegativeInfinity },
        },
        expectedClr: new object?[] { double.NaN, double.PositiveInfinity, double.NegativeInfinity });

    [Fact]
    public Task Decimal32() => RunCliCrossTest(
        "Decimal32(4)",
        "(0, -99.9999),(1, 0),(2, 99.9999)",
        new string?[] { "-99.9999", "0", "99.9999" },
        bulkRows: new[]
        {
            new Row<decimal> { Id = 0, Val = -99.9999m },
            new Row<decimal> { Id = 1, Val = 0m },
            new Row<decimal> { Id = 2, Val = 99.9999m },
        },
        expectedClr: new object?[] { -99.9999m, 0m, 99.9999m });

    [Fact]
    public Task Decimal64() => RunCliCrossTest(
        "Decimal64(8)",
        "(0, -999999.99999999),(1, 0),(2, 999999.99999999)",
        new string?[] { "-999999.99999999", "0", "999999.99999999" },
        bulkRows: new[]
        {
            new Row<decimal> { Id = 0, Val = -999999.99999999m },
            new Row<decimal> { Id = 1, Val = 0m },
            new Row<decimal> { Id = 2, Val = 999999.99999999m },
        },
        expectedClr: new object?[] { -999999.99999999m, 0m, 999999.99999999m });

    [Fact]
    public Task Decimal128() => RunCliCrossTest(
        "Decimal128(18)",
        "(0, -99999999.999999999999999999),(1, 0),(2, 99999999.999999999999999999)",
        new string?[] { "-99999999.999999999999999999", "0", "99999999.999999999999999999" },
        bulkRows: new[]
        {
            new Row<decimal> { Id = 0, Val = -99999999.999999999999999999m },
            new Row<decimal> { Id = 1, Val = 0m },
            new Row<decimal> { Id = 2, Val = 99999999.999999999999999999m },
        },
        expectedClr: new object?[] { -99999999.999999999999999999m, 0m, 99999999.999999999999999999m });

    [Fact]
    public Task Decimal256() => RunCliCrossTest(
        "Decimal256(30)",
        "(0, -123456789.012345678901234567890123456789),(1, 0),(2, 123456789.012345678901234567890123456789)",
        new string?[]
        {
            "-123456789.012345678901234567890123456789",
            "0",
            "123456789.012345678901234567890123456789",
        },
        bulkRows: new[]
        {
            new Row<ClickHouseDecimal> { Id = 0, Val = ClickHouseDecimal.Parse("-123456789.012345678901234567890123456789") },
            new Row<ClickHouseDecimal> { Id = 1, Val = ClickHouseDecimal.Parse("0") },
            new Row<ClickHouseDecimal> { Id = 2, Val = ClickHouseDecimal.Parse("123456789.012345678901234567890123456789") },
        });

    [Fact]
    public Task String_() => RunCliCrossTest(
        "String",
        "(0, ''),(1, 'hello world'),(2, 'тест 🦀 混合'),(3, 'tab\\there\\nnewline')",
        new string?[] { "", "hello world", "тест 🦀 混合", "tab\there\nnewline" },
        bulkRows: new[]
        {
            new Row<string> { Id = 0, Val = "" },
            new Row<string> { Id = 1, Val = "hello world" },
            new Row<string> { Id = 2, Val = "тест 🦀 混合" },
            new Row<string> { Id = 3, Val = "tab\there\nnewline" },
        },
        expectedClr: new object?[] { "", "hello world", "тест 🦀 混合", "tab\there\nnewline" });

    // Canonical leg only for the FixedString CLR shape: toString() trims trailing NUL
    // padding on the server side; the raw CLR representation is pinned in
    // NativeLimitationProbeTests.
    [Fact]
    public Task FixedString() => RunCliCrossTest(
        "FixedString(8)",
        "(0, ''),(1, 'abcd'),(2, 'test1234')",
        new string?[] { "", "abcd", "test1234" },
        bulkRows: new[]
        {
            new Row<string> { Id = 0, Val = "" },
            new Row<string> { Id = 1, Val = "abcd" },
            new Row<string> { Id = 2, Val = "test1234" },
        });

    [Fact]
    public Task Enum8() => RunCliCrossTest(
        "Enum8('a' = 1, 'b' = 2, 'c' = 3)",
        "(0, 'a'),(1, 'b'),(2, 'c')",
        new string?[] { "a", "b", "c" },
        bulkRows: new[]
        {
            new Row<sbyte> { Id = 0, Val = 1 },
            new Row<sbyte> { Id = 1, Val = 2 },
            new Row<sbyte> { Id = 2, Val = 3 },
        });

    [Fact]
    public Task Enum16() => RunCliCrossTest(
        "Enum16('alpha' = 1000, 'beta' = 2000, 'gamma' = 3000)",
        "(0, 'alpha'),(1, 'beta'),(2, 'gamma')",
        new string?[] { "alpha", "beta", "gamma" },
        bulkRows: new[]
        {
            new Row<short> { Id = 0, Val = 1000 },
            new Row<short> { Id = 1, Val = 2000 },
            new Row<short> { Id = 2, Val = 3000 },
        });

    [Fact]
    public Task Uuid() => RunCliCrossTest(
        "UUID",
        "(0, '00000000-0000-0000-0000-000000000000'),(1, '550e8400-e29b-41d4-a716-446655440000'),(2, 'ffffffff-ffff-ffff-ffff-ffffffffffff')",
        new string?[]
        {
            "00000000-0000-0000-0000-000000000000",
            "550e8400-e29b-41d4-a716-446655440000",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
        },
        bulkRows: new[]
        {
            new Row<Guid> { Id = 0, Val = Guid.Empty },
            new Row<Guid> { Id = 1, Val = Guid.Parse("550e8400-e29b-41d4-a716-446655440000") },
            new Row<Guid> { Id = 2, Val = Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff") },
        },
        expectedClr: new object?[]
        {
            Guid.Empty,
            Guid.Parse("550e8400-e29b-41d4-a716-446655440000"),
            Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff"),
        });

    [Fact]
    public Task IPv4() => RunCliCrossTest(
        "IPv4",
        "(0, '0.0.0.0'),(1, '192.168.1.1'),(2, '255.255.255.255')",
        new string?[] { "0.0.0.0", "192.168.1.1", "255.255.255.255" },
        bulkRows: new[]
        {
            new Row<IPAddress> { Id = 0, Val = IPAddress.Parse("0.0.0.0") },
            new Row<IPAddress> { Id = 1, Val = IPAddress.Parse("192.168.1.1") },
            new Row<IPAddress> { Id = 2, Val = IPAddress.Parse("255.255.255.255") },
        },
        expectedClr: new object?[]
        {
            IPAddress.Parse("0.0.0.0"),
            IPAddress.Parse("192.168.1.1"),
            IPAddress.Parse("255.255.255.255"),
        });

    [Fact]
    public Task IPv6() => RunCliCrossTest(
        "IPv6",
        "(0, '::'),(1, '2001:db8::1'),(2, 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff')",
        new string?[] { "::", "2001:db8::1", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff" },
        bulkRows: new[]
        {
            new Row<IPAddress> { Id = 0, Val = IPAddress.IPv6Any },
            new Row<IPAddress> { Id = 1, Val = IPAddress.Parse("2001:db8::1") },
            new Row<IPAddress> { Id = 2, Val = IPAddress.Parse("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff") },
        },
        expectedClr: new object?[]
        {
            IPAddress.IPv6Any,
            IPAddress.Parse("2001:db8::1"),
            IPAddress.Parse("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"),
        });

    #endregion

    #region Temporal

    [Fact]
    public Task Date() => RunCliCrossTest(
        "Date",
        "(0, '1970-01-01'),(1, '2000-06-15'),(2, '2149-06-06')",
        new string?[] { "1970-01-01", "2000-06-15", "2149-06-06" },
        bulkRows: new[]
        {
            new Row<DateOnly> { Id = 0, Val = new DateOnly(1970, 1, 1) },
            new Row<DateOnly> { Id = 1, Val = new DateOnly(2000, 6, 15) },
            new Row<DateOnly> { Id = 2, Val = new DateOnly(2149, 6, 6) },
        },
        expectedClr: new object?[]
        {
            new DateOnly(1970, 1, 1),
            new DateOnly(2000, 6, 15),
            new DateOnly(2149, 6, 6),
        });

    // Date32 extremes (1900-01-01 / 2299-12-31) are probed separately; the existing bulk
    // suite anchors 2283-11-11 as its upper value, so the matrix stays within that.
    [Fact]
    public Task Date32() => RunCliCrossTest(
        "Date32",
        "(0, '1900-01-01'),(1, '2000-06-15'),(2, '2283-11-11')",
        new string?[] { "1900-01-01", "2000-06-15", "2283-11-11" },
        bulkRows: new[]
        {
            new Row<DateOnly> { Id = 0, Val = new DateOnly(1900, 1, 1) },
            new Row<DateOnly> { Id = 1, Val = new DateOnly(2000, 6, 15) },
            new Row<DateOnly> { Id = 2, Val = new DateOnly(2283, 11, 11) },
        },
        expectedClr: new object?[]
        {
            new DateOnly(1900, 1, 1),
            new DateOnly(2000, 6, 15),
            new DateOnly(2283, 11, 11),
        });

    [Fact]
    public Task DateTime_() => RunCliCrossTest(
        "DateTime('UTC')",
        "(0, '1970-01-01 00:00:00'),(1, '2024-06-15 12:00:00'),(2, '2106-02-07 06:28:15')",
        new string?[] { "1970-01-01 00:00:00", "2024-06-15 12:00:00", "2106-02-07 06:28:15" },
        bulkRows: new[]
        {
            new Row<DateTime> { Id = 0, Val = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc) },
            new Row<DateTime> { Id = 1, Val = new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Utc) },
            new Row<DateTime> { Id = 2, Val = new DateTime(2106, 2, 7, 6, 28, 15, DateTimeKind.Utc) },
        },
        expectedClr: new object?[]
        {
            new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Utc),
            new DateTime(2106, 2, 7, 6, 28, 15, DateTimeKind.Utc),
        });

    [Fact]
    public Task DateTime64_3() => RunCliCrossTest(
        "DateTime64(3, 'UTC')",
        "(0, '1970-01-01 00:00:00.001'),(1, '2024-01-01 12:30:45.123'),(2, '2100-12-31 23:59:59.999')",
        new string?[]
        {
            "1970-01-01 00:00:00.001",
            "2024-01-01 12:30:45.123",
            "2100-12-31 23:59:59.999",
        },
        bulkRows: new[]
        {
            new Row<DateTime> { Id = 0, Val = new DateTime(1970, 1, 1, 0, 0, 0, 1, DateTimeKind.Utc) },
            new Row<DateTime> { Id = 1, Val = new DateTime(2024, 1, 1, 12, 30, 45, 123, DateTimeKind.Utc) },
            new Row<DateTime> { Id = 2, Val = new DateTime(2100, 12, 31, 23, 59, 59, 999, DateTimeKind.Utc) },
        },
        expectedClr: new object?[]
        {
            new DateTime(1970, 1, 1, 0, 0, 0, 1, DateTimeKind.Utc),
            new DateTime(2024, 1, 1, 12, 30, 45, 123, DateTimeKind.Utc),
            new DateTime(2100, 12, 31, 23, 59, 59, 999, DateTimeKind.Utc),
        });

    [Fact]
    public Task DateTime64_6() => RunCliCrossTest(
        "DateTime64(6, 'UTC')",
        "(0, '1970-01-01 00:00:00.001000'),(1, '2024-01-01 12:30:45.456000')",
        new string?[] { "1970-01-01 00:00:00.001000", "2024-01-01 12:30:45.456000" },
        bulkRows: new[]
        {
            new Row<DateTime> { Id = 0, Val = new DateTime(1970, 1, 1, 0, 0, 0, 1, DateTimeKind.Utc) },
            new Row<DateTime> { Id = 1, Val = new DateTime(2024, 1, 1, 12, 30, 45, 456, DateTimeKind.Utc) },
        },
        expectedClr: new object?[]
        {
            new DateTime(1970, 1, 1, 0, 0, 0, 1, DateTimeKind.Utc),
            new DateTime(2024, 1, 1, 12, 30, 45, 456, DateTimeKind.Utc),
        });

    private static readonly string[] TimeCliArgs = { "--enable_time_time64_type=1" };
    private static readonly string[] TimeNativeSettings = { "SET enable_time_time64_type=1" };

    [Fact]
    public Task Time_() => RunCliCrossTest(
        "Time",
        "(0, '00:00:00'),(1, '12:30:45'),(2, '23:59:59')",
        new string?[] { "00:00:00", "12:30:45", "23:59:59" },
        bulkRows: new[]
        {
            new Row<TimeOnly> { Id = 0, Val = TimeOnly.MinValue },
            new Row<TimeOnly> { Id = 1, Val = new TimeOnly(12, 30, 45) },
            new Row<TimeOnly> { Id = 2, Val = new TimeOnly(23, 59, 59) },
        },
        cliArgs: TimeCliArgs,
        nativeSessionSettings: TimeNativeSettings);

    [Fact]
    public Task Time64_3() => RunCliCrossTest(
        "Time64(3)",
        "(0, '00:00:00.000'),(1, '12:30:45.123'),(2, '23:59:59.999')",
        new string?[] { "00:00:00.000", "12:30:45.123", "23:59:59.999" },
        bulkRows: new[]
        {
            new Row<TimeOnly> { Id = 0, Val = TimeOnly.MinValue },
            new Row<TimeOnly> { Id = 1, Val = new TimeOnly(12, 30, 45, 123) },
            new Row<TimeOnly> { Id = 2, Val = new TimeOnly(23, 59, 59, 999) },
        },
        cliArgs: TimeCliArgs,
        nativeSessionSettings: TimeNativeSettings);

    #endregion

    #region Nullable

    [Fact]
    public Task NullableInt32() => RunCliCrossTest(
        "Nullable(Int32)",
        "(0, -2147483648),(1, NULL),(2, 2147483647)",
        new string?[] { "-2147483648", null, "2147483647" },
        bulkRows: new[]
        {
            new Row<int?> { Id = 0, Val = int.MinValue },
            new Row<int?> { Id = 1, Val = null },
            new Row<int?> { Id = 2, Val = int.MaxValue },
        },
        expectedClr: new object?[] { int.MinValue, null, int.MaxValue });

    [Fact]
    public Task NullableDecimal64() => RunCliCrossTest(
        "Nullable(Decimal64(4))",
        "(0, -12.3456),(1, NULL),(2, 12.3456)",
        new string?[] { "-12.3456", null, "12.3456" },
        bulkRows: new[]
        {
            new Row<decimal?> { Id = 0, Val = -12.3456m },
            new Row<decimal?> { Id = 1, Val = null },
            new Row<decimal?> { Id = 2, Val = 12.3456m },
        },
        expectedClr: new object?[] { -12.3456m, null, 12.3456m });

    [Fact]
    public Task NullableDateTime64() => RunCliCrossTest(
        "Nullable(DateTime64(3, 'UTC'))",
        "(0, '2024-01-01 12:30:45.123'),(1, NULL)",
        new string?[] { "2024-01-01 12:30:45.123", null },
        bulkRows: new[]
        {
            new Row<DateTime?> { Id = 0, Val = new DateTime(2024, 1, 1, 12, 30, 45, 123, DateTimeKind.Utc) },
            new Row<DateTime?> { Id = 1, Val = null },
        },
        expectedClr: new object?[]
        {
            new DateTime(2024, 1, 1, 12, 30, 45, 123, DateTimeKind.Utc),
            null,
        });

    [Fact]
    public Task NullableUuid() => RunCliCrossTest(
        "Nullable(UUID)",
        "(0, '550e8400-e29b-41d4-a716-446655440000'),(1, NULL)",
        new string?[] { "550e8400-e29b-41d4-a716-446655440000", null },
        bulkRows: new[]
        {
            new Row<Guid?> { Id = 0, Val = Guid.Parse("550e8400-e29b-41d4-a716-446655440000") },
            new Row<Guid?> { Id = 1, Val = null },
        },
        expectedClr: new object?[]
        {
            Guid.Parse("550e8400-e29b-41d4-a716-446655440000"),
            null,
        });

    #endregion

    #region Composites

    [Fact]
    public Task ArrayNullableString() => RunCliCrossTest(
        "Array(Nullable(String))",
        "(0, ['a', NULL, 'c']),(1, [])",
        new string?[] { "['a',NULL,'c']", "[]" },
        bulkRows: new[]
        {
            new Row<string?[]> { Id = 0, Val = new[] { "a", null, "c" } },
            new Row<string?[]> { Id = 1, Val = Array.Empty<string?>() },
        },
        expectedClr: new object?[]
        {
            new[] { "a", null, "c" },
            Array.Empty<string?>(),
        });

    [Fact]
    public Task ArrayArrayInt32() => RunCliCrossTest(
        "Array(Array(Int32))",
        "(0, [[1,2],[3]]),(1, [[]]),(2, [])",
        new string?[] { "[[1,2],[3]]", "[[]]", "[]" },
        bulkRows: new[]
        {
            new Row<int[][]> { Id = 0, Val = new[] { new[] { 1, 2 }, new[] { 3 } } },
            new Row<int[][]> { Id = 1, Val = new[] { Array.Empty<int>() } },
            new Row<int[][]> { Id = 2, Val = Array.Empty<int[]>() },
        },
        expectedClr: new object?[]
        {
            new[] { new[] { 1, 2 }, new[] { 3 } },
            new[] { Array.Empty<int>() },
            Array.Empty<int[]>(),
        });

    [Fact]
    public Task MapStringInt32() => RunCliCrossTest(
        "Map(String, Int32)",
        "(0, {}),(1, {'a': 1}),(2, {'x': 10, 'y': 20})",
        new string?[] { "{}", "{'a':1}", "{'x':10,'y':20}" },
        bulkRows: new[]
        {
            new Row<Dictionary<string, int>> { Id = 0, Val = new() },
            new Row<Dictionary<string, int>> { Id = 1, Val = new() { ["a"] = 1 } },
            new Row<Dictionary<string, int>> { Id = 2, Val = new() { ["x"] = 10, ["y"] = 20 } },
        },
        expectedClr: new object?[]
        {
            new Dictionary<string, int>(),
            new Dictionary<string, int> { ["a"] = 1 },
            new Dictionary<string, int> { ["x"] = 10, ["y"] = 20 },
        });

    [Fact]
    public Task MapStringNullableInt32() => RunCliCrossTest(
        "Map(String, Nullable(Int32))",
        "(0, {'a': 1, 'b': NULL})",
        new string?[] { "{'a':1,'b':NULL}" },
        bulkRows: new[]
        {
            new Row<Dictionary<string, int?>> { Id = 0, Val = new() { ["a"] = 1, ["b"] = null } },
        },
        expectedClr: new object?[]
        {
            new Dictionary<string, int?> { ["a"] = 1, ["b"] = null },
        });

    [Fact]
    public Task TupleIntString() => RunCliCrossTest(
        "Tuple(Int32, String)",
        "(0, (1, 'a')),(1, (42, 'hello'))",
        new string?[] { "(1,'a')", "(42,'hello')" },
        bulkRows: new[]
        {
            new Row<object[]> { Id = 0, Val = new object[] { 1, "a" } },
            new Row<object[]> { Id = 1, Val = new object[] { 42, "hello" } },
        },
        expectedClr: new object?[]
        {
            Tuple.Create(1, "a"),
            Tuple.Create(42, "hello"),
        });

    [Fact]
    public Task TupleNullableStringInt32() => RunCliCrossTest(
        "Tuple(Nullable(String), Int32)",
        "(0, ('hello', 1)),(1, (NULL, 42))",
        new string?[] { "('hello',1)", "(NULL,42)" },
        bulkRows: new[]
        {
            new Row<object?[]> { Id = 0, Val = new object?[] { "hello", 1 } },
            new Row<object?[]> { Id = 1, Val = new object?[] { null, 42 } },
        },
        expectedClr: new object?[]
        {
            Tuple.Create((string?)"hello", 1),
            Tuple.Create((string?)null, 42),
        });

    // No bulk extractor coverage for Array(Tuple(...)) in the existing suite; the
    // native-insert leg goes via SQL.
    [Fact]
    public Task ArrayTupleIntString() => RunCliCrossTest<object>(
        "Array(Tuple(Int32, String))",
        "(0, [(1, 'x'), (2, 'y')]),(1, [])",
        new string?[] { "[(1,'x'),(2,'y')]", "[]" },
        expectedClr: new object?[]
        {
            new[] { Tuple.Create(1, "x"), Tuple.Create(2, "y") },
            Array.Empty<Tuple<int, string>>(),
        });

    [Fact]
    public Task LowCardinalityString() => RunCliCrossTest(
        "LowCardinality(String)",
        "(0, 'red'),(1, 'green'),(2, 'red')",
        new string?[] { "red", "green", "red" },
        bulkRows: new[]
        {
            new Row<string> { Id = 0, Val = "red" },
            new Row<string> { Id = 1, Val = "green" },
            new Row<string> { Id = 2, Val = "red" },
        },
        expectedClr: new object?[] { "red", "green", "red" });

    [Fact]
    public Task LowCardinalityNullableString() => RunCliCrossTest(
        "LowCardinality(Nullable(String))",
        "(0, 'red'),(1, NULL),(2, 'red')",
        new string?[] { "red", null, "red" },
        bulkRows: new[]
        {
            new Row<string?> { Id = 0, Val = "red" },
            new Row<string?> { Id = 1, Val = null },
            new Row<string?> { Id = 2, Val = "red" },
        },
        expectedClr: new object?[] { "red", null, "red" });

    #endregion

    #region Geo

    [Fact]
    public Task Point_() => RunCliCrossTest(
        "Point",
        "(0, (0, 0)),(1, (1.5, -2.5))",
        new string?[] { "(0,0)", "(1.5,-2.5)" },
        bulkRows: new[]
        {
            new Row<Point> { Id = 0, Val = Point.Zero },
            new Row<Point> { Id = 1, Val = new Point(1.5, -2.5) },
        },
        expectedClr: new object?[] { Point.Zero, new Point(1.5, -2.5) });

    [Fact]
    public Task Ring_() => RunCliCrossTest(
        "Ring",
        "(0, [(0, 0), (1, 0), (1, 1), (0, 1)]),(1, [])",
        new string?[] { "[(0,0),(1,0),(1,1),(0,1)]", "[]" },
        bulkRows: new[]
        {
            new Row<Point[]> { Id = 0, Val = new[] { new Point(0, 0), new Point(1, 0), new Point(1, 1), new Point(0, 1) } },
            new Row<Point[]> { Id = 1, Val = Array.Empty<Point>() },
        });

    [Fact]
    public Task LineString_() => RunCliCrossTest(
        "LineString",
        "(0, [(0, 0), (1, 1), (2, 0)]),(1, [])",
        new string?[] { "[(0,0),(1,1),(2,0)]", "[]" },
        bulkRows: new[]
        {
            new Row<Point[]> { Id = 0, Val = new[] { new Point(0, 0), new Point(1, 1), new Point(2, 0) } },
            new Row<Point[]> { Id = 1, Val = Array.Empty<Point>() },
        });

    [Fact]
    public Task Polygon_() => RunCliCrossTest(
        "Polygon",
        "(0, [[(0, 0), (5, 0), (5, 5), (0, 5)], [(1, 1), (2, 1), (2, 2), (1, 2)]]),(1, [])",
        new string?[] { "[[(0,0),(5,0),(5,5),(0,5)],[(1,1),(2,1),(2,2),(1,2)]]", "[]" },
        bulkRows: new[]
        {
            new Row<Point[][]>
            {
                Id = 0,
                Val = new[]
                {
                    new[] { new Point(0, 0), new Point(5, 0), new Point(5, 5), new Point(0, 5) },
                    new[] { new Point(1, 1), new Point(2, 1), new Point(2, 2), new Point(1, 2) },
                },
            },
            new Row<Point[][]> { Id = 1, Val = Array.Empty<Point[]>() },
        });

    [Fact]
    public Task MultiPolygon_() => RunCliCrossTest(
        "MultiPolygon",
        "(0, [[[(0, 0), (1, 0), (1, 1), (0, 1)]], [[(5, 5), (6, 5), (6, 6), (5, 6)]]]),(1, [])",
        new string?[] { "[[[(0,0),(1,0),(1,1),(0,1)]],[[(5,5),(6,5),(6,6),(5,6)]]]", "[]" },
        bulkRows: new[]
        {
            new Row<Point[][][]>
            {
                Id = 0,
                Val = new[]
                {
                    new[] { new[] { new Point(0, 0), new Point(1, 0), new Point(1, 1), new Point(0, 1) } },
                    new[] { new[] { new Point(5, 5), new Point(6, 5), new Point(6, 6), new Point(5, 6) } },
                },
            },
            new Row<Point[][][]> { Id = 1, Val = Array.Empty<Point[][]>() },
        });

    #endregion

    #region Experimental (JSON / Dynamic / Variant)

    private static readonly string[] JsonCliArgs = { "--allow_experimental_json_type=1" };
    private static readonly string[] JsonNativeSettings = { "SET allow_experimental_json_type = 1" };
    private static readonly string[] DynamicCliArgs = { "--allow_experimental_dynamic_type=1" };
    private static readonly string[] DynamicNativeSettings = { "SET allow_experimental_dynamic_type = 1" };
    private static readonly string[] VariantCliArgs =
    {
        "--allow_experimental_variant_type=1",
        "--allow_suspicious_variant_types=1",
    };
    private static readonly string[] VariantNativeSettings =
    {
        "SET allow_experimental_variant_type = 1",
        "SET allow_suspicious_variant_types = 1",
    };

    // Keys chosen already in alphabetical order — the JSON type re-orders paths, so this
    // keeps the anchored canonical form order-stable.
    [Fact]
    public Task Json_() => RunCliCrossTest(
        "JSON",
        "(0, '{}'),(1, '{\"age\":30,\"name\":\"alice\"}')",
        new string?[] { "{}", "{\"age\":30,\"name\":\"alice\"}" },
        bulkRows: new[]
        {
            new Row<string> { Id = 0, Val = "{}" },
            new Row<string> { Id = 1, Val = "{\"age\":30,\"name\":\"alice\"}" },
        },
        cliArgs: JsonCliArgs,
        nativeSessionSettings: JsonNativeSettings);

    // Server quirk (consistent across all clients): toString() of a NULL Dynamic yields
    // an empty string, while a NULL Variant yields the literal text "ᴺᵁᴸᴸ" — neither is
    // SQL NULL.
    [Fact]
    public Task Dynamic_() => RunCliCrossTest(
        "Dynamic",
        "(0, NULL),(1, 42::Int64),(2, 'hello')",
        new string?[] { "", "42", "hello" },
        bulkRows: new[]
        {
            new Row<ClickHouseDynamic> { Id = 0, Val = ClickHouseDynamic.Null },
            new Row<ClickHouseDynamic> { Id = 1, Val = new ClickHouseDynamic(0, (long)42, "Int64") },
            new Row<ClickHouseDynamic> { Id = 2, Val = new ClickHouseDynamic(1, "hello", "String") },
        },
        cliArgs: DynamicCliArgs,
        nativeSessionSettings: DynamicNativeSettings);

    [Fact]
    public Task Variant_() => RunCliCrossTest(
        "Variant(Int64, String)",
        "(0, NULL),(1, 42::Int64),(2, 'hello')",
        new string?[] { "ᴺᵁᴸᴸ", "42", "hello" },
        bulkRows: new[]
        {
            new Row<ClickHouseVariant> { Id = 0, Val = ClickHouseVariant.Null },
            new Row<ClickHouseVariant> { Id = 1, Val = new ClickHouseVariant(0, (long)42) },
            new Row<ClickHouseVariant> { Id = 2, Val = new ClickHouseVariant(1, "hello") },
        },
        cliArgs: VariantCliArgs,
        nativeSessionSettings: VariantNativeSettings);

    #endregion

    #region Nested

    // Nested(key, value) is sugar for parallel arrays on the wire, so the canonical read
    // is two columns and this fact is written by hand rather than via RunCliCrossTest.
    [Fact]
    public async Task Nested_()
    {
        var table = $"smoke_cli_{Guid.NewGuid():N}";
        var expected = new[]
        {
            new string?[] { "['a','b']", "[10,20]" },
            new string?[] { "[]", "[]" },
        };
        try
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString,
                $"CREATE TABLE {table} (id Int32, nested Nested(key String, value Int32)) ENGINE = Memory");

            // (a) CLI insert -> CH.Native reads
            await CliQueryHelper.ExecuteNonQueryAsync(_fixture,
                $"INSERT INTO {table} VALUES (0, ['a','b'], [10,20]),(1, [], [])");

            var nativeCanonical = await QueryNativeAsync(null,
                $"SELECT toString(nested.key), toString(nested.value) FROM {table} ORDER BY id");
            Assert.Equal(expected, nativeCanonical.Select(r => r.Cast<string?>().ToArray()).ToArray());

            var nativeRaw = await QueryNativeAsync(null,
                $"SELECT nested.key, nested.value FROM {table} ORDER BY id");
            Assert.Equal(new[] { "a", "b" }, nativeRaw[0][0]);
            Assert.Equal(new[] { 10, 20 }, nativeRaw[0][1]);
            Assert.Equal(Array.Empty<string>(), nativeRaw[1][0]);
            Assert.Equal(Array.Empty<int>(), nativeRaw[1][1]);

            // (b) CH.Native bulk insert -> CLI reads
            await NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString,
                $"TRUNCATE TABLE {table}");

            await using var conn = new ClickHouseConnection(_fixture.NativeConnectionString);
            await conn.OpenAsync();
            await using var inserter = conn.CreateBulkInserter<NestedRow>(table);
            await inserter.InitAsync();
            await inserter.AddAsync(new NestedRow { Id = 0, NestedKey = new[] { "a", "b" }, NestedValue = new[] { 10, 20 } });
            await inserter.AddAsync(new NestedRow { Id = 1, NestedKey = Array.Empty<string>(), NestedValue = Array.Empty<int>() });
            await inserter.CompleteAsync();

            var cliRead = await CliQueryHelper.QueryCanonicalAsync(_fixture,
                $"SELECT toString(nested.key), toString(nested.value) FROM {table} ORDER BY id");
            Assert.Equal(expected, cliRead.ToArray());
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    #endregion

    private class Row<T>
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "val", Order = 1)] public T Val { get; set; } = default!;
    }

    private class NestedRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "nested.key", Order = 1)] public string[] NestedKey { get; set; } = Array.Empty<string>();
        [ClickHouseColumn(Name = "nested.value", Order = 2)] public int[] NestedValue { get; set; } = Array.Empty<int>();
    }
}
