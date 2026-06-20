using System.Net;
using CH.Native.SmokeTests.Fixtures;
using CH.Native.SmokeTests.Helpers;
using ClickHouse.Driver.Copy;
using ClickHouse.Driver.Utility;
using Xunit;
using DriverConnection = ClickHouse.Driver.ADO.ClickHouseConnection;

// ClickHouseBulkCopy is marked obsolete in 1.2.0 (functionality moving to
// ClickHouseClient) but is still the driver's stable bulk-write surface today.
#pragma warning disable CS0618

namespace CH.Native.SmokeTests.Query;

// The direction the other smoke suites don't cover: the official ClickHouse.Driver
// (HTTP, port 8123) writes, CH.Native reads over the native protocol, and the result is
// asserted against ANCHORED CLR values — so a failure points at whichever leg deviates
// from ground truth rather than just a disagreement between the two clients.
[Collection("SmokeTest")]
public class CrossClientDriverInsertSmokeTests
{
    private readonly SmokeTestFixture _fixture;

    public CrossClientDriverInsertSmokeTests(SmokeTestFixture fixture)
    {
        _fixture = fixture;
    }

    private async Task RunDriverInsertTest(
        string columnType,
        object?[] driverValues,
        object?[]? expectedClr = null,
        string[]? expectedCanonical = null)
    {
        expectedClr ??= driverValues;
        var table = $"smoke_xdrv_{Guid.NewGuid():N}";
        try
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString,
                $"CREATE TABLE {table} (id Int32, val {columnType}) ENGINE = Memory");

            using (var conn = new DriverConnection(_fixture.DriverConnectionString))
            {
                await conn.OpenAsync();
                using var bulk = new ClickHouseBulkCopy(conn)
                {
                    DestinationTableName = table,
                    ColumnNames = new[] { "id", "val" },
                };
                await bulk.WriteToServerAsync(
                    driverValues.Select((v, i) => new object?[] { i, v }));
            }

            var nativeRaw = await NativeQueryHelper.QueryStreamAsync(
                _fixture.NativeConnectionString,
                $"SELECT val FROM {table} ORDER BY id");
            ResultComparer.AssertResultsEqual(
                nativeRaw,
                expectedClr.Select(v => new[] { v }).ToList(),
                $"Driver insert -> native read: {columnType}");

            if (expectedCanonical is not null)
            {
                var nativeCanonical = await NativeQueryHelper.QueryStreamAsync(
                    _fixture.NativeConnectionString,
                    $"SELECT toString(val) FROM {table} ORDER BY id");
                Assert.Equal(expectedCanonical,
                    nativeCanonical.Select(r => (string?)r[0]).ToArray());
            }
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public Task Int64_Extremes() => RunDriverInsertTest(
        "Int64",
        new object?[] { long.MinValue, 0L, long.MaxValue });

    [Fact]
    public Task UInt64_AboveLongMax() => RunDriverInsertTest(
        "UInt64",
        new object?[] { 0ul, (ulong)long.MaxValue + 1, ulong.MaxValue },
        expectedCanonical: new[] { "0", "9223372036854775808", "18446744073709551615" });

    [Fact]
    public Task Float64_Specials() => RunDriverInsertTest(
        "Float64",
        new object?[] { double.NaN, double.PositiveInfinity, double.NegativeInfinity },
        expectedCanonical: new[] { "nan", "inf", "-inf" });

    [Fact]
    public Task Decimal128_FullScale() => RunDriverInsertTest(
        "Decimal128(18)",
        new object?[] { -99999999.999999999999999999m, 0m, 99999999.999999999999999999m });

    [Fact]
    public Task String_Utf8AndControlChars() => RunDriverInsertTest(
        "String",
        new object?[] { "", "тест 🦀 混合", "tab\there\nnewline" });

    [Fact]
    public Task Date32_WideRange() => RunDriverInsertTest(
        "Date32",
        new object?[] { new DateOnly(1900, 1, 1), new DateOnly(2000, 6, 15), new DateOnly(2283, 11, 11) },
        expectedCanonical: new[] { "1900-01-01", "2000-06-15", "2283-11-11" });

    [Fact]
    public Task DateTime64_Millis() => RunDriverInsertTest(
        "DateTime64(3, 'UTC')",
        new object?[]
        {
            new DateTime(1970, 1, 1, 0, 0, 0, 1, DateTimeKind.Utc),
            new DateTime(2024, 1, 1, 12, 30, 45, 123, DateTimeKind.Utc),
        });

    [Fact]
    public Task Uuid_() => RunDriverInsertTest(
        "UUID",
        new object?[] { Guid.Empty, Guid.Parse("550e8400-e29b-41d4-a716-446655440000") });

    [Fact]
    public Task IPv6_() => RunDriverInsertTest(
        "IPv6",
        new object?[] { IPAddress.Parse("2001:db8::1"), IPAddress.IPv6Loopback });

    [Fact]
    public Task ArrayInt32() => RunDriverInsertTest(
        "Array(Int32)",
        new object?[] { new[] { 1, 2, 3 }, Array.Empty<int>() });

    [Fact]
    public Task MapStringInt32() => RunDriverInsertTest(
        "Map(String, Int32)",
        new object?[]
        {
            new Dictionary<string, int> { ["a"] = 1, ["b"] = 2 },
            new Dictionary<string, int>(),
        });

    [Fact]
    public Task NullableInt32_WithDbNull() => RunDriverInsertTest(
        "Nullable(Int32)",
        new object?[] { 42, DBNull.Value, int.MinValue },
        expectedClr: new object?[] { 42, null, int.MinValue });

    [Fact]
    public Task LowCardinalityString() => RunDriverInsertTest(
        "LowCardinality(String)",
        new object?[] { "red", "green", "red" });

    // One wide row shape through ClickHouseBulkCopy, read back column-by-column natively.
    [Fact]
    public async Task WideRow_MultiType()
    {
        var table = $"smoke_xdrv_{Guid.NewGuid():N}";
        try
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString,
                $@"CREATE TABLE {table} (
                    id Int64,
                    name Nullable(String),
                    tags Array(Int32),
                    ts DateTime64(3, 'UTC'),
                    uid UUID,
                    amount Decimal64(4),
                    color LowCardinality(String)
                ) ENGINE = Memory");

            var ts = new DateTime(2024, 6, 15, 8, 30, 0, 250, DateTimeKind.Utc);
            var uid = Guid.Parse("61f0c404-5cb3-4f7b-907b-a6006ad3dba0");

            using (var conn = new DriverConnection(_fixture.DriverConnectionString))
            {
                await conn.OpenAsync();
                using var bulk = new ClickHouseBulkCopy(conn)
                {
                    DestinationTableName = table,
                    ColumnNames = new[] { "id", "name", "tags", "ts", "uid", "amount", "color" },
                };
                await bulk.WriteToServerAsync(new List<object?[]>
                {
                    new object?[] { 1L, "alice", new[] { 1, 2 }, ts, uid, 12.3456m, "red" },
                    new object?[] { 2L, DBNull.Value, Array.Empty<int>(), ts, Guid.Empty, 0m, "green" },
                });
            }

            var native = await NativeQueryHelper.QueryStreamAsync(
                _fixture.NativeConnectionString,
                $"SELECT id, name, tags, ts, uid, amount, color FROM {table} ORDER BY id");

            ResultComparer.AssertResultsEqual(native, new List<object?[]>
            {
                new object?[] { 1L, "alice", new[] { 1, 2 }, ts, uid, 12.3456m, "red" },
                new object?[] { 2L, null, Array.Empty<int>(), ts, Guid.Empty, 0m, "green" },
            }, "WideRow_MultiType");
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    // Parameterized single-row insert through the official driver's {name:Type} binding.
    [Fact]
    public async Task ParameterizedInsert_UInt64AndString()
    {
        var table = $"smoke_xdrv_{Guid.NewGuid():N}";
        try
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString,
                $"CREATE TABLE {table} (id Int32, big UInt64, txt String) ENGINE = Memory");

            using (var conn = new DriverConnection(_fixture.DriverConnectionString))
            {
                await conn.OpenAsync();
                using var cmd = conn.CreateCommand();
                cmd.CommandText =
                    $"INSERT INTO {table} (id, big, txt) VALUES ({{id:Int32}}, {{big:UInt64}}, {{txt:String}})";
                cmd.AddParameter("id", 0);
                cmd.AddParameter("big", ulong.MaxValue);
                cmd.AddParameter("txt", "тест 🦀");
                await cmd.ExecuteNonQueryAsync();
            }

            var native = await NativeQueryHelper.QueryStreamAsync(
                _fixture.NativeConnectionString,
                $"SELECT id, big, txt FROM {table}");
            ResultComparer.AssertResultsEqual(native, new List<object?[]>
            {
                new object?[] { 0, ulong.MaxValue, "тест 🦀" },
            }, "ParameterizedInsert");
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    #region Timezone / DST (mirrors jvm-clickhouse-native CrossClientTimezoneDstIT)

    // Anchored UTC instants written by the official driver into zoned DateTime columns,
    // read back by CH.Native. The absolute instant must survive regardless of the
    // column's display timezone; the fall-back pair shares a local wall clock (01:30
    // Europe/London on 2024-10-27) but must remain two distinct instants.
    private async Task RunTimezoneTest(string zone, DateTime[] utcInstants)
    {
        var table = $"smoke_xdrv_{Guid.NewGuid():N}";
        try
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString,
                $"CREATE TABLE {table} (id Int32, dt DateTime('{zone}'), dt64 DateTime64(3, '{zone}')) ENGINE = Memory");

            using (var conn = new DriverConnection(_fixture.DriverConnectionString))
            {
                await conn.OpenAsync();
                using var bulk = new ClickHouseBulkCopy(conn)
                {
                    DestinationTableName = table,
                    ColumnNames = new[] { "id", "dt", "dt64" },
                };
                await bulk.WriteToServerAsync(
                    utcInstants.Select((t, i) => new object?[] { i, t, t }));
            }

            var native = await NativeQueryHelper.QueryStreamAsync(
                _fixture.NativeConnectionString,
                $"SELECT toUnixTimestamp(dt), toUnixTimestamp64Milli(dt64) FROM {table} ORDER BY id");

            for (int i = 0; i < utcInstants.Length; i++)
            {
                var expectedSeconds = new DateTimeOffset(utcInstants[i]).ToUnixTimeSeconds();
                var expectedMillis = new DateTimeOffset(utcInstants[i]).ToUnixTimeMilliseconds();
                Assert.Equal(expectedSeconds, Convert.ToInt64(native[i][0]));
                Assert.Equal(expectedMillis, Convert.ToInt64(native[i][1]));
            }
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public Task Timezone_London_SpringForwardGap() => RunTimezoneTest(
        "Europe/London",
        new[]
        {
            // 2024-03-31: 01:00 local jumps to 02:00 — instants either side of the gap.
            new DateTime(2024, 3, 31, 0, 59, 59, DateTimeKind.Utc),
            new DateTime(2024, 3, 31, 1, 0, 0, DateTimeKind.Utc),
        });

    [Fact]
    public Task Timezone_London_FallBackOverlap() => RunTimezoneTest(
        "Europe/London",
        new[]
        {
            // 2024-10-27: both are 01:30 local (BST vs GMT) — must stay distinct.
            new DateTime(2024, 10, 27, 0, 30, 0, DateTimeKind.Utc),
            new DateTime(2024, 10, 27, 1, 30, 0, DateTimeKind.Utc),
        });

    [Fact]
    public Task Timezone_Kolkata_NonWholeHourOffset() => RunTimezoneTest(
        "Asia/Kolkata",
        new[] { new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Utc) });

    [Fact]
    public Task Timezone_Kathmandu_QuarterHourOffset() => RunTimezoneTest(
        "Asia/Kathmandu",
        new[] { new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Utc) });

    [Fact]
    public Task Timezone_LordHowe_HalfHourDst() => RunTimezoneTest(
        "Australia/Lord_Howe",
        new[]
        {
            new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc),  // DST (+11:00)
            new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Utc),  // standard (+10:30)
        });

    #endregion
}
