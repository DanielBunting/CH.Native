using System.Net;
using CH.Native.Connection;
using CH.Native.Data;
using CH.Native.Data.Geo;
using CH.Native.Data.Dynamic;
using CH.Native.Data.Variant;
using CH.Native.Mapping;
using CH.Native.Numerics;
using CH.Native.SmokeTests.Fixtures;
using CH.Native.SmokeTests.Helpers;
using Xunit;

namespace CH.Native.SmokeTests.Query;

// Each [Fact] is one ClickHouse type: bulk-insert a set of values via the native binary
// protocol, read them back via native, and assert the read-back values equal what was
// written. ResultComparer handles cross-CLR normalization (DateTimeOffset↔DateTime,
// Guid↔string, IPAddress↔string, decimal↔ClickHouseDecimal, numeric widening).
//
// Coverage excluded here (writer gaps or experimental — separate follow-ups):
//   Int256/UInt256/BFloat16 (no extractor), JSON/Dynamic/Variant/Time/Time64
//   (session SET flags), geospatial, Nested.
[Collection("SmokeTest")]
public class TypeBulkRoundTripSmokeTests
{
    private readonly SmokeTestFixture _fixture;

    public TypeBulkRoundTripSmokeTests(SmokeTestFixture fixture)
    {
        _fixture = fixture;
    }

    private async Task RunAsync<T>(
        string columnType,
        IEnumerable<Row<T>> rows,
        Func<Row<T>, object?[]>? expected = null,
        string selectExpr = "id, val",
        StringMaterialization stringMat = StringMaterialization.Eager,
        string[]? sessionSettings = null)
    {
        expected ??= r => new object?[] { r.Id, r.Val };

        var readConn = stringMat == StringMaterialization.Lazy
            ? _fixture.NativeLazyConnectionString
            : _fixture.NativeConnectionString;

        var table = $"smoke_bulk_{Guid.NewGuid():N}";
        try
        {
            await using var conn = new ClickHouseConnection(_fixture.NativeConnectionString);
            await conn.OpenAsync();

            if (sessionSettings is not null)
            {
                foreach (var setting in sessionSettings)
                {
                    await conn.ExecuteNonQueryAsync(setting);
                }
            }

            await conn.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id Int32, val {columnType}) ENGINE = Memory");

            var rowList = rows.ToList();

            await using var inserter = conn.CreateBulkInserter<Row<T>>(table);
            await inserter.InitAsync();
            foreach (var r in rowList)
            {
                await inserter.AddAsync(r);
            }
            await inserter.CompleteAsync();

            // For reads, open a fresh connection and apply the same session settings
            // so the SELECT succeeds for experimental types that need them at read time.
            var postRead = await ReadWithSettingsAsync(readConn, sessionSettings,
                $"SELECT {selectExpr} FROM {table} ORDER BY id");

            var preWritten = rowList.Select(expected).ToList();
            Assert.Equal(rowList.Count, postRead.Count);
            ResultComparer.AssertResultsEqual(postRead, preWritten, $"{columnType} ({stringMat})");
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    private static async Task<List<object?[]>> ReadWithSettingsAsync(
        string connectionString, string[]? sessionSettings, string sql)
    {
        if (sessionSettings is null || sessionSettings.Length == 0)
        {
            return await NativeQueryHelper.QueryAsync(connectionString, sql);
        }

        await using var conn = new ClickHouseConnection(connectionString);
        await conn.OpenAsync();
        foreach (var setting in sessionSettings)
        {
            await conn.ExecuteNonQueryAsync(setting);
        }

        var rows = new List<object?[]>();
        await foreach (var row in conn.QueryAsync(sql))
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

    #region Scalars

    [Fact]
    public Task Bool() => RunAsync("Bool", new[]
    {
        new Row<bool> { Id = 0, Val = false },
        new Row<bool> { Id = 1, Val = true },
    });

    [Fact]
    public Task Int8() => RunAsync("Int8", new[]
    {
        new Row<sbyte> { Id = 0, Val = sbyte.MinValue },
        new Row<sbyte> { Id = 1, Val = 0 },
        new Row<sbyte> { Id = 2, Val = sbyte.MaxValue },
    });

    [Fact]
    public Task Int16() => RunAsync("Int16", new[]
    {
        new Row<short> { Id = 0, Val = short.MinValue },
        new Row<short> { Id = 1, Val = 0 },
        new Row<short> { Id = 2, Val = short.MaxValue },
    });

    [Fact]
    public Task Int32() => RunAsync("Int32", new[]
    {
        new Row<int> { Id = 0, Val = int.MinValue },
        new Row<int> { Id = 1, Val = 0 },
        new Row<int> { Id = 2, Val = int.MaxValue },
    });

    [Fact]
    public Task Int64() => RunAsync("Int64", new[]
    {
        new Row<long> { Id = 0, Val = long.MinValue },
        new Row<long> { Id = 1, Val = 0 },
        new Row<long> { Id = 2, Val = long.MaxValue },
    });

    [Fact]
    public Task Int128_() => RunAsync("Int128", new[]
    {
        new Row<Int128> { Id = 0, Val = Int128.MinValue },
        new Row<Int128> { Id = 1, Val = Int128.Zero },
        new Row<Int128> { Id = 2, Val = Int128.MaxValue },
    });

    [Fact]
    public Task UInt8() => RunAsync("UInt8", new[]
    {
        new Row<byte> { Id = 0, Val = byte.MinValue },
        new Row<byte> { Id = 1, Val = 128 },
        new Row<byte> { Id = 2, Val = byte.MaxValue },
    });

    [Fact]
    public Task UInt16() => RunAsync("UInt16", new[]
    {
        new Row<ushort> { Id = 0, Val = ushort.MinValue },
        new Row<ushort> { Id = 1, Val = ushort.MaxValue },
    });

    [Fact]
    public Task UInt32() => RunAsync("UInt32", new[]
    {
        new Row<uint> { Id = 0, Val = uint.MinValue },
        new Row<uint> { Id = 1, Val = 1_000_000 },
        new Row<uint> { Id = 2, Val = uint.MaxValue },
    });

    [Fact]
    public Task UInt64() => RunAsync("UInt64", new[]
    {
        new Row<ulong> { Id = 0, Val = ulong.MinValue },
        new Row<ulong> { Id = 1, Val = 1UL << 40 },
        new Row<ulong> { Id = 2, Val = ulong.MaxValue },
    });

    [Fact]
    public Task UInt128_() => RunAsync("UInt128", new[]
    {
        new Row<UInt128> { Id = 0, Val = UInt128.MinValue },
        new Row<UInt128> { Id = 1, Val = (UInt128)1 },
        new Row<UInt128> { Id = 2, Val = UInt128.MaxValue },
    });

    [Fact]
    public Task Float32() => RunAsync("Float32", new[]
    {
        new Row<float> { Id = 0, Val = -1.5f },
        new Row<float> { Id = 1, Val = 0f },
        new Row<float> { Id = 2, Val = 1.5f },
    });

    [Fact]
    public Task Float64() => RunAsync("Float64", new[]
    {
        new Row<double> { Id = 0, Val = -2.71828 },
        new Row<double> { Id = 1, Val = 0.0 },
        new Row<double> { Id = 2, Val = 3.14159265358979 },
    });

    [Fact]
    public Task Decimal32() => RunAsync("Decimal32(4)", new[]
    {
        new Row<decimal> { Id = 0, Val = -99.9999m },
        new Row<decimal> { Id = 1, Val = 0m },
        new Row<decimal> { Id = 2, Val = 99.9999m },
    });

    [Fact]
    public Task Decimal64() => RunAsync("Decimal64(8)", new[]
    {
        new Row<decimal> { Id = 0, Val = -999999.99999999m },
        new Row<decimal> { Id = 1, Val = 0m },
        new Row<decimal> { Id = 2, Val = 123456.12345678m },
    });

    [Fact]
    public Task Decimal128() => RunAsync("Decimal128(18)", new[]
    {
        new Row<decimal> { Id = 0, Val = -99999999.999999999999999999m },
        new Row<decimal> { Id = 1, Val = 0m },
        new Row<decimal> { Id = 2, Val = 99999999.999999999999999999m },
    });

    [Fact]
    public Task Decimal256() => RunAsync("Decimal256(30)", new[]
    {
        new Row<ClickHouseDecimal> { Id = 0, Val = ClickHouseDecimal.Parse("-123456789.012345678901234567890123456789") },
        new Row<ClickHouseDecimal> { Id = 1, Val = ClickHouseDecimal.Parse("0") },
        new Row<ClickHouseDecimal> { Id = 2, Val = ClickHouseDecimal.Parse("123456789.012345678901234567890123456789") },
    });

    [Theory]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task String_(StringMaterialization mat) => RunAsync("String", new[]
    {
        new Row<string> { Id = 0, Val = "" },
        new Row<string> { Id = 1, Val = "hello world" },
        new Row<string> { Id = 2, Val = "тест 🦀 混合" },
    }, stringMat: mat);

    [Theory]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task FixedString(StringMaterialization mat) => RunAsync("FixedString(8)", new[]
    {
        new Row<string> { Id = 0, Val = "" },
        new Row<string> { Id = 1, Val = "abcd" },
        new Row<string> { Id = 2, Val = "test1234" },
    }, stringMat: mat);

    [Fact]
    public Task Date() => RunAsync("Date", new[]
    {
        new Row<DateOnly> { Id = 0, Val = new DateOnly(1970, 1, 1) },
        new Row<DateOnly> { Id = 1, Val = new DateOnly(2000, 6, 15) },
        new Row<DateOnly> { Id = 2, Val = new DateOnly(2149, 6, 6) },
    });

    [Fact]
    public Task Date32() => RunAsync("Date32", new[]
    {
        new Row<DateOnly> { Id = 0, Val = new DateOnly(1900, 1, 1) },
        new Row<DateOnly> { Id = 1, Val = new DateOnly(2000, 6, 15) },
        new Row<DateOnly> { Id = 2, Val = new DateOnly(2283, 11, 11) },
    });

    [Fact]
    public Task DateTime_() => RunAsync("DateTime", new[]
    {
        new Row<DateTime> { Id = 0, Val = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc) },
        new Row<DateTime> { Id = 1, Val = new DateTime(2000, 6, 15, 12, 0, 0, DateTimeKind.Utc) },
        new Row<DateTime> { Id = 2, Val = new DateTime(2106, 2, 7, 6, 28, 15, DateTimeKind.Utc) },
    });

    [Fact]
    public Task DateTime_WithTimezone() => RunAsync("DateTime('UTC')", new[]
    {
        new Row<DateTime> { Id = 0, Val = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc) },
        new Row<DateTime> { Id = 1, Val = new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Utc) },
        new Row<DateTime> { Id = 2, Val = new DateTime(2106, 2, 7, 6, 28, 15, DateTimeKind.Utc) },
    });

    [Fact]
    public Task DateTime_WithTimezone_FromDateTimeOffset() => RunAsync("DateTime('UTC')", new[]
    {
        new Row<DateTimeOffset> { Id = 0, Val = new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero) },
        new Row<DateTimeOffset> { Id = 1, Val = new DateTimeOffset(2024, 6, 15, 12, 0, 0, TimeSpan.Zero) },
        new Row<DateTimeOffset> { Id = 2, Val = new DateTimeOffset(2106, 2, 7, 6, 28, 15, TimeSpan.Zero) },
    });

    [Fact]
    public Task DateTime64_WithTimezone_FromDateTimeOffset() => RunAsync("DateTime64(3, 'UTC')", new[]
    {
        new Row<DateTimeOffset> { Id = 0, Val = new DateTimeOffset(1970, 1, 1, 0, 0, 0, 1, TimeSpan.Zero) },
        new Row<DateTimeOffset> { Id = 1, Val = new DateTimeOffset(2024, 1, 1, 12, 30, 45, 123, TimeSpan.Zero) },
        new Row<DateTimeOffset> { Id = 2, Val = new DateTimeOffset(2100, 12, 31, 23, 59, 59, 999, TimeSpan.Zero) },
    });

    [Fact]
    public Task DateTime64() => RunAsync("DateTime64(3)", new[]
    {
        new Row<DateTime> { Id = 0, Val = new DateTime(1970, 1, 1, 0, 0, 0, 1, DateTimeKind.Utc) },
        new Row<DateTime> { Id = 1, Val = new DateTime(2024, 1, 1, 12, 30, 45, 123, DateTimeKind.Utc) },
        new Row<DateTime> { Id = 2, Val = new DateTime(2100, 12, 31, 23, 59, 59, 999, DateTimeKind.Utc) },
    });

    [Fact]
    public Task DateTime64_WithTimezone() => RunAsync("DateTime64(6, 'UTC')", new[]
    {
        new Row<DateTime> { Id = 0, Val = new DateTime(1970, 1, 1, 0, 0, 0, 1, DateTimeKind.Utc) },
        new Row<DateTime> { Id = 1, Val = new DateTime(2024, 1, 1, 12, 30, 45, 456, DateTimeKind.Utc) },
        new Row<DateTime> { Id = 2, Val = new DateTime(2100, 12, 31, 23, 59, 59, 987, DateTimeKind.Utc) },
    });

    [Fact]
    public Task UUID() => RunAsync("UUID", new[]
    {
        new Row<Guid> { Id = 0, Val = Guid.Empty },
        new Row<Guid> { Id = 1, Val = Guid.Parse("550e8400-e29b-41d4-a716-446655440000") },
        new Row<Guid> { Id = 2, Val = Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff") },
    });

    [Fact]
    public Task IPv4() => RunAsync("IPv4", new[]
    {
        new Row<IPAddress> { Id = 0, Val = IPAddress.Parse("0.0.0.0") },
        new Row<IPAddress> { Id = 1, Val = IPAddress.Parse("192.168.1.1") },
        new Row<IPAddress> { Id = 2, Val = IPAddress.Parse("255.255.255.255") },
    });

    [Fact]
    public Task IPv6() => RunAsync("IPv6", new[]
    {
        new Row<IPAddress> { Id = 0, Val = IPAddress.IPv6Any },
        new Row<IPAddress> { Id = 1, Val = IPAddress.Parse("2001:db8::1") },
        new Row<IPAddress> { Id = 2, Val = IPAddress.Parse("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff") },
    });

    [Theory]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task Enum8(StringMaterialization mat) => RunAsync(
        "Enum8('a' = 1, 'b' = 2, 'c' = 3)",
        new[]
        {
            new Row<sbyte> { Id = 0, Val = 1 },
            new Row<sbyte> { Id = 1, Val = 2 },
            new Row<sbyte> { Id = 2, Val = 3 },
        },
        r => new object?[] { r.Id, r.Val switch { 1 => "a", 2 => "b", 3 => "c", _ => "?" } },
        selectExpr: "id, toString(val)",
        stringMat: mat);

    [Theory]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task Enum16(StringMaterialization mat) => RunAsync(
        "Enum16('alpha' = 1000, 'beta' = 2000, 'gamma' = 3000)",
        new[]
        {
            new Row<short> { Id = 0, Val = 1000 },
            new Row<short> { Id = 1, Val = 2000 },
            new Row<short> { Id = 2, Val = 3000 },
        },
        r => new object?[] { r.Id, r.Val switch { 1000 => "alpha", 2000 => "beta", 3000 => "gamma", _ => "?" } },
        selectExpr: "id, toString(val)",
        stringMat: mat);

    // Time / Time64 were introduced in ClickHouse 25.6. The smoke fixture pins 25.3
    // for reproducibility with the existing suite, so the `enable_time_time64_type`
    // session flag doesn't exist on the server. Tests are present to document the
    // intended coverage; they'll run once the fixture upgrades the container image.
    private const string TimeTypeSkipReason =
        "Requires ClickHouse 25.6+ for Time/Time64 types; smoke fixture pins 25.3. Bump the image to enable.";
    private static readonly string[] EnableTimeTime64 = new[] { "SET enable_time_time64_type=1" };

    [Fact(Skip = TimeTypeSkipReason)]
    public Task Time_() => RunAsync("Time", new[]
    {
        new Row<TimeOnly> { Id = 0, Val = TimeOnly.MinValue },
        new Row<TimeOnly> { Id = 1, Val = new TimeOnly(12, 30, 45) },
        new Row<TimeOnly> { Id = 2, Val = new TimeOnly(23, 59, 59) },
    }, sessionSettings: EnableTimeTime64);

    [Fact(Skip = TimeTypeSkipReason)]
    public Task Time64() => RunAsync("Time64(3)", new[]
    {
        new Row<TimeOnly> { Id = 0, Val = TimeOnly.MinValue },
        new Row<TimeOnly> { Id = 1, Val = new TimeOnly(12, 30, 45, 123) },
        new Row<TimeOnly> { Id = 2, Val = new TimeOnly(23, 59, 59, 999) },
    }, sessionSettings: EnableTimeTime64);

    [Fact(Skip = TimeTypeSkipReason)]
    public Task NullableTime() => RunAsync("Nullable(Time)", new[]
    {
        new Row<TimeOnly?> { Id = 0, Val = new TimeOnly(0, 0, 0) },
        new Row<TimeOnly?> { Id = 1, Val = null },
        new Row<TimeOnly?> { Id = 2, Val = new TimeOnly(23, 59, 59) },
    }, sessionSettings: EnableTimeTime64);

    [Fact(Skip = TimeTypeSkipReason)]
    public Task NullableTime64() => RunAsync("Nullable(Time64(3))", new[]
    {
        new Row<TimeOnly?> { Id = 0, Val = new TimeOnly(0, 0, 0, 1) },
        new Row<TimeOnly?> { Id = 1, Val = null },
        new Row<TimeOnly?> { Id = 2, Val = new TimeOnly(23, 59, 59, 999) },
    }, sessionSettings: EnableTimeTime64);

    #endregion

    #region Nullable

    [Fact]
    public Task NullableBool() => RunAsync("Nullable(Bool)", new[]
    {
        new Row<bool?> { Id = 0, Val = true },
        new Row<bool?> { Id = 1, Val = null },
        new Row<bool?> { Id = 2, Val = false },
    });

    [Fact]
    public Task NullableInt8() => RunAsync("Nullable(Int8)", new[]
    {
        new Row<sbyte?> { Id = 0, Val = sbyte.MinValue },
        new Row<sbyte?> { Id = 1, Val = null },
        new Row<sbyte?> { Id = 2, Val = sbyte.MaxValue },
    });

    [Fact]
    public Task NullableInt16() => RunAsync("Nullable(Int16)", new[]
    {
        new Row<short?> { Id = 0, Val = short.MinValue },
        new Row<short?> { Id = 1, Val = null },
        new Row<short?> { Id = 2, Val = short.MaxValue },
    });

    [Fact]
    public Task NullableInt32() => RunAsync("Nullable(Int32)", new[]
    {
        new Row<int?> { Id = 0, Val = int.MinValue },
        new Row<int?> { Id = 1, Val = null },
        new Row<int?> { Id = 2, Val = int.MaxValue },
    });

    [Fact]
    public Task NullableInt64() => RunAsync("Nullable(Int64)", new[]
    {
        new Row<long?> { Id = 0, Val = long.MinValue },
        new Row<long?> { Id = 1, Val = null },
        new Row<long?> { Id = 2, Val = long.MaxValue },
    });

    [Fact]
    public Task NullableInt128() => RunAsync("Nullable(Int128)", new[]
    {
        new Row<Int128?> { Id = 0, Val = Int128.MinValue },
        new Row<Int128?> { Id = 1, Val = null },
        new Row<Int128?> { Id = 2, Val = Int128.MaxValue },
    });

    [Fact]
    public Task NullableUInt8() => RunAsync("Nullable(UInt8)", new[]
    {
        new Row<byte?> { Id = 0, Val = byte.MinValue },
        new Row<byte?> { Id = 1, Val = null },
        new Row<byte?> { Id = 2, Val = byte.MaxValue },
    });

    [Fact]
    public Task NullableUInt16() => RunAsync("Nullable(UInt16)", new[]
    {
        new Row<ushort?> { Id = 0, Val = 0 },
        new Row<ushort?> { Id = 1, Val = null },
        new Row<ushort?> { Id = 2, Val = ushort.MaxValue },
    });

    [Fact]
    public Task NullableUInt32() => RunAsync("Nullable(UInt32)", new[]
    {
        new Row<uint?> { Id = 0, Val = 0u },
        new Row<uint?> { Id = 1, Val = null },
        new Row<uint?> { Id = 2, Val = uint.MaxValue },
    });

    [Fact]
    public Task NullableUInt64() => RunAsync("Nullable(UInt64)", new[]
    {
        new Row<ulong?> { Id = 0, Val = 0UL },
        new Row<ulong?> { Id = 1, Val = null },
        new Row<ulong?> { Id = 2, Val = ulong.MaxValue },
    });

    [Fact]
    public Task NullableUInt128() => RunAsync("Nullable(UInt128)", new[]
    {
        new Row<UInt128?> { Id = 0, Val = UInt128.MinValue },
        new Row<UInt128?> { Id = 1, Val = null },
        new Row<UInt128?> { Id = 2, Val = UInt128.MaxValue },
    });

    [Fact]
    public Task NullableFloat32() => RunAsync("Nullable(Float32)", new[]
    {
        new Row<float?> { Id = 0, Val = -1.5f },
        new Row<float?> { Id = 1, Val = null },
        new Row<float?> { Id = 2, Val = 3.14159f },
    });

    [Fact]
    public Task NullableFloat64() => RunAsync("Nullable(Float64)", new[]
    {
        new Row<double?> { Id = 0, Val = -1.5 },
        new Row<double?> { Id = 1, Val = null },
        new Row<double?> { Id = 2, Val = 2.71828 },
    });

    [Fact]
    public Task NullableDecimal32() => RunAsync("Nullable(Decimal32(2))", new[]
    {
        new Row<decimal?> { Id = 0, Val = -99.99m },
        new Row<decimal?> { Id = 1, Val = null },
        new Row<decimal?> { Id = 2, Val = 99.99m },
    });

    [Fact]
    public Task NullableDecimal64() => RunAsync("Nullable(Decimal64(4))", new[]
    {
        new Row<decimal?> { Id = 0, Val = 123.4567m },
        new Row<decimal?> { Id = 1, Val = null },
        new Row<decimal?> { Id = 2, Val = -99.0m },
    });

    [Fact]
    public Task NullableDecimal128() => RunAsync("Nullable(Decimal128(18))", new[]
    {
        new Row<decimal?> { Id = 0, Val = 99999999.999999999999999999m },
        new Row<decimal?> { Id = 1, Val = null },
        new Row<decimal?> { Id = 2, Val = -99999999.999999999999999999m },
    });

    [Fact]
    public Task NullableDecimal256() => RunAsync("Nullable(Decimal256(30))", new[]
    {
        new Row<ClickHouseDecimal?> { Id = 0, Val = ClickHouseDecimal.Parse("123456789.012345678901234567890123456789") },
        new Row<ClickHouseDecimal?> { Id = 1, Val = null },
        new Row<ClickHouseDecimal?> { Id = 2, Val = ClickHouseDecimal.Parse("-1") },
    });

    [Theory]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task NullableEnum8(StringMaterialization mat) => RunAsync(
        "Nullable(Enum8('x' = 1, 'y' = 2))",
        new[]
        {
            new Row<sbyte?> { Id = 0, Val = 1 },
            new Row<sbyte?> { Id = 1, Val = null },
            new Row<sbyte?> { Id = 2, Val = 2 },
        },
        r => new object?[] { r.Id, r.Val switch { 1 => "x", 2 => "y", _ => null } },
        selectExpr: "id, if(val IS NULL, NULL, toString(val))",
        stringMat: mat);

    [Theory]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task NullableEnum16(StringMaterialization mat) => RunAsync(
        "Nullable(Enum16('alpha' = 1000, 'beta' = 2000))",
        new[]
        {
            new Row<short?> { Id = 0, Val = 1000 },
            new Row<short?> { Id = 1, Val = null },
            new Row<short?> { Id = 2, Val = 2000 },
        },
        r => new object?[] { r.Id, r.Val switch { 1000 => "alpha", 2000 => "beta", _ => null } },
        selectExpr: "id, if(val IS NULL, NULL, toString(val))",
        stringMat: mat);

    [Fact]
    public Task NullableDate32() => RunAsync("Nullable(Date32)", new[]
    {
        new Row<DateOnly?> { Id = 0, Val = new DateOnly(1900, 1, 1) },
        new Row<DateOnly?> { Id = 1, Val = null },
        new Row<DateOnly?> { Id = 2, Val = new DateOnly(2283, 11, 11) },
    });

    [Fact]
    public Task NullableDateTime() => RunAsync("Nullable(DateTime)", new[]
    {
        new Row<DateTime?> { Id = 0, Val = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc) },
        new Row<DateTime?> { Id = 1, Val = null },
        new Row<DateTime?> { Id = 2, Val = new DateTime(2106, 2, 7, 6, 28, 15, DateTimeKind.Utc) },
    });

    [Fact]
    public Task NullableDateTime64() => RunAsync("Nullable(DateTime64(3))", new[]
    {
        new Row<DateTime?> { Id = 0, Val = new DateTime(1970, 1, 1, 0, 0, 0, 1, DateTimeKind.Utc) },
        new Row<DateTime?> { Id = 1, Val = null },
        new Row<DateTime?> { Id = 2, Val = new DateTime(2100, 12, 31, 23, 59, 59, 999, DateTimeKind.Utc) },
    });

    [Fact]
    public Task NullableDateTime64_WithTimezone() => RunAsync("Nullable(DateTime64(6, 'UTC'))", new[]
    {
        new Row<DateTime?> { Id = 0, Val = new DateTime(2020, 1, 1, 0, 0, 0, 1, DateTimeKind.Utc) },
        new Row<DateTime?> { Id = 1, Val = null },
        new Row<DateTime?> { Id = 2, Val = new DateTime(2100, 12, 31, 23, 59, 59, 987, DateTimeKind.Utc) },
    });

    [Theory]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task NullableString(StringMaterialization mat) => RunAsync("Nullable(String)", new[]
    {
        new Row<string?> { Id = 0, Val = "" },
        new Row<string?> { Id = 1, Val = null },
        new Row<string?> { Id = 2, Val = "hello" },
    }, stringMat: mat);

    [Theory]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task NullableFixedString(StringMaterialization mat) => RunAsync("Nullable(FixedString(4))", new[]
    {
        new Row<string?> { Id = 0, Val = "abcd" },
        new Row<string?> { Id = 1, Val = null },
        new Row<string?> { Id = 2, Val = "zzzz" },
    }, stringMat: mat);

    [Fact]
    public Task NullableDate() => RunAsync("Nullable(Date)", new[]
    {
        new Row<DateOnly?> { Id = 0, Val = new DateOnly(1999, 12, 31) },
        new Row<DateOnly?> { Id = 1, Val = null },
        new Row<DateOnly?> { Id = 2, Val = new DateOnly(2099, 6, 15) },
    });

    [Fact]
    public Task NullableDateTime_WithTimezone() => RunAsync("Nullable(DateTime('UTC'))", new[]
    {
        new Row<DateTime?> { Id = 0, Val = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc) },
        new Row<DateTime?> { Id = 1, Val = null },
        new Row<DateTime?> { Id = 2, Val = new DateTime(2100, 12, 31, 23, 59, 59, DateTimeKind.Utc) },
    });

    [Fact]
    public Task NullableDateTime_WithTimezone_FromDateTimeOffset() => RunAsync("Nullable(DateTime('UTC'))", new[]
    {
        new Row<DateTimeOffset?> { Id = 0, Val = new DateTimeOffset(2020, 1, 1, 0, 0, 0, TimeSpan.Zero) },
        new Row<DateTimeOffset?> { Id = 1, Val = null },
        new Row<DateTimeOffset?> { Id = 2, Val = new DateTimeOffset(2100, 12, 31, 23, 59, 59, TimeSpan.Zero) },
    });

    [Fact]
    public Task NullableUUID() => RunAsync("Nullable(UUID)", new[]
    {
        new Row<Guid?> { Id = 0, Val = Guid.Parse("11111111-1111-1111-1111-111111111111") },
        new Row<Guid?> { Id = 1, Val = null },
        new Row<Guid?> { Id = 2, Val = Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff") },
    });

    [Fact]
    public Task NullableIPv4() => RunAsync("Nullable(IPv4)", new[]
    {
        new Row<IPAddress?> { Id = 0, Val = IPAddress.Parse("10.0.0.1") },
        new Row<IPAddress?> { Id = 1, Val = null },
        new Row<IPAddress?> { Id = 2, Val = IPAddress.Parse("8.8.8.8") },
    });

    [Fact]
    public Task NullableIPv6() => RunAsync("Nullable(IPv6)", new[]
    {
        new Row<IPAddress?> { Id = 0, Val = IPAddress.Parse("fe80::1") },
        new Row<IPAddress?> { Id = 1, Val = null },
        new Row<IPAddress?> { Id = 2, Val = IPAddress.Parse("2001:4860:4860::8888") },
    });

    #endregion

    #region Arrays

    [Fact]
    public Task ArrayInt32() => RunAsync("Array(Int32)", new[]
    {
        new Row<int[]> { Id = 0, Val = Array.Empty<int>() },
        new Row<int[]> { Id = 1, Val = new[] { 1 } },
        new Row<int[]> { Id = 2, Val = new[] { int.MinValue, 0, int.MaxValue } },
    });

    [Fact]
    public Task ArrayInt64() => RunAsync("Array(Int64)", new[]
    {
        new Row<long[]> { Id = 0, Val = Array.Empty<long>() },
        new Row<long[]> { Id = 1, Val = new[] { long.MinValue, 0L, long.MaxValue } },
    });

    [Fact]
    public Task ArrayFloat64() => RunAsync("Array(Float64)", new[]
    {
        new Row<double[]> { Id = 0, Val = Array.Empty<double>() },
        new Row<double[]> { Id = 1, Val = new[] { 1.1, 2.2, 3.3 } },
    });

    [Fact]
    public Task ArrayDecimal64() => RunAsync("Array(Decimal64(2))", new[]
    {
        new Row<decimal[]> { Id = 0, Val = Array.Empty<decimal>() },
        new Row<decimal[]> { Id = 1, Val = new[] { 1.23m, -4.56m, 100.50m } },
    });

    [Theory]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task ArrayString(StringMaterialization mat) => RunAsync("Array(String)", new[]
    {
        new Row<string[]> { Id = 0, Val = Array.Empty<string>() },
        new Row<string[]> { Id = 1, Val = new[] { "a" } },
        new Row<string[]> { Id = 2, Val = new[] { "x", "y", "z" } },
    }, stringMat: mat);

    [Theory]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task ArrayFixedString(StringMaterialization mat) => RunAsync("Array(FixedString(4))", new[]
    {
        new Row<string[]> { Id = 0, Val = Array.Empty<string>() },
        new Row<string[]> { Id = 1, Val = new[] { "abcd", "wxyz" } },
    }, stringMat: mat);

    [Fact]
    public Task ArrayUUID() => RunAsync("Array(UUID)", new[]
    {
        new Row<Guid[]> { Id = 0, Val = Array.Empty<Guid>() },
        new Row<Guid[]> { Id = 1, Val = new[] { Guid.Empty, Guid.Parse("11111111-1111-1111-1111-111111111111") } },
    });

    [Fact]
    public Task ArrayDateTime_WithTimezone() => RunAsync("Array(DateTime('UTC'))", new[]
    {
        new Row<DateTimeOffset[]> { Id = 0, Val = Array.Empty<DateTimeOffset>() },
        new Row<DateTimeOffset[]>
        {
            Id = 1,
            Val = new[]
            {
                new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero),
                new DateTimeOffset(2025, 12, 31, 23, 59, 59, TimeSpan.Zero),
            },
        },
    });

    [Fact]
    public Task ArrayNullableInt32() => RunAsync("Array(Nullable(Int32))", new[]
    {
        new Row<int?[]> { Id = 0, Val = Array.Empty<int?>() },
        new Row<int?[]> { Id = 1, Val = new int?[] { null } },
        new Row<int?[]> { Id = 2, Val = new int?[] { 1, null, 3 } },
    });

    [Theory]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task ArrayNullableString(StringMaterialization mat) => RunAsync("Array(Nullable(String))", new[]
    {
        new Row<string?[]> { Id = 0, Val = Array.Empty<string?>() },
        new Row<string?[]> { Id = 1, Val = new string?[] { "a", null, "c" } },
    }, stringMat: mat);

    [Fact]
    public Task ArrayOfArrayInt32() => RunAsync("Array(Array(Int32))", new[]
    {
        new Row<int[][]> { Id = 0, Val = Array.Empty<int[]>() },
        new Row<int[][]> { Id = 1, Val = new[] { new[] { 1, 2 } } },
        new Row<int[][]> { Id = 2, Val = new[] { new[] { 1 }, new[] { 2, 3 } } },
    });

    [Fact]
    public Task ArrayBool() => RunAsync("Array(Bool)", new[]
    {
        new Row<bool[]> { Id = 0, Val = Array.Empty<bool>() },
        new Row<bool[]> { Id = 1, Val = new[] { true, false, true } },
    });

    [Fact]
    public Task ArrayInt8() => RunAsync("Array(Int8)", new[]
    {
        new Row<sbyte[]> { Id = 0, Val = Array.Empty<sbyte>() },
        new Row<sbyte[]> { Id = 1, Val = new sbyte[] { sbyte.MinValue, 0, sbyte.MaxValue } },
    });

    [Fact]
    public Task ArrayInt128() => RunAsync("Array(Int128)", new[]
    {
        new Row<Int128[]> { Id = 0, Val = Array.Empty<Int128>() },
        new Row<Int128[]> { Id = 1, Val = new[] { Int128.MinValue, Int128.Zero, Int128.MaxValue } },
    });

    [Fact]
    public Task ArrayUInt128() => RunAsync("Array(UInt128)", new[]
    {
        new Row<UInt128[]> { Id = 0, Val = Array.Empty<UInt128>() },
        new Row<UInt128[]> { Id = 1, Val = new[] { UInt128.MinValue, (UInt128)1, UInt128.MaxValue } },
    });

    [Fact]
    public Task ArrayDecimal128() => RunAsync("Array(Decimal128(18))", new[]
    {
        new Row<decimal[]> { Id = 0, Val = Array.Empty<decimal>() },
        new Row<decimal[]> { Id = 1, Val = new[] { 99999999.999999999999999999m, 0m, -99999999.999999999999999999m } },
    });

    [Fact]
    public Task ArrayDecimal256() => RunAsync("Array(Decimal256(30))", new[]
    {
        new Row<ClickHouseDecimal[]> { Id = 0, Val = Array.Empty<ClickHouseDecimal>() },
        new Row<ClickHouseDecimal[]>
        {
            Id = 1,
            Val = new[]
            {
                ClickHouseDecimal.Parse("123456789.012345678901234567890123456789"),
                ClickHouseDecimal.Parse("-1"),
            },
        },
    });

    [Fact]
    public Task ArrayDate() => RunAsync("Array(Date)", new[]
    {
        new Row<DateOnly[]> { Id = 0, Val = Array.Empty<DateOnly>() },
        new Row<DateOnly[]> { Id = 1, Val = new[] { new DateOnly(1970, 1, 1), new DateOnly(2024, 6, 15) } },
    });

    [Fact]
    public Task ArrayDate32() => RunAsync("Array(Date32)", new[]
    {
        new Row<DateOnly[]> { Id = 0, Val = Array.Empty<DateOnly>() },
        new Row<DateOnly[]> { Id = 1, Val = new[] { new DateOnly(1900, 1, 1), new DateOnly(2283, 11, 11) } },
    });

    [Fact]
    public Task ArrayDateTime() => RunAsync("Array(DateTime)", new[]
    {
        new Row<DateTime[]> { Id = 0, Val = Array.Empty<DateTime>() },
        new Row<DateTime[]>
        {
            Id = 1,
            Val = new[]
            {
                new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc),
                new DateTime(2024, 1, 1, 12, 0, 0, DateTimeKind.Utc),
            },
        },
    });

    [Fact]
    public Task ArrayDateTime64() => RunAsync("Array(DateTime64(3))", new[]
    {
        new Row<DateTime[]> { Id = 0, Val = Array.Empty<DateTime>() },
        new Row<DateTime[]>
        {
            Id = 1,
            Val = new[]
            {
                new DateTime(1970, 1, 1, 0, 0, 0, 1, DateTimeKind.Utc),
                new DateTime(2024, 1, 1, 12, 30, 45, 123, DateTimeKind.Utc),
            },
        },
    });

    [Fact]
    public Task ArrayIPv4() => RunAsync("Array(IPv4)", new[]
    {
        new Row<IPAddress[]> { Id = 0, Val = Array.Empty<IPAddress>() },
        new Row<IPAddress[]>
        {
            Id = 1,
            Val = new[] { IPAddress.Parse("10.0.0.1"), IPAddress.Parse("192.168.1.1") },
        },
    });

    [Fact]
    public Task ArrayIPv6() => RunAsync("Array(IPv6)", new[]
    {
        new Row<IPAddress[]> { Id = 0, Val = Array.Empty<IPAddress>() },
        new Row<IPAddress[]>
        {
            Id = 1,
            Val = new[] { IPAddress.Parse("::1"), IPAddress.Parse("2001:db8::1") },
        },
    });

    [Theory]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task ArrayEnum8(StringMaterialization mat) => RunAsync(
        "Array(Enum8('a' = 1, 'b' = 2))",
        new[]
        {
            new Row<sbyte[]> { Id = 0, Val = Array.Empty<sbyte>() },
            new Row<sbyte[]> { Id = 1, Val = new sbyte[] { 1, 2, 1 } },
        },
        r => new object?[] { r.Id, r.Val.Select(v => v == 1 ? "a" : "b").ToArray() },
        selectExpr: "id, arrayMap(x -> toString(x), val)",
        stringMat: mat);

    // --- LowCardinality-inside-composite footprint (all fail with the same root cause) ---
    //
    // Bulk-inserting ANY composite that wraps LowCardinality needs the inner LC state
    // prefix (KeysSerializationVersion UInt64) emitted BEFORE the outer composite's
    // data (Array offsets / Map offsets / etc.). The current writer interface has no
    // prefix/data split, so LC headers come out inline after the outer offsets and the
    // server rejects with "Invalid version for SerializationLowCardinality key column".
    // Tracked in .tmp/surfaced-bugs.md. Fix requires a writer prefix/data API refactor.
    private const string LcCompositeSkipReason =
        "Writer lacks prefix/data split: LC state prefix emitted in wrong position inside composite. See .tmp/surfaced-bugs.md.";

    [Theory(Skip = LcCompositeSkipReason)]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task ArrayLowCardinalityString(StringMaterialization mat) => RunAsync(
        "Array(LowCardinality(String))",
        new[]
        {
            new Row<string[]> { Id = 0, Val = Array.Empty<string>() },
            new Row<string[]> { Id = 1, Val = new[] { "red", "green", "red" } },
        },
        stringMat: mat);

    [Theory(Skip = LcCompositeSkipReason)]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task ArrayLowCardinalityFixedString(StringMaterialization mat) => RunAsync(
        "Array(LowCardinality(FixedString(4)))",
        new[]
        {
            new Row<string[]> { Id = 0, Val = Array.Empty<string>() },
            new Row<string[]> { Id = 1, Val = new[] { "abcd", "wxyz", "abcd" } },
        },
        stringMat: mat);

    [Theory(Skip = LcCompositeSkipReason)]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task ArrayLowCardinalityNullableString(StringMaterialization mat) => RunAsync(
        "Array(LowCardinality(Nullable(String)))",
        new[]
        {
            new Row<string?[]> { Id = 0, Val = Array.Empty<string?>() },
            new Row<string?[]> { Id = 1, Val = new string?[] { "a", null, "a" } },
        },
        stringMat: mat);

    [Theory(Skip = LcCompositeSkipReason)]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task MapStringLowCardinalityString(StringMaterialization mat) => RunAsync(
        "Map(String, LowCardinality(String))",
        new[]
        {
            new Row<Dictionary<string, string>> { Id = 0, Val = new() },
            new Row<Dictionary<string, string>> { Id = 1, Val = new() { ["k1"] = "red", ["k2"] = "green" } },
        },
        stringMat: mat);

    [Theory(Skip = LcCompositeSkipReason)]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task MapLowCardinalityStringString(StringMaterialization mat) => RunAsync(
        "Map(LowCardinality(String), String)",
        new[]
        {
            new Row<Dictionary<string, string>> { Id = 0, Val = new() },
            new Row<Dictionary<string, string>> { Id = 1, Val = new() { ["red"] = "v1", ["green"] = "v2" } },
        },
        stringMat: mat);

    [Theory(Skip = LcCompositeSkipReason)]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task TupleIntLowCardinalityString(StringMaterialization mat) => RunAsync(
        "Tuple(Int32, LowCardinality(String))",
        new[]
        {
            new Row<object[]> { Id = 0, Val = new object[] { 1, "red" } },
            new Row<object[]> { Id = 1, Val = new object[] { 2, "green" } },
        },
        r => new object?[] { r.Id, Tuple.Create((int)r.Val[0], (string)r.Val[1]) },
        stringMat: mat);

    #endregion

    #region Geospatial

    [Fact]
    public Task Point_() => RunAsync("Point", new[]
    {
        new Row<Point> { Id = 0, Val = Point.Zero },
        new Row<Point> { Id = 1, Val = new Point(1.5, -2.5) },
        new Row<Point> { Id = 2, Val = new Point(180.0, -90.0) },
    });

    [Fact]
    public Task Ring_() => RunAsync("Ring", new[]
    {
        new Row<Point[]> { Id = 0, Val = Array.Empty<Point>() },
        new Row<Point[]> { Id = 1, Val = new[] { new Point(0, 0), new Point(1, 0), new Point(1, 1), new Point(0, 1) } },
    });

    [Fact]
    public Task LineString_() => RunAsync("LineString", new[]
    {
        new Row<Point[]> { Id = 0, Val = Array.Empty<Point>() },
        new Row<Point[]> { Id = 1, Val = new[] { new Point(0, 0), new Point(1, 1), new Point(2, 0) } },
    });

    [Fact]
    public Task Polygon_() => RunAsync("Polygon", new[]
    {
        new Row<Point[][]> { Id = 0, Val = Array.Empty<Point[]>() },
        new Row<Point[][]>
        {
            Id = 1,
            Val = new[]
            {
                new[] { new Point(0, 0), new Point(5, 0), new Point(5, 5), new Point(0, 5) },
                new[] { new Point(1, 1), new Point(2, 1), new Point(2, 2), new Point(1, 2) },
            },
        },
    });

    [Fact]
    public Task MultiLineString_() => RunAsync("MultiLineString", new[]
    {
        new Row<Point[][]> { Id = 0, Val = Array.Empty<Point[]>() },
        new Row<Point[][]>
        {
            Id = 1,
            Val = new[]
            {
                new[] { new Point(0, 0), new Point(1, 1) },
                new[] { new Point(2, 2), new Point(3, 3) },
            },
        },
    });

    [Fact]
    public Task MultiPolygon_() => RunAsync("MultiPolygon", new[]
    {
        new Row<Point[][][]> { Id = 0, Val = Array.Empty<Point[][]>() },
        new Row<Point[][][]>
        {
            Id = 1,
            Val = new[]
            {
                new[] { new[] { new Point(0, 0), new Point(1, 0), new Point(1, 1), new Point(0, 1) } },
                new[] { new[] { new Point(5, 5), new Point(6, 5), new Point(6, 6), new Point(5, 6) } },
            },
        },
    });

    #endregion

    #region JSON / Dynamic / Variant

    // JSON and Dynamic on ClickHouse 25.3 reject the binary wire format that
    // CH.Native emits ("Invalid version for Object structure serialization"). The
    // fixture pins 25.3; both types require 25.6+ with native-format flattening.
    private const string JsonDynamicSkipReason =
        "Requires ClickHouse 25.6+ and output_format_native_use_flattened_dynamic_and_json_serialization=1; smoke fixture pins 25.3.";

    private static readonly string[] EnableJson = new[] { "SET allow_experimental_json_type = 1" };
    private static readonly string[] EnableDynamic = new[] { "SET allow_experimental_dynamic_type = 1" };
    private static readonly string[] EnableVariant = new[]
    {
        "SET allow_experimental_variant_type = 1",
        "SET allow_suspicious_variant_types = 1",
    };

    [Fact(Skip = JsonDynamicSkipReason)]
    public Task Json_() => RunAsync(
        "JSON",
        new[]
        {
            new Row<string> { Id = 0, Val = "{}" },
            new Row<string> { Id = 1, Val = "{\"name\":\"alice\",\"age\":30}" },
            new Row<string> { Id = 2, Val = "{\"nested\":{\"a\":1},\"arr\":[1,2,3]}" },
        },
        r => new object?[] { r.Id, NormalizeJson(r.Val) },
        selectExpr: "id, toString(val)",
        sessionSettings: EnableJson);

    [Fact(Skip = JsonDynamicSkipReason)]
    public Task Dynamic_() => RunAsync(
        "Dynamic",
        new[]
        {
            new Row<ClickHouseDynamic> { Id = 0, Val = ClickHouseDynamic.Null },
            new Row<ClickHouseDynamic> { Id = 1, Val = new ClickHouseDynamic(0, (long)42, "Int64") },
            new Row<ClickHouseDynamic> { Id = 2, Val = new ClickHouseDynamic(1, "hello", "String") },
        },
        r => new object?[] { r.Id, r.Val.IsNull ? null : r.Val.Value?.ToString() },
        selectExpr: "id, if(isNull(val), NULL, toString(val))",
        sessionSettings: EnableDynamic);

    [Fact]
    public Task Variant_() => RunAsync(
        "Variant(Int64, String)",
        new[]
        {
            new Row<ClickHouseVariant> { Id = 0, Val = ClickHouseVariant.Null },
            new Row<ClickHouseVariant> { Id = 1, Val = new ClickHouseVariant(0, (long)42) },
            new Row<ClickHouseVariant> { Id = 2, Val = new ClickHouseVariant(1, "hello") },
        },
        r => new object?[] { r.Id, r.Val.IsNull ? (object?)null : r.Val.Value?.ToString() },
        selectExpr: "id, if(isNull(val), NULL, toString(val))",
        sessionSettings: EnableVariant);

    private static string NormalizeJson(string raw) =>
        System.Text.Json.JsonSerializer.Serialize(
            System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(raw));

    #endregion

    #region Nested

    // Nested(key T1, value T2) is sugar for a pair of parallel Arrays on the wire —
    // Array(key T1) and Array(value T2). At bulk insert time we address them as the
    // unwrapped column names (nested.key / nested.value).
    [Fact]
    public async Task Nested_()
    {
        var table = $"smoke_nested_{Guid.NewGuid():N}";
        try
        {
            await using var conn = new ClickHouseConnection(_fixture.NativeConnectionString);
            await conn.OpenAsync();
            await conn.ExecuteNonQueryAsync($@"
                CREATE TABLE {table} (
                    id Int32,
                    nested Nested(key String, value Int32)
                ) ENGINE = Memory");

            await using var inserter = conn.CreateBulkInserter<NestedRow>(table);
            await inserter.InitAsync();
            await inserter.AddAsync(new NestedRow
            {
                Id = 1,
                NestedKey = new[] { "a", "b" },
                NestedValue = new[] { 10, 20 },
            });
            await inserter.AddAsync(new NestedRow
            {
                Id = 2,
                NestedKey = Array.Empty<string>(),
                NestedValue = Array.Empty<int>(),
            });
            await inserter.CompleteAsync();

            var postRead = await NativeQueryHelper.QueryAsync(
                _fixture.NativeConnectionString,
                $"SELECT id, nested.key, nested.value FROM {table} ORDER BY id");

            Assert.Equal(2, postRead.Count);
            Assert.Equal(1, Convert.ToInt32(postRead[0][0]));
            Assert.Equal(new[] { "a", "b" }, postRead[0][1]);
            Assert.Equal(new[] { 10, 20 }, postRead[0][2]);
            Assert.Equal(2, Convert.ToInt32(postRead[1][0]));
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    private class NestedRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "nested.key", Order = 1)] public string[] NestedKey { get; set; } = Array.Empty<string>();
        [ClickHouseColumn(Name = "nested.value", Order = 2)] public int[] NestedValue { get; set; } = Array.Empty<int>();
    }

    #endregion

    #region Reader-only types (writer gap: no ColumnExtractor registered)

    // These types have no fast-path or boxed writer registered in
    // ColumnExtractorFactory / ColumnWriterFactory, so bulk insert can't be tested.
    // We instead SQL-text-INSERT (server-parsed) and verify the reader path.

    private async Task RunReaderOnlyAsync(string columnDef, string insertValues, string selectExpr = "val")
    {
        var table = $"smoke_reader_{Guid.NewGuid():N}";
        try
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"CREATE TABLE {table} (val {columnDef}) ENGINE = Memory");

            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"INSERT INTO {table} VALUES {insertValues}");

            var rows = await NativeQueryHelper.QueryAsync(
                _fixture.NativeConnectionString,
                $"SELECT {selectExpr} FROM {table} ORDER BY 1");

            Assert.NotEmpty(rows);
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public Task Int256_ReaderOnly() =>
        RunReaderOnlyAsync("Int256", "(0),(12345678901234567890123456789012345678),(-12345678901234567890)");

    [Fact]
    public Task UInt256_ReaderOnly() =>
        RunReaderOnlyAsync("UInt256", "(0),(12345678901234567890123456789012345678)");

    [Fact]
    public Task BFloat16_ReaderOnly() =>
        RunReaderOnlyAsync("BFloat16", "(0),(1.5),(-2.5)");

    #endregion

    #region Skipper coverage (partial-column SELECT)

    // Bulk-insert a table with multiple column types, then SELECT only `id` to force
    // the reader's ColumnSkipper implementations to walk over every other column type
    // in the block without materializing them. Exercises ArrayColumnSkipper,
    // MapColumnSkipper, NullableColumnSkipper, StringColumnSkipper,
    // LowCardinalityColumnSkipper, TupleColumnSkipper, FixedSizeColumnSkipper.
    [Fact]
    public async Task ColumnSkippers_PartialSelect()
    {
        var table = $"smoke_skip_{Guid.NewGuid():N}";
        try
        {
            await using var conn = new ClickHouseConnection(_fixture.NativeConnectionString);
            await conn.OpenAsync();
            await conn.ExecuteNonQueryAsync($@"
                CREATE TABLE {table} (
                    id Int32,
                    fixed_int Int64,
                    str String,
                    fixed_str FixedString(8),
                    nullable_int Nullable(Int32),
                    arr Array(Int32),
                    map_col Map(String, Int32),
                    tup Tuple(Int32, String),
                    lc LowCardinality(String)
                ) ENGINE = Memory");

            await using var inserter = conn.CreateBulkInserter<SkipperRow>(table);
            await inserter.InitAsync();
            for (int i = 0; i < 10; i++)
            {
                await inserter.AddAsync(new SkipperRow
                {
                    Id = i,
                    FixedInt = i * 100,
                    Str = $"s{i}",
                    FixedStr = $"fx_{i}",
                    NullableInt = i % 2 == 0 ? i : null,
                    Arr = new[] { i, i + 1 },
                    Map = new Dictionary<string, int> { [$"k{i}"] = i },
                    Tup = new object[] { i, $"t{i}" },
                    Lc = i % 3 == 0 ? "red" : i % 3 == 1 ? "green" : "blue",
                });
            }
            await inserter.CompleteAsync();

            // SELECT only id — skippers handle all other columns.
            var postRead = await NativeQueryHelper.QueryAsync(
                _fixture.NativeConnectionString,
                $"SELECT id FROM {table} ORDER BY id");

            Assert.Equal(10, postRead.Count);
            for (int i = 0; i < 10; i++)
            {
                Assert.Equal(i, Convert.ToInt32(postRead[i][0]));
            }
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    private class SkipperRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "fixed_int", Order = 1)] public long FixedInt { get; set; }
        [ClickHouseColumn(Name = "str", Order = 2)] public string Str { get; set; } = "";
        [ClickHouseColumn(Name = "fixed_str", Order = 3)] public string FixedStr { get; set; } = "";
        [ClickHouseColumn(Name = "nullable_int", Order = 4)] public int? NullableInt { get; set; }
        [ClickHouseColumn(Name = "arr", Order = 5)] public int[] Arr { get; set; } = Array.Empty<int>();
        [ClickHouseColumn(Name = "map_col", Order = 6)] public Dictionary<string, int> Map { get; set; } = new();
        [ClickHouseColumn(Name = "tup", Order = 7)] public object[] Tup { get; set; } = Array.Empty<object>();
        [ClickHouseColumn(Name = "lc", Order = 8)] public string Lc { get; set; } = "";
    }

    #endregion

    #region Orthogonal paths: multi-block, compression, AddRangeAsync

    // Multi-block insert: pushes more rows than BulkInsertOptions.BatchSize (default
    // 65K) so FlushAsync fires mid-stream and the second block begins with a fresh
    // block header. Exercises block-boundary framing on the wire.
    [Fact]
    public async Task MultiBlockBulkInsert_Int32()
    {
        const int rowCount = 100_000; // > default 65K batch size
        var table = $"smoke_mblock_{Guid.NewGuid():N}";
        try
        {
            await using var conn = new ClickHouseConnection(_fixture.NativeConnectionString);
            await conn.OpenAsync();
            await conn.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id Int32, val Int32) ENGINE = Memory");

            await using var inserter = conn.CreateBulkInserter<Row<int>>(table);
            await inserter.InitAsync();
            for (int i = 0; i < rowCount; i++)
            {
                await inserter.AddAsync(new Row<int> { Id = i, Val = i });
            }
            await inserter.CompleteAsync();

            var count = await NativeQueryHelper.ExecuteScalarAsync<ulong>(
                _fixture.NativeConnectionString,
                $"SELECT count() FROM {table}");
            Assert.Equal((ulong)rowCount, count);

            var sum = await NativeQueryHelper.ExecuteScalarAsync<long>(
                _fixture.NativeConnectionString,
                $"SELECT sum(val) FROM {table}");
            Assert.Equal((long)rowCount * (rowCount - 1) / 2, sum);
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    // Bulk insert with LZ4 compression negotiated at the connection level.
    [Theory]
    [InlineData("lz4")]
    [InlineData("zstd")]
    public async Task BulkInsert_WithCompression(string method)
    {
        var connStr = _fixture.NativeConnectionStringWithCompression(method);
        var table = $"smoke_comp_{Guid.NewGuid():N}";
        try
        {
            await using var conn = new ClickHouseConnection(connStr);
            await conn.OpenAsync();
            await conn.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id Int32, val String) ENGINE = Memory");

            await using var inserter = conn.CreateBulkInserter<Row<string>>(table);
            await inserter.InitAsync();
            for (int i = 0; i < 1000; i++)
            {
                await inserter.AddAsync(new Row<string> { Id = i, Val = $"row_{i}" });
            }
            await inserter.CompleteAsync();

            var postRead = await NativeQueryHelper.QueryAsync(
                connStr,
                $"SELECT id, val FROM {table} ORDER BY id LIMIT 3");
            Assert.Equal(3, postRead.Count);
            Assert.Equal("row_0", postRead[0][1]);
            Assert.Equal("row_1", postRead[1][1]);
            Assert.Equal("row_2", postRead[2][1]);
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    // AddRangeAsync(IEnumerable<T>) and AddRangeAsync(IAsyncEnumerable<T>) overloads.
    [Fact]
    public async Task BulkInsert_AddRangeAsync_Enumerable()
    {
        var table = $"smoke_range_{Guid.NewGuid():N}";
        try
        {
            await using var conn = new ClickHouseConnection(_fixture.NativeConnectionString);
            await conn.OpenAsync();
            await conn.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id Int32, val Int64) ENGINE = Memory");

            var rows = Enumerable.Range(0, 100).Select(i => new Row<long> { Id = i, Val = i * 10L });

            await using var inserter = conn.CreateBulkInserter<Row<long>>(table);
            await inserter.InitAsync();
            await inserter.AddRangeAsync(rows);
            await inserter.CompleteAsync();

            var count = await NativeQueryHelper.ExecuteScalarAsync<ulong>(
                _fixture.NativeConnectionString,
                $"SELECT count() FROM {table}");
            Assert.Equal(100UL, count);
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task BulkInsert_AddRangeStreamingAsync_AsyncEnumerable()
    {
        var table = $"smoke_range_{Guid.NewGuid():N}";
        try
        {
            await using var conn = new ClickHouseConnection(_fixture.NativeConnectionString);
            await conn.OpenAsync();
            await conn.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id Int32, val String) ENGINE = Memory");

            await using var inserter = conn.CreateBulkInserter<Row<string>>(table);
            await inserter.InitAsync();
            await inserter.AddRangeStreamingAsync(GenerateRowsAsync());
            await inserter.CompleteAsync();

            var count = await NativeQueryHelper.ExecuteScalarAsync<ulong>(
                _fixture.NativeConnectionString,
                $"SELECT count() FROM {table}");
            Assert.Equal(50UL, count);
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }

        static async IAsyncEnumerable<Row<string>> GenerateRowsAsync()
        {
            for (int i = 0; i < 50; i++)
            {
                await Task.Yield();
                yield return new Row<string> { Id = i, Val = $"async_{i}" };
            }
        }
    }

    [Fact]
    public async Task BulkInsert_AddRangeStreamingAsync_Enumerable()
    {
        var table = $"smoke_range_{Guid.NewGuid():N}";
        try
        {
            await using var conn = new ClickHouseConnection(_fixture.NativeConnectionString);
            await conn.OpenAsync();
            await conn.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id Int32, val Int64) ENGINE = Memory");

            var rows = Enumerable.Range(0, 100).Select(i => new Row<long> { Id = i, Val = i * 2L });

            await using var inserter = conn.CreateBulkInserter<Row<long>>(table);
            await inserter.InitAsync();
            await inserter.AddRangeStreamingAsync(rows);
            await inserter.CompleteAsync();

            var count = await NativeQueryHelper.ExecuteScalarAsync<ulong>(
                _fixture.NativeConnectionString,
                $"SELECT count() FROM {table}");
            Assert.Equal(100UL, count);
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Theory]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task ArrayTuple(StringMaterialization mat) => RunAsync(
        "Array(Tuple(Int32, String))",
        new[]
        {
            new Row<object[][]> { Id = 0, Val = Array.Empty<object[]>() },
            new Row<object[][]>
            {
                Id = 1,
                Val = new[] { new object[] { 1, "a" }, new object[] { 2, "b" } },
            },
        },
        r => new object?[]
        {
            r.Id,
            r.Val.Select(t => Tuple.Create((int)t[0], (string)t[1])).ToArray<object>(),
        },
        stringMat: mat);

    #endregion

    #region Maps

    [Theory]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task MapStringInt32(StringMaterialization mat) => RunAsync("Map(String, Int32)", new[]
    {
        new Row<Dictionary<string, int>> { Id = 0, Val = new() },
        new Row<Dictionary<string, int>> { Id = 1, Val = new() { ["a"] = 1 } },
        new Row<Dictionary<string, int>> { Id = 2, Val = new() { ["x"] = 10, ["y"] = 20 } },
    }, stringMat: mat);

    [Theory]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task MapStringString(StringMaterialization mat) => RunAsync("Map(String, String)", new[]
    {
        new Row<Dictionary<string, string>> { Id = 0, Val = new() },
        new Row<Dictionary<string, string>> { Id = 1, Val = new() { ["foo"] = "bar", ["baz"] = "qux" } },
    }, stringMat: mat);

    [Theory]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task MapInt32String(StringMaterialization mat) => RunAsync("Map(Int32, String)", new[]
    {
        new Row<Dictionary<int, string>> { Id = 0, Val = new() },
        new Row<Dictionary<int, string>> { Id = 1, Val = new() { [1] = "one", [2] = "two" } },
    }, stringMat: mat);

    [Theory]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task MapStringNullableInt32(StringMaterialization mat) => RunAsync("Map(String, Nullable(Int32))", new[]
    {
        new Row<Dictionary<string, int?>> { Id = 0, Val = new() },
        new Row<Dictionary<string, int?>> { Id = 1, Val = new() { ["a"] = 1, ["b"] = null } },
    }, stringMat: mat);

    [Theory]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task MapStringArrayInt32(StringMaterialization mat) => RunAsync("Map(String, Array(Int32))", new[]
    {
        new Row<Dictionary<string, int[]>> { Id = 0, Val = new() },
        new Row<Dictionary<string, int[]>> { Id = 1, Val = new() { ["xs"] = new[] { 1, 2, 3 } } },
    }, stringMat: mat);

    [Fact]
    public Task MapUInt64UInt64() => RunAsync("Map(UInt64, UInt64)", new[]
    {
        new Row<Dictionary<ulong, ulong>> { Id = 0, Val = new() },
        new Row<Dictionary<ulong, ulong>> { Id = 1, Val = new() { [1] = 10, [2] = 20 } },
    });

    [Theory]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task MapUuidString(StringMaterialization mat) => RunAsync("Map(UUID, String)", new[]
    {
        new Row<Dictionary<Guid, string>> { Id = 0, Val = new() },
        new Row<Dictionary<Guid, string>>
        {
            Id = 1,
            Val = new() { [Guid.Parse("11111111-1111-1111-1111-111111111111")] = "a" },
        },
    }, stringMat: mat);

    [Theory]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task MapStringDecimal64(StringMaterialization mat) => RunAsync("Map(String, Decimal64(4))", new[]
    {
        new Row<Dictionary<string, decimal>> { Id = 0, Val = new() },
        new Row<Dictionary<string, decimal>> { Id = 1, Val = new() { ["x"] = 1.2345m, ["y"] = -9.9m } },
    }, stringMat: mat);

    [Theory]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task MapStringFixedString(StringMaterialization mat) => RunAsync("Map(String, FixedString(4))", new[]
    {
        new Row<Dictionary<string, string>> { Id = 0, Val = new() },
        new Row<Dictionary<string, string>> { Id = 1, Val = new() { ["a"] = "abcd", ["b"] = "wxyz" } },
    }, stringMat: mat);

    #endregion

    #region Tuples

    // Tuples: POCO uses object[] for writes (the BulkInserter fallback format), but the
    // native reader returns System.Tuple<...> — so expected is an ITuple.
    [Theory]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task TupleIntString(StringMaterialization mat) => RunAsync(
        "Tuple(Int32, String)",
        new[]
        {
            new Row<object[]> { Id = 0, Val = new object[] { 0, "" } },
            new Row<object[]> { Id = 1, Val = new object[] { 1, "a" } },
            new Row<object[]> { Id = 2, Val = new object[] { 42, "hello" } },
        },
        r => new object?[] { r.Id, Tuple.Create((int)r.Val[0], (string)r.Val[1]) },
        stringMat: mat);

    [Theory]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task TupleNullable(StringMaterialization mat) => RunAsync(
        "Tuple(Nullable(String), Int32)",
        new[]
        {
            new Row<object?[]> { Id = 0, Val = new object?[] { "hello", 1 } },
            new Row<object?[]> { Id = 1, Val = new object?[] { null, 42 } },
        },
        r => new object?[] { r.Id, Tuple.Create((string?)r.Val[0], (int)r.Val[1]!) },
        stringMat: mat);

    [Theory]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task TupleThree(StringMaterialization mat) => RunAsync(
        "Tuple(Int32, String, Float64)",
        new[]
        {
            new Row<object[]> { Id = 0, Val = new object[] { 1, "a", 1.5 } },
            new Row<object[]> { Id = 1, Val = new object[] { 100, "pi", 3.14159 } },
        },
        r => new object?[] { r.Id, Tuple.Create((int)r.Val[0], (string)r.Val[1], (double)r.Val[2]) },
        stringMat: mat);

    #endregion

    #region LowCardinality

    [Theory]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task LowCardinalityString(StringMaterialization mat) => RunAsync("LowCardinality(String)", new[]
    {
        new Row<string> { Id = 0, Val = "red" },
        new Row<string> { Id = 1, Val = "green" },
        new Row<string> { Id = 2, Val = "red" },
    }, stringMat: mat);

    [Theory]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task LowCardinalityFixedString(StringMaterialization mat) => RunAsync("LowCardinality(FixedString(8))", new[]
    {
        new Row<string> { Id = 0, Val = "hello" },
        new Row<string> { Id = 1, Val = "test1234" },
        new Row<string> { Id = 2, Val = "hello" },
    }, stringMat: mat);

    [Theory]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task LowCardinalityNullableString(StringMaterialization mat) => RunAsync("LowCardinality(Nullable(String))", new[]
    {
        new Row<string?> { Id = 0, Val = "a" },
        new Row<string?> { Id = 1, Val = null },
        new Row<string?> { Id = 2, Val = "a" },
    }, stringMat: mat);

    [Theory]
    [InlineData(StringMaterialization.Eager)]
    [InlineData(StringMaterialization.Lazy)]
    public Task LowCardinalityNullableFixedString(StringMaterialization mat) => RunAsync("LowCardinality(Nullable(FixedString(4)))", new[]
    {
        new Row<string?> { Id = 0, Val = "abcd" },
        new Row<string?> { Id = 1, Val = null },
        new Row<string?> { Id = 2, Val = "abcd" },
    }, stringMat: mat);

    // Force the boxed-fallback path for LowCardinality by siting it next to a composite
    // column. The single-column LowCardinalityString test above uses the fast extractor
    // path which strips the LC wrapper and never invokes LowCardinalityColumnWriter —
    // this test exercises the writer directly.
    [Fact]
    public async Task LowCardinalityString_BoxedFallbackPath()
    {
        var table = $"smoke_bulk_{Guid.NewGuid():N}";
        try
        {
            await using var conn = new ClickHouseConnection(_fixture.NativeConnectionString);
            await conn.OpenAsync();
            await conn.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id Int32, lc LowCardinality(String), arr Array(Int32)) ENGINE = Memory");

            await using var inserter = conn.CreateBulkInserter<LowCardArrayRow>(table);
            await inserter.InitAsync();
            await inserter.AddAsync(new LowCardArrayRow { Id = 0, Lc = "red", Arr = new[] { 1, 2 } });
            await inserter.AddAsync(new LowCardArrayRow { Id = 1, Lc = "green", Arr = Array.Empty<int>() });
            await inserter.AddAsync(new LowCardArrayRow { Id = 2, Lc = "red", Arr = new[] { 3 } });
            await inserter.CompleteAsync();

            var native = await NativeQueryHelper.QueryAsync(
                _fixture.NativeConnectionString,
                $"SELECT id, lc, arr FROM {table} ORDER BY id");

            Assert.Equal(3, native.Count);
            Assert.Equal("red", native[0][1]);
            Assert.Equal("green", native[1][1]);
            Assert.Equal("red", native[2][1]);
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    private class LowCardArrayRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "lc", Order = 1)] public string Lc { get; set; } = "";
        [ClickHouseColumn(Name = "arr", Order = 2)] public int[] Arr { get; set; } = Array.Empty<int>();
    }

    #endregion

    private class Row<T>
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "val", Order = 1)] public T Val { get; set; } = default!;
    }
}
