using Apache.Arrow;
using Apache.Arrow.Types;
using Xunit;

namespace CH.Native.Adbc.Tests.Integration;

/// <summary>
/// End-to-end round-trip tests for the scalar type matrix: create a table of a given ClickHouse
/// type, insert known values, SELECT through the ADBC driver, and assert both the Arrow array type
/// and the decoded values. Complements the in-memory converter unit tests with real server output.
/// </summary>
[Trait("Category", "Integration")]
[Collection("AdbcClickHouse")]
public class ScalarTypeRoundTripTests : AdbcIntegrationTestBase
{
    public ScalarTypeRoundTripTests(AdbcClickHouseFixture fixture) : base(fixture) { }

    private static string Table() => "adbc_rt_" + Guid.NewGuid().ToString("N");

    [Fact]
    public async Task SignedIntegers_RoundTrip()
    {
        var t = Table();
        await ExecuteSetupAsync(
            $"CREATE TABLE {t} (a Int8, b Int16, c Int32, d Int64) ENGINE = Memory",
            $"INSERT INTO {t} VALUES (-8, -16, -32, -64)");

        using var batch = await QueryOneBatchAsync($"SELECT a, b, c, d FROM {t}");
        Assert.Equal((sbyte)-8, Assert.IsType<Int8Array>(batch.Column(0)).GetValue(0));
        Assert.Equal((short)-16, Assert.IsType<Int16Array>(batch.Column(1)).GetValue(0));
        Assert.Equal(-32, Assert.IsType<Int32Array>(batch.Column(2)).GetValue(0));
        Assert.Equal(-64L, Assert.IsType<Int64Array>(batch.Column(3)).GetValue(0));
    }

    [Fact]
    public async Task UnsignedIntegers_RoundTrip()
    {
        var t = Table();
        await ExecuteSetupAsync(
            $"CREATE TABLE {t} (a UInt8, b UInt16, c UInt32, d UInt64) ENGINE = Memory",
            $"INSERT INTO {t} VALUES (200, 40000, 3000000000, 18446744073709551615)");

        using var batch = await QueryOneBatchAsync($"SELECT a, b, c, d FROM {t}");
        Assert.Equal((byte)200, Assert.IsType<UInt8Array>(batch.Column(0)).GetValue(0));
        Assert.Equal((ushort)40000, Assert.IsType<UInt16Array>(batch.Column(1)).GetValue(0));
        Assert.Equal(3000000000U, Assert.IsType<UInt32Array>(batch.Column(2)).GetValue(0));
        Assert.Equal(18446744073709551615UL, Assert.IsType<UInt64Array>(batch.Column(3)).GetValue(0));
    }

    [Fact]
    public async Task FloatingPoint_RoundTrip()
    {
        var t = Table();
        await ExecuteSetupAsync(
            $"CREATE TABLE {t} (a Float32, b Float64) ENGINE = Memory",
            $"INSERT INTO {t} VALUES (1.5, -2.25)");

        using var batch = await QueryOneBatchAsync($"SELECT a, b FROM {t}");
        Assert.Equal(1.5f, Assert.IsType<FloatArray>(batch.Column(0)).GetValue(0));
        Assert.Equal(-2.25d, Assert.IsType<DoubleArray>(batch.Column(1)).GetValue(0));
    }

    [Fact]
    public async Task Bool_RoundTrip()
    {
        var t = Table();
        await ExecuteSetupAsync(
            $"CREATE TABLE {t} (a Bool, b Bool) ENGINE = Memory",
            $"INSERT INTO {t} VALUES (true, false)");

        using var batch = await QueryOneBatchAsync($"SELECT a, b FROM {t}");
        Assert.True(Assert.IsType<BooleanArray>(batch.Column(0)).GetValue(0));
        Assert.False(Assert.IsType<BooleanArray>(batch.Column(1)).GetValue(0));
    }

    [Fact]
    public async Task StringAndFixedString_RoundTrip()
    {
        var t = Table();
        await ExecuteSetupAsync(
            $"CREATE TABLE {t} (s String, fs FixedString(4)) ENGINE = Memory",
            $"INSERT INTO {t} VALUES ('hello', 'ab')");

        using var batch = await QueryOneBatchAsync($"SELECT s, fs FROM {t}");
        Assert.Equal("hello", Assert.IsType<StringArray>(batch.Column(0)).GetString(0));

        var fixedBytes = Assert.IsType<BinaryArray>(batch.Column(1)).GetBytes(0).ToArray();
        Assert.Equal(new byte[] { (byte)'a', (byte)'b', 0, 0 }, fixedBytes);
    }

    [Fact]
    public async Task UuidAndIpAddresses_RoundTrip()
    {
        var t = Table();
        await ExecuteSetupAsync(
            $"CREATE TABLE {t} (u UUID, v4 IPv4, v6 IPv6) ENGINE = Memory",
            $"INSERT INTO {t} VALUES ('11111111-2222-3333-4444-555555555555', '192.168.0.1', '2001:db8::1')");

        using var batch = await QueryOneBatchAsync($"SELECT u, v4, v6 FROM {t}");
        Assert.Equal("11111111-2222-3333-4444-555555555555", Assert.IsType<StringArray>(batch.Column(0)).GetString(0));
        Assert.Equal("192.168.0.1", Assert.IsType<StringArray>(batch.Column(1)).GetString(0));
        Assert.Equal("2001:db8::1", Assert.IsType<StringArray>(batch.Column(2)).GetString(0));
    }

    [Fact]
    public async Task DateTypes_RoundTrip()
    {
        var t = Table();
        await ExecuteSetupAsync(
            $"CREATE TABLE {t} (d Date, d32 Date32) ENGINE = Memory",
            $"INSERT INTO {t} VALUES ('2026-06-29', '2026-06-29')");

        using var batch = await QueryOneBatchAsync($"SELECT d, d32 FROM {t}");
        var expected = new DateOnly(2026, 6, 29);
        Assert.Equal(expected, Assert.IsType<Date32Array>(batch.Column(0)).GetDateOnly(0));
        Assert.Equal(expected, Assert.IsType<Date32Array>(batch.Column(1)).GetDateOnly(0));
    }

    [Fact]
    public async Task DateTimeTypes_RoundTrip()
    {
        var t = Table();
        await ExecuteSetupAsync(
            $"CREATE TABLE {t} (dt DateTime('UTC'), dt64 DateTime64(3, 'UTC'), dtn DateTime) ENGINE = Memory",
            $"INSERT INTO {t} VALUES ('2026-06-29 12:00:00', '2026-06-29 12:00:00.123', '2026-06-29 12:00:00')");

        using var batch = await QueryOneBatchAsync($"SELECT dt, dt64, dtn FROM {t}");

        var dt = Assert.IsType<TimestampArray>(batch.Column(0));
        Assert.Equal(TimeUnit.Second, ((TimestampType)dt.Data.DataType).Unit);
        Assert.Equal(new DateTimeOffset(2026, 6, 29, 12, 0, 0, TimeSpan.Zero), dt.GetTimestamp(0));

        var dt64 = Assert.IsType<TimestampArray>(batch.Column(1));
        Assert.Equal(TimeUnit.Millisecond, ((TimestampType)dt64.Data.DataType).Unit);
        Assert.Equal(new DateTimeOffset(2026, 6, 29, 12, 0, 0, 123, TimeSpan.Zero), dt64.GetTimestamp(0));

        // Naive DateTime: the exact instant depends on the server timezone, so assert only the
        // Arrow type/unit and that a value was produced.
        var dtn = Assert.IsType<TimestampArray>(batch.Column(2));
        Assert.Equal(TimeUnit.Second, ((TimestampType)dtn.Data.DataType).Unit);
        Assert.NotNull(dtn.GetTimestamp(0));
    }

    [Fact]
    public async Task Decimals_RoundTrip()
    {
        var t = Table();
        await ExecuteSetupAsync(
            $"CREATE TABLE {t} (a Decimal32(2), b Decimal64(4), c Decimal128(6), d Decimal256(8)) ENGINE = Memory",
            $"INSERT INTO {t} VALUES (12.34, 12.3456, 12.345678, 12.34567891)");

        using var batch = await QueryOneBatchAsync($"SELECT a, b, c, d FROM {t}");

        var a = Assert.IsType<Decimal128Array>(batch.Column(0));
        Assert.Equal(2, ((Decimal128Type)a.Data.DataType).Scale);
        Assert.Equal(12.34m, a.GetValue(0));

        Assert.Equal(12.3456m, Assert.IsType<Decimal128Array>(batch.Column(1)).GetValue(0));
        Assert.Equal(12.345678m, Assert.IsType<Decimal128Array>(batch.Column(2)).GetValue(0));

        var d = Assert.IsType<Decimal256Array>(batch.Column(3));
        Assert.Equal(76, ((Decimal256Type)d.Data.DataType).Precision);
        Assert.Equal(12.34567891m, d.GetValue(0));
    }

    [Fact]
    public async Task Enums_RoundTrip()
    {
        var t = Table();
        await ExecuteSetupAsync(
            $"CREATE TABLE {t} (e8 Enum8('a' = -1, 'b' = 2), e16 Enum16('x' = -1000, 'y' = 1000)) ENGINE = Memory",
            $"INSERT INTO {t} VALUES ('a', 'x')");

        using var batch = await QueryOneBatchAsync($"SELECT e8, e16 FROM {t}");
        // The scalar tier surfaces enums as their underlying integer value.
        Assert.Equal((sbyte)-1, Assert.IsType<Int8Array>(batch.Column(0)).GetValue(0));
        Assert.Equal((short)-1000, Assert.IsType<Int16Array>(batch.Column(1)).GetValue(0));
    }

    [Fact]
    public async Task Nullable_RoundTrip()
    {
        var t = Table();
        await ExecuteSetupAsync(
            $"CREATE TABLE {t} (a Nullable(Int32), b Nullable(String)) ENGINE = Memory",
            $"INSERT INTO {t} VALUES (1, 'x'), (NULL, NULL)");

        using var batch = await QueryOneBatchAsync($"SELECT a, b FROM {t} ORDER BY a NULLS LAST");
        Assert.Equal(2, batch.Length);

        var ints = Assert.IsType<Int32Array>(batch.Column(0));
        Assert.Equal(1, ints.GetValue(0));
        Assert.True(ints.IsNull(1));

        var strings = Assert.IsType<StringArray>(batch.Column(1));
        Assert.Equal("x", strings.GetString(0));
        Assert.True(strings.IsNull(1));

        Assert.True(batch.Schema.GetFieldByIndex(0).IsNullable);
        Assert.True(batch.Schema.GetFieldByIndex(1).IsNullable);
    }

    [Fact]
    public async Task LowCardinality_RoundTrip()
    {
        var t = Table();
        await ExecuteSetupAsync(
            $"CREATE TABLE {t} (s LowCardinality(String), n LowCardinality(Nullable(String))) ENGINE = Memory",
            $"INSERT INTO {t} VALUES ('p', 'x'), ('q', NULL)");

        using var batch = await QueryOneBatchAsync($"SELECT s, n FROM {t} ORDER BY s");

        var s = Assert.IsType<StringArray>(batch.Column(0));
        Assert.Equal("p", s.GetString(0));
        Assert.Equal("q", s.GetString(1));

        var n = Assert.IsType<StringArray>(batch.Column(1));
        Assert.Equal("x", n.GetString(0));
        Assert.True(n.IsNull(1));
        Assert.True(batch.Schema.GetFieldByIndex(1).IsNullable);
    }

    [Fact]
    public async Task EmptyResultSet_ProducesNoBatch()
    {
        await AssertNoRowsAsync("SELECT toInt32(1) AS x WHERE 0");
    }
}
