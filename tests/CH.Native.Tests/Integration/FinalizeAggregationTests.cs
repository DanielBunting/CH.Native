using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// The supported way to consume <c>AggregatingMergeTree</c> aggregates: project the
/// state through <c>finalizeAggregation()</c> server-side and read the resulting value.
/// Because the finalized column comes back as an ordinary typed column, this exercises
/// CH.Native's normal readers — and it works for inner types whose raw <i>state</i>
/// format CH.Native deliberately doesn't decode (String, Decimal, etc.).
/// </summary>
[Collection("ClickHouse")]
public class FinalizeAggregationTests
{
    private readonly ClickHouseFixture _fixture;

    public FinalizeAggregationTests(ClickHouseFixture fixture) => _fixture = fixture;

    /// <summary>
    /// Builds an AggregatingMergeTree MV holding <c>maxState(v)</c> over a single inserted
    /// row, so <c>finalizeAggregation(state)</c> == that row's value. <paramref name="body"/>
    /// reads the finalized column however it likes.
    /// </summary>
    private async Task WithMaxStateAsync(string innerType, string literal, Func<ClickHouseConnection, string, Task> body)
    {
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();
        var src = $"fin_src_{Guid.NewGuid():N}";
        var mv = $"fin_mv_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync($"CREATE TABLE {src} (id Int32, v {innerType}) ENGINE=MergeTree ORDER BY id");
        await conn.ExecuteNonQueryAsync(
            $"CREATE MATERIALIZED VIEW {mv} ENGINE=AggregatingMergeTree ORDER BY id AS " +
            $"SELECT id, maxState(v) AS s FROM {src} GROUP BY id");
        try
        {
            await conn.ExecuteNonQueryAsync($"INSERT INTO {src} VALUES (1, {literal})");
            await body(conn, mv);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {mv}");
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {src}");
        }
    }

    // Broad type sweep. Reads finalizeAggregation as a server-rendered toString() and
    // compares to an anchored value — type-agnostic, so one theory covers the whole
    // scalar surface. Proves the finalize + read path works for each inner type.
    [Theory]
    [InlineData("Int8", "toInt8(-5)", "-5")]
    [InlineData("Int16", "toInt16(-1000)", "-1000")]
    [InlineData("Int32", "toInt32(123456)", "123456")]
    [InlineData("Int64", "toInt64(-9223372036854775808)", "-9223372036854775808")]
    [InlineData("UInt8", "toUInt8(255)", "255")]
    [InlineData("UInt16", "toUInt16(65535)", "65535")]
    [InlineData("UInt32", "toUInt32(4294967295)", "4294967295")]
    [InlineData("UInt64", "toUInt64(18446744073709551615)", "18446744073709551615")]
    [InlineData("Int128", "toInt128('170141183460469231731687303715884105727')", "170141183460469231731687303715884105727")]
    [InlineData("Int256", "toInt256('-5')", "-5")]
    [InlineData("UInt256", "toUInt256('5')", "5")]
    [InlineData("Float32", "toFloat32(1.5)", "1.5")]
    [InlineData("Float64", "toFloat64(2.5)", "2.5")]
    [InlineData("Decimal32(4)", "toDecimal32('1.2345', 4)", "1.2345")]
    [InlineData("Decimal64(4)", "toDecimal64('1.2345', 4)", "1.2345")]
    [InlineData("Decimal128(4)", "toDecimal128('1.2345', 4)", "1.2345")]
    [InlineData("Decimal256(4)", "toDecimal256('1.2345', 4)", "1.2345")]
    [InlineData("Date", "toDate('2024-01-01')", "2024-01-01")]
    [InlineData("Date32", "toDate32('2024-01-01')", "2024-01-01")]
    [InlineData("DateTime('UTC')", "toDateTime('2024-01-01 12:00:00', 'UTC')", "2024-01-01 12:00:00")]
    [InlineData("UUID", "toUUID('00000000-0000-0000-0000-000000000abc')", "00000000-0000-0000-0000-000000000abc")]
    [InlineData("String", "'hello world'", "hello world")]
    [InlineData("Bool", "true", "true")]
    [InlineData("IPv4", "toIPv4('1.2.3.4')", "1.2.3.4")]
    [InlineData("IPv6", "toIPv6('2001:db8::1')", "2001:db8::1")]
    public Task Finalize_AcrossInnerTypes_RendersAnchoredValue(string innerType, string literal, string expected) =>
        WithMaxStateAsync(innerType, literal, async (conn, mv) =>
        {
            var actual = await conn.ExecuteScalarAsync<string>(
                $"SELECT toString(finalizeAggregation(s)) FROM {mv} ORDER BY id LIMIT 1");
            Assert.Equal(expected, actual);
        });

    // Typed reads — confirm the finalized value comes back as the right CLR type, not
    // just a String. min/max preserve the inner type, so these read it directly.
    [Fact]
    public Task Finalize_Max_Int32_ReadsAsInt() =>
        WithMaxStateAsync("Int32", "toInt32(42)", async (conn, mv) =>
            Assert.Equal(42, await conn.ExecuteScalarAsync<int>($"SELECT finalizeAggregation(s) FROM {mv} LIMIT 1")));

    [Fact]
    public Task Finalize_Max_Int64_ReadsAsLong() =>
        WithMaxStateAsync("Int64", "toInt64(9223372036854775807)", async (conn, mv) =>
            Assert.Equal(long.MaxValue, await conn.ExecuteScalarAsync<long>($"SELECT finalizeAggregation(s) FROM {mv} LIMIT 1")));

    [Fact]
    public Task Finalize_Max_Float64_ReadsAsDouble() =>
        WithMaxStateAsync("Float64", "toFloat64(3.25)", async (conn, mv) =>
            Assert.Equal(3.25, await conn.ExecuteScalarAsync<double>($"SELECT finalizeAggregation(s) FROM {mv} LIMIT 1")));

    [Fact]
    public Task Finalize_Max_DateTime_ReadsAsDateTimeOffset() =>
        // A timezone-aware DateTime('UTC') surfaces as DateTimeOffset in CH.Native.
        WithMaxStateAsync("DateTime('UTC')", "toDateTime('2024-06-01 08:30:00', 'UTC')", async (conn, mv) =>
            Assert.Equal(
                new DateTimeOffset(2024, 6, 1, 8, 30, 0, TimeSpan.Zero),
                await conn.ExecuteScalarAsync<DateTimeOffset>($"SELECT finalizeAggregation(s) FROM {mv} LIMIT 1")));

    [Fact]
    public Task Finalize_Max_Uuid_ReadsAsGuid() =>
        WithMaxStateAsync("UUID", "toUUID('00000000-0000-0000-0000-000000000abc')", async (conn, mv) =>
            Assert.Equal(
                Guid.Parse("00000000-0000-0000-0000-000000000abc"),
                await conn.ExecuteScalarAsync<Guid>($"SELECT finalizeAggregation(s) FROM {mv} LIMIT 1")));

    // Functions whose finalized type differs from the inner type.
    [Fact]
    public async Task Finalize_DifferentReturnTypes_CountSumAvgUniq()
    {
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();
        var src = $"fin_multi_src_{Guid.NewGuid():N}";
        var mv = $"fin_multi_mv_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync($"CREATE TABLE {src} (id Int32, v Int32) ENGINE=MergeTree ORDER BY id");
        await conn.ExecuteNonQueryAsync(
            $"CREATE MATERIALIZED VIEW {mv} ENGINE=AggregatingMergeTree ORDER BY id AS " +
            $"SELECT id, countState() AS c, sumState(v) AS s, avgState(v) AS a, " +
            $"uniqExactState(v) AS u, groupArrayState(v) AS g FROM {src} GROUP BY id");
        try
        {
            await conn.ExecuteNonQueryAsync($"INSERT INTO {src} VALUES (1, 10), (1, 20), (1, 10), (1, 40)");

            // count() -> UInt64
            Assert.Equal(4UL, await conn.ExecuteScalarAsync<ulong>(
                $"SELECT toUInt64(finalizeAggregation(c)) FROM {mv} WHERE id = 1"));
            // sum(Int32) -> Int64 in ClickHouse
            Assert.Equal(80L, await conn.ExecuteScalarAsync<long>(
                $"SELECT toInt64(finalizeAggregation(s)) FROM {mv} WHERE id = 1"));
            // avg -> Float64
            Assert.Equal(20.0, await conn.ExecuteScalarAsync<double>(
                $"SELECT finalizeAggregation(a) FROM {mv} WHERE id = 1"));
            // uniqExact -> UInt64 (distinct {10,20,40} = 3)
            Assert.Equal(3UL, await conn.ExecuteScalarAsync<ulong>(
                $"SELECT toUInt64(finalizeAggregation(u)) FROM {mv} WHERE id = 1"));
            // groupArray -> Array(Int32)
            var arr = await conn.ExecuteScalarAsync<int[]>(
                $"SELECT finalizeAggregation(g) FROM {mv} WHERE id = 1");
            Assert.Equal(new[] { 10, 20, 10, 40 }, arr);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {mv}");
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {src}");
        }
    }
}
