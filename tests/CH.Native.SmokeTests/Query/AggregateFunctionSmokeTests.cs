using CH.Native.SmokeTests.Fixtures;
using CH.Native.SmokeTests.Helpers;
using Xunit;

namespace CH.Native.SmokeTests.Query;

/// <summary>
/// Cross-driver parity smoke tests for the aggregate-function feature.
///
/// Two paths exercised:
/// <list type="bullet">
/// <item><c>SimpleAggregateFunction(fn, T)</c> — transparent wire-format
/// pass-through for <c>T</c>. Both CH.Native and the reference ADO driver should
/// surface the inner CLR value; parity falls out of inner-type parity.</item>
/// <item>Server-side <c>finalizeAggregation(state)</c> — the documented workaround
/// for opaque <c>AggregateFunction</c> states. Both drivers see a plain scalar
/// after the server finalizes, so parity is straightforward and acts as a
/// regression guard if either driver ever changes its handling of the finalized
/// type.</item>
/// </list>
///
/// Direct <c>AggregateFunction(...)</c> column parity is intentionally skipped:
/// CH.Native does not read raw aggregate-state columns (they are opaque,
/// server-internal blobs), so there is nothing to compare against the reference
/// driver. The supported query path — <c>finalizeAggregation()</c> — is covered
/// here; raw-state rejection is covered by the integration tests.
/// </summary>
[Collection("SmokeTest")]
public class AggregateFunctionSmokeTests
{
    private readonly SmokeTestFixture _fixture;

    public AggregateFunctionSmokeTests(SmokeTestFixture fixture) => _fixture = fixture;

    // --- SimpleAggregateFunction transparent-pass-through parity ------------

    [Fact]
    public Task SimpleAggregateFunction_Sum_Int64() =>
        RunSimpleAggregateTest(
            columnDef: "id Int32, total SimpleAggregateFunction(sum, Int64)",
            insertValues: "(1, 100),(1, 200),(2, 5)",
            selectExpr: "id, total",
            orderBy: "id");

    [Fact]
    public Task SimpleAggregateFunction_Max_Int32() =>
        RunSimpleAggregateTest(
            columnDef: "id Int32, m SimpleAggregateFunction(max, Int32)",
            insertValues: "(1, 7),(1, 12),(1, 4),(2, 99)",
            selectExpr: "id, m",
            orderBy: "id");

    [Fact]
    public Task SimpleAggregateFunction_Min_String() =>
        RunSimpleAggregateTest(
            columnDef: "id Int32, smallest SimpleAggregateFunction(min, String)",
            insertValues: "(1, 'banana'),(1, 'apple'),(1, 'cherry'),(2, 'zebra')",
            selectExpr: "id, smallest",
            orderBy: "id");

    // --- finalizeAggregation() server-side workaround parity ----------------

    [Fact]
    public Task FinalizeAggregation_SumState_Int32_ReturnsScalar() =>
        RunFinalizeAggregationTest(
            stateColumnType: "AggregateFunction(sum, Int32)",
            stateExpr: "sumState(toInt32(number))",
            finalizeWrapper: "toInt64(finalizeAggregation(s))",
            rowCount: 100);

    [Fact]
    public Task FinalizeAggregation_CountState_NoArgs_ReturnsScalar() =>
        RunFinalizeAggregationTest(
            stateColumnType: "AggregateFunction(count)",
            stateExpr: "countState()",
            finalizeWrapper: "toInt64(finalizeAggregation(s))",
            rowCount: 100);

    // --- Helpers ------------------------------------------------------------

    /// <summary>
    /// Round-trips a SimpleAggregateFunction column through both drivers. The
    /// AggregatingMergeTree engine merges values on FINAL, exercising the
    /// transparent wire-format pass-through end-to-end.
    /// </summary>
    private async Task RunSimpleAggregateTest(
        string columnDef, string insertValues, string selectExpr, string orderBy)
    {
        var table = $"smoke_simpleagg_{Guid.NewGuid():N}";
        try
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"CREATE TABLE {table} ({columnDef}) ENGINE = AggregatingMergeTree ORDER BY id");

            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"INSERT INTO {table} VALUES {insertValues}");
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"OPTIMIZE TABLE {table} FINAL");

            var sql = $"SELECT {selectExpr} FROM {table} ORDER BY {orderBy}";

            var native = await NativeQueryHelper.QueryStreamAsync(_fixture.NativeConnectionString, sql);
            var driver = await DriverQueryHelper.QueryStreamAsync(_fixture.DriverConnectionString, sql);

            ResultComparer.AssertResultsEqual(native, driver, $"SimpleAggregate: {columnDef}");
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    /// <summary>
    /// Stores opaque <c>AggregateFunction</c> states via a Memory table seeded by
    /// <c>SELECT *State(...) FROM numbers(N)</c>, then reads through
    /// <c>finalizeAggregation</c> on both drivers. Tests the recommended
    /// workaround pattern for aggregates outside CH.Native's tier-1 set.
    /// </summary>
    private async Task RunFinalizeAggregationTest(
        string stateColumnType, string stateExpr, string finalizeWrapper, int rowCount)
    {
        var table = $"smoke_finalize_{Guid.NewGuid():N}";
        try
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"CREATE TABLE {table} (s {stateColumnType}) ENGINE = Memory");

            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"INSERT INTO {table} SELECT {stateExpr} FROM numbers({rowCount})");

            var sql = $"SELECT {finalizeWrapper} FROM {table}";

            var native = await NativeQueryHelper.QueryStreamAsync(_fixture.NativeConnectionString, sql);
            var driver = await DriverQueryHelper.QueryStreamAsync(_fixture.DriverConnectionString, sql);

            ResultComparer.AssertResultsEqual(native, driver, $"finalizeAggregation: {stateColumnType}");
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    // --- finalizeAggregation cross-driver parity across inner types ----------
    // Builds maxState(v) over a single value (so finalize == that value) for a broad
    // inner-type matrix, then reads the finalized column with BOTH clients and asserts
    // they agree. ResultComparer normalises the documented cross-client representation
    // differences (DateTimeOffset↔DateTime, Int128/256↔BigInteger, ClickHouseDecimal
    // variants, IPAddress, Guid). This is the supported AggregatingMergeTree read path.

    [Theory]
    [InlineData("Int8", "toInt8(-5)")]
    [InlineData("Int16", "toInt16(-1000)")]
    [InlineData("Int32", "toInt32(123456)")]
    [InlineData("Int64", "toInt64(-9223372036854775808)")]
    [InlineData("UInt8", "toUInt8(255)")]
    [InlineData("UInt16", "toUInt16(65535)")]
    [InlineData("UInt32", "toUInt32(4294967295)")]
    [InlineData("UInt64", "toUInt64(18446744073709551615)")]
    [InlineData("Int128", "toInt128('170141183460469231731687303715884105727')")]
    [InlineData("Int256", "toInt256('-5')")]
    [InlineData("UInt256", "toUInt256('5')")]
    [InlineData("Float32", "toFloat32(1.5)")]
    [InlineData("Float64", "toFloat64(2.5)")]
    [InlineData("Decimal32(4)", "toDecimal32('1.2345', 4)")]
    [InlineData("Decimal64(4)", "toDecimal64('1.2345', 4)")]
    [InlineData("Decimal128(4)", "toDecimal128('1.2345', 4)")]
    [InlineData("Decimal256(4)", "toDecimal256('1.2345', 4)")]
    [InlineData("Date", "toDate('2024-01-01')")]
    [InlineData("Date32", "toDate32('2024-01-01')")]
    [InlineData("DateTime('UTC')", "toDateTime('2024-01-01 12:00:00', 'UTC')")]
    [InlineData("UUID", "toUUID('00000000-0000-0000-0000-000000000abc')")]
    [InlineData("String", "'hello world'")]
    [InlineData("IPv4", "toIPv4('1.2.3.4')")]
    [InlineData("IPv6", "toIPv6('2001:db8::1')")]
    public Task FinalizeAggregation_MaxState_AcrossTypes_CrossDriverParity(string innerType, string literal) =>
        RunFinalizeMaxParityTest(innerType, literal);

    private async Task RunFinalizeMaxParityTest(string innerType, string literal)
    {
        var src = $"smoke_finmax_{Guid.NewGuid():N}";
        var mv = $"smoke_finmax_mv_{Guid.NewGuid():N}";
        try
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"CREATE TABLE {src} (id Int32, v {innerType}) ENGINE = MergeTree ORDER BY id");
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"CREATE MATERIALIZED VIEW {mv} ENGINE = AggregatingMergeTree ORDER BY id AS " +
                $"SELECT id, maxState(v) AS s FROM {src} GROUP BY id");
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"INSERT INTO {src} VALUES (1, {literal})");

            var sql = $"SELECT finalizeAggregation(s) FROM {mv} ORDER BY id";
            var native = await NativeQueryHelper.QueryStreamAsync(_fixture.NativeConnectionString, sql);
            var driver = await DriverQueryHelper.QueryStreamAsync(_fixture.DriverConnectionString, sql);

            ResultComparer.AssertResultsEqual(native, driver, $"finalize max({innerType})");
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString, $"DROP TABLE IF EXISTS {mv}");
            await NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString, $"DROP TABLE IF EXISTS {src}");
        }
    }

    // Functions whose finalized type differs from the inner type, compared cross-driver.
    [Theory]
    [InlineData("countState()", "toInt64(finalizeAggregation(s))")]
    [InlineData("sumState(toInt32(number))", "toInt64(finalizeAggregation(s))")]
    [InlineData("avgState(toInt32(number))", "finalizeAggregation(s)")]
    [InlineData("uniqExactState(toUInt64(number))", "toUInt64(finalizeAggregation(s))")]
    [InlineData("groupArrayState(toInt32(number))", "finalizeAggregation(s)")]
    public async Task FinalizeAggregation_ReturnTypeVariety_CrossDriverParity(string stateExpr, string finalizeWrapper)
    {
        var src = $"smoke_finret_{Guid.NewGuid():N}";
        var mv = $"smoke_finret_mv_{Guid.NewGuid():N}";
        try
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"CREATE TABLE {src} (number UInt64) ENGINE = MergeTree ORDER BY number");
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"CREATE MATERIALIZED VIEW {mv} ENGINE = AggregatingMergeTree ORDER BY tuple() AS " +
                $"SELECT {stateExpr} AS s FROM {src}");
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"INSERT INTO {src} SELECT number FROM numbers(50)");

            var sql = $"SELECT {finalizeWrapper} FROM {mv}";
            var native = await NativeQueryHelper.QueryStreamAsync(_fixture.NativeConnectionString, sql);
            var driver = await DriverQueryHelper.QueryStreamAsync(_fixture.DriverConnectionString, sql);

            ResultComparer.AssertResultsEqual(native, driver, $"finalize {stateExpr}");
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString, $"DROP TABLE IF EXISTS {mv}");
            await NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString, $"DROP TABLE IF EXISTS {src}");
        }
    }
}
