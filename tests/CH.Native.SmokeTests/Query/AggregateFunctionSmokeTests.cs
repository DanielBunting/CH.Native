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
/// CH.Native surfaces <c>ClickHouseAggregateState</c> (function name + opaque
/// bytes), the reference driver doesn't have a comparable wrapper, and the
/// <see cref="ResultComparer"/> can't meaningfully equate them. The state-bytes
/// path is covered by the unit and integration tests instead.
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
}
