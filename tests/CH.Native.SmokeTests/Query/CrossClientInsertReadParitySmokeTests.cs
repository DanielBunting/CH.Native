using System.Net;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SmokeTests.Fixtures;
using CH.Native.SmokeTests.Helpers;
using Xunit;
using DriverConnection = ClickHouse.Driver.ADO.ClickHouseConnection;

// ClickHouseBulkCopy is the driver's stable bulk surface today (obsolete pending move).
#pragma warning disable CS0618

namespace CH.Native.SmokeTests.Query;

// The full insert × read matrix: each type is written three ways into one table —
// clickhouse-client (id 0), CH.Native bulk insert (id 1), ClickHouse.Driver bulk copy
// (id 2) — then read back through BOTH .NET clients and asserted against anchored
// expected values. Because all three rows assert against the SAME anchor per client,
// a pass proves the value is insert-source-independent: it does not matter which
// client wrote it. Per-client expectations differ only where the clients' CLR
// representations genuinely differ (Enum names vs numbers, DateOnly vs DateTime, …) —
// those splits are themselves pinned in ClientBehaviorComparisonTests.
[Collection("SmokeTest")]
public class CrossClientInsertReadParitySmokeTests
{
    private readonly SmokeTestFixture _fixture;

    public CrossClientInsertReadParitySmokeTests(SmokeTestFixture fixture)
    {
        _fixture = fixture;
    }

    /// <param name="expected">Anchored value all native-read rows must equal (via ResultComparer).</param>
    /// <param name="driverAssert">Per-row driver assertion when the driver's representation
    /// differs; null means ResultComparer against <paramref name="expected"/>.</param>
    private async Task RunParity<T>(
        string columnType,
        string cliLiteral,
        T nativeBulkValue,
        object? driverBulkValue,
        object? expected,
        Action<object?>? driverAssert = null)
    {
        var table = $"smoke_parity_{Guid.NewGuid():N}";
        try
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString,
                $"CREATE TABLE {table} (id Int32, val {columnType}) ENGINE = Memory");

            // Writer 1: clickhouse-client
            await CliQueryHelper.ExecuteNonQueryAsync(_fixture,
                $"INSERT INTO {table} VALUES (0, {cliLiteral})");

            // Writer 2: CH.Native bulk inserter
            await using (var conn = new ClickHouseConnection(_fixture.NativeConnectionString))
            {
                await conn.OpenAsync();
                await using var ins = conn.CreateBulkInserter<Row<T>>(table);
                await ins.InitAsync();
                await ins.AddAsync(new Row<T> { Id = 1, Val = nativeBulkValue });
                await ins.CompleteAsync();
            }

            // Writer 3: ClickHouse.Driver bulk copy
            using (var conn = new DriverConnection(_fixture.DriverConnectionString))
            {
                await conn.OpenAsync();
                using var bulk = new ClickHouse.Driver.Copy.ClickHouseBulkCopy(conn)
                {
                    DestinationTableName = table,
                    ColumnNames = new[] { "id", "val" },
                };
                await bulk.WriteToServerAsync(new List<object?[]> { new object?[] { 2, driverBulkValue } });
            }

            // Reader 1: CH.Native — every row equals the anchor regardless of writer.
            var nativeRows = await NativeQueryHelper.QueryStreamAsync(
                _fixture.NativeConnectionString, $"SELECT val FROM {table} ORDER BY id");
            Assert.Equal(3, nativeRows.Count);
            ResultComparer.AssertResultsEqual(
                nativeRows,
                Enumerable.Repeat(new[] { expected }, 3).ToList(),
                $"native read parity: {columnType}");

            // Reader 2: ClickHouse.Driver — same anchor (or the driver's representation).
            var driverRows = await DriverQueryHelper.QueryStreamAsync(
                _fixture.DriverConnectionString, $"SELECT val FROM {table} ORDER BY id");
            Assert.Equal(3, driverRows.Count);
            if (driverAssert is null)
            {
                ResultComparer.AssertResultsEqual(
                    driverRows,
                    Enumerable.Repeat(new[] { expected }, 3).ToList(),
                    $"driver read parity: {columnType}");
            }
            else
            {
                Assert.All(driverRows, r => driverAssert(r[0]));
            }
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public Task Int64_Min() => RunParity(
        "Int64", "-9223372036854775808", long.MinValue, long.MinValue, long.MinValue);

    [Fact]
    public Task UInt64_AboveLongMax() => RunParity(
        "UInt64", "18446744073709551615", ulong.MaxValue, ulong.MaxValue, ulong.MaxValue);

    [Fact]
    public Task Int128_Max() => RunParity(
        "Int128", "170141183460469231731687303715884105727",
        Int128.MaxValue,
        System.Numerics.BigInteger.Parse("170141183460469231731687303715884105727"),
        Int128.MaxValue);

    [Fact]
    public Task Float64_Pi() => RunParity(
        "Float64", "3.14159265358979", 3.14159265358979, 3.14159265358979, 3.14159265358979);

    [Fact]
    public Task Float64_NaN() => RunParity(
        "Float64", "nan", double.NaN, double.NaN, double.NaN);

    // Driver reads decimals as its own ClickHouse.Driver.Numerics.ClickHouseDecimal.
    [Fact]
    public Task Decimal64() => RunParity(
        "Decimal64(4)", "12.3456", 12.3456m, 12.3456m, 12.3456m,
        driverAssert: v => Assert.Equal(12.3456m, Convert.ToDecimal(v)));

    [Fact]
    public Task String_Utf8() => RunParity(
        "String", "'тест 🦀'", "тест 🦀", "тест 🦀", "тест 🦀");

    [Fact]
    public Task Bool_() => RunParity(
        "Bool", "true", true, true, true);

    [Fact]
    public Task Uuid_() => RunParity(
        "UUID", "'550e8400-e29b-41d4-a716-446655440000'",
        Guid.Parse("550e8400-e29b-41d4-a716-446655440000"),
        Guid.Parse("550e8400-e29b-41d4-a716-446655440000"),
        Guid.Parse("550e8400-e29b-41d4-a716-446655440000"));

    [Fact]
    public Task IPv6_() => RunParity(
        "IPv6", "'2001:db8::1'",
        IPAddress.Parse("2001:db8::1"), IPAddress.Parse("2001:db8::1"), IPAddress.Parse("2001:db8::1"));

    [Fact]
    public Task Date_() => RunParity(
        "Date", "'2000-06-15'",
        new DateOnly(2000, 6, 15), new DateOnly(2000, 6, 15), new DateOnly(2000, 6, 15),
        // Driver represents Date as DateTime (Kind=Utc).
        driverAssert: v => Assert.Equal(new DateTime(2000, 6, 15, 0, 0, 0, DateTimeKind.Utc), v));

    [Fact]
    public Task Date32_PreEpoch() => RunParity(
        "Date32", "'1944-06-06'",
        new DateOnly(1944, 6, 6), new DateOnly(1944, 6, 6), new DateOnly(1944, 6, 6),
        driverAssert: v => Assert.Equal(new DateTime(1944, 6, 6, 0, 0, 0, DateTimeKind.Utc), v));

    [Fact]
    public Task DateTime64_3() => RunParity(
        "DateTime64(3, 'UTC')", "'2024-01-01 12:30:45.123'",
        new DateTime(2024, 1, 1, 12, 30, 45, 123, DateTimeKind.Utc),
        new DateTime(2024, 1, 1, 12, 30, 45, 123, DateTimeKind.Utc),
        new DateTime(2024, 1, 1, 12, 30, 45, 123, DateTimeKind.Utc));

    [Fact]
    public Task ArrayInt32() => RunParity(
        "Array(Int32)", "[1,2,3]",
        new[] { 1, 2, 3 }, new[] { 1, 2, 3 }, new[] { 1, 2, 3 });

    [Fact]
    public Task MapStringInt32() => RunParity(
        "Map(String, Int32)", "map('a', 1, 'b', 2)",
        new Dictionary<string, int> { ["a"] = 1, ["b"] = 2 },
        new Dictionary<string, int> { ["a"] = 1, ["b"] = 2 },
        new Dictionary<string, int> { ["a"] = 1, ["b"] = 2 });

    [Fact]
    public Task NullableInt32_Null() => RunParity<int?>(
        "Nullable(Int32)", "NULL", null, DBNull.Value, null,
        driverAssert: v => Assert.Null(v));

    [Fact]
    public Task LowCardinalityString() => RunParity(
        "LowCardinality(String)", "'red'", "red", "red", "red");

    // Enum8: the two clients deliberately differ on representation — CH.Native returns
    // the number, the driver returns the member name (pinned in
    // ClientBehaviorComparisonTests). Parity here means each is internally consistent
    // across all three writers.
    [Fact]
    public Task Enum8_() => RunParity(
        "Enum8('a' = 1, 'b' = 2)", "'a'",
        (sbyte)1, "a", (sbyte)1,
        driverAssert: v => Assert.Equal("a", v));

    private class Row<T>
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "val", Order = 1)] public T Val { get; set; } = default!;
    }
}
