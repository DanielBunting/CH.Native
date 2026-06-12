using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SmokeTests.Fixtures;
using CH.Native.SmokeTests.Helpers;
using ClickHouse.Driver.Utility;
using Xunit;
using DriverConnection = ClickHouse.Driver.ADO.ClickHouseConnection;

namespace CH.Native.SmokeTests.Query;

// Three-way behavior pinning: for scenarios where the clients genuinely differ, each
// test states what clickhouse-client (ground truth), CH.Native, AND the official
// ClickHouse.Driver do today. Driver-side findings live in LIMITATIONS.md alongside the
// CH.Native ones; if an upstream driver release changes a pinned behavior, the test
// fails loudly and the doc gets updated.
[Collection("SmokeTest")]
public class ClientBehaviorComparisonTests
{
    private readonly SmokeTestFixture _fixture;

    public ClientBehaviorComparisonTests(SmokeTestFixture fixture)
    {
        _fixture = fixture;
    }

    private async Task<List<object?[]>> QueryNativeAsync(string sql)
    {
        await using var conn = new ClickHouseConnection(_fixture.NativeConnectionString);
        await conn.OpenAsync();
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

    private Task<List<object?[]>> QueryDriverAsync(string sql) =>
        DriverQueryHelper.QueryStreamAsync(_fixture.DriverConnectionString, sql);

    // LIMITATIONS.md #3 / #D1: neither .NET client supports Interval columns; only the
    // CLI answers. The two clients fail with different exception types.
    [Fact]
    public async Task Interval_OnlyCliSupports()
    {
        var cli = await CliQueryHelper.QueryCanonicalAsync(_fixture, "SELECT INTERVAL 3 DAY");
        Assert.Equal("3", cli[0][0]);

        var nativeEx = await Assert.ThrowsAsync<NotSupportedException>(
            () => QueryNativeAsync("SELECT INTERVAL 3 DAY"));
        Assert.Contains("IntervalDay", nativeEx.Message);

        var driverEx = await Assert.ThrowsAsync<ArgumentException>(
            () => QueryDriverAsync("SELECT INTERVAL 3 DAY"));
        Assert.Contains("IntervalDay", driverEx.Message);
    }

    // LIMITATIONS.md #4: the Nothing type splits the clients — the driver (like the CLI)
    // answers bare SELECT NULL / SELECT []; CH.Native throws.
    [Fact]
    public async Task NothingType_DriverHandles_NativeThrows()
    {
        Assert.Null((await QueryDriverAsync("SELECT NULL"))[0][0]);
        Assert.Empty((System.Collections.IEnumerable)(await QueryDriverAsync("SELECT []"))[0][0]!);

        await Assert.ThrowsAsync<NotSupportedException>(() => QueryNativeAsync("SELECT NULL"));
    }

    // LIMITATIONS.md #1 / #D3: both .NET clients truncate DateTime64(9) to 100ns ticks —
    // a shared System.DateTime resolution limit, not protocol-specific. The CLI shows
    // the server kept all nine digits.
    [Fact]
    public async Task DateTime64_9_BothNetClientsTruncateTo100ns()
    {
        var table = $"smoke_cmp_{Guid.NewGuid():N}";
        try
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString,
                $"CREATE TABLE {table} (val DateTime64(9, 'UTC')) ENGINE = Memory");
            await CliQueryHelper.ExecuteNonQueryAsync(_fixture,
                $"INSERT INTO {table} VALUES ('2024-01-01 00:00:00.123456789')");

            var cli = await CliQueryHelper.QueryCanonicalAsync(_fixture,
                $"SELECT toString(val) FROM {table}");
            Assert.Equal("2024-01-01 00:00:00.123456789", cli[0][0]);

            var expected = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddTicks(1234567);
            Assert.Equal(expected, Assert.IsType<DateTime>(
                (await QueryNativeAsync($"SELECT val FROM {table}"))[0][0]));
            Assert.Equal(expected, Assert.IsType<DateTime>(
                (await QueryDriverAsync($"SELECT val FROM {table}"))[0][0]));
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    // LIMITATIONS.md #2 / #D4: both .NET clients decode String as UTF-8 with U+FFFD
    // replacement, so invalid byte sequences are unrecoverable through either.
    [Fact]
    public async Task InvalidUtf8_BothNetClientsReplaceWithFffd()
    {
        var table = $"smoke_cmp_{Guid.NewGuid():N}";
        try
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString,
                $"CREATE TABLE {table} (val String) ENGINE = Memory");
            await CliQueryHelper.ExecuteNonQueryAsync(_fixture,
                $"INSERT INTO {table} VALUES (unhex('FF61'))");

            var cliHex = await CliQueryHelper.QueryCanonicalAsync(_fixture,
                $"SELECT hex(val) FROM {table}");
            Assert.Equal("FF61", cliHex[0][0]);

            Assert.Equal("�a", (await QueryNativeAsync($"SELECT val FROM {table}"))[0][0]);
            Assert.Equal("�a", (await QueryDriverAsync($"SELECT val FROM {table}"))[0][0]);
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    // LIMITATIONS.md #D2 — the .NET analogue of the clickhouse-jdbc 0.9.0 DST bug:
    // ClickHouse.Driver returns zoned DateTime columns as wall-clock System.DateTime in
    // the column zone, so the two distinct instants sharing the 01:30 Europe/London wall
    // clock on 2024-10-27 collapse to IDENTICAL tick values (the Kind flag differs —
    // Unspecified vs Utc — but carries no usable offset). CH.Native returns
    // DateTimeOffset and keeps the instants distinct.
    [Fact]
    public async Task DstFallBackOverlap_DriverCollapsesWallClock_NativePreservesInstants()
    {
        const string sql =
            "SELECT toDateTime(1729989000, 'Europe/London'), toDateTime(1729992600, 'Europe/London')";

        var native = (await QueryNativeAsync(sql))[0];
        var nFirst = Assert.IsType<DateTimeOffset>(native[0]);
        var nSecond = Assert.IsType<DateTimeOffset>(native[1]);
        Assert.Equal(TimeSpan.FromHours(1), nSecond.UtcDateTime - nFirst.UtcDateTime);

        var driver = (await QueryDriverAsync(sql))[0];
        var dFirst = Assert.IsType<DateTime>(driver[0]);
        var dSecond = Assert.IsType<DateTime>(driver[1]);
        Assert.Equal(dFirst.Ticks, dSecond.Ticks); // distinct instants, same wall clock
        Assert.Equal(DateTimeKind.Unspecified, dFirst.Kind);
        Assert.Equal(DateTimeKind.Utc, dSecond.Kind);
    }

    // LIMITATIONS.md #D5: the driver's parameter binding travels as text and the
    // server's default fast float parser flushes the minimum denormal to zero (the
    // server-side issue jvm-clickhouse-native documented; SETTINGS
    // precise_float_parsing=1 is the upstream workaround). CH.Native's binary bulk
    // insert carries the exact bit pattern. Read paths are NOT affected in either
    // client: a binary-stored denormal reads back bit-exact through both.
    [Fact]
    public async Task Float64MinDenormal_DriverTextParamFlushesToZero_NativeBinaryExact()
    {
        const double minDenormal = double.Epsilon; // 4.9406564584124654E-324
        var table = $"smoke_cmp_{Guid.NewGuid():N}";
        try
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString,
                $"CREATE TABLE {table} (id Int32, val Float64) ENGINE = Memory");

            using (var conn = new DriverConnection(_fixture.DriverConnectionString))
            {
                await conn.OpenAsync();
                using var cmd = conn.CreateCommand();
                cmd.CommandText =
                    $"INSERT INTO {table} (id, val) VALUES ({{id:Int32}}, {{v:Float64}})";
                cmd.AddParameter("id", 0);
                cmd.AddParameter("v", minDenormal);
                await cmd.ExecuteNonQueryAsync();
            }

            await using (var conn = new ClickHouseConnection(_fixture.NativeConnectionString))
            {
                await conn.OpenAsync();
                await using var ins = conn.CreateBulkInserter<FloatRow>(table);
                await ins.InitAsync();
                await ins.AddAsync(new FloatRow { Id = 1, Val = minDenormal });
                await ins.CompleteAsync();
            }

            var cli = await CliQueryHelper.QueryCanonicalAsync(_fixture,
                $"SELECT toString(val) FROM {table} ORDER BY id");
            Assert.Equal("0", cli[0][0]);       // driver text param: flushed to zero
            Assert.Equal("5e-324", cli[1][0]);  // native binary insert: exact

            // Both clients read the surviving denormal bit-exactly.
            foreach (var rows in new[]
            {
                await QueryNativeAsync($"SELECT val FROM {table} WHERE id = 1"),
                await QueryDriverAsync($"SELECT val FROM {table} WHERE id = 1"),
            })
            {
                Assert.Equal(1L, BitConverter.DoubleToInt64Bits((double)rows[0][0]!));
            }
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    // LIMITATIONS.md #D6: the driver materializes Dynamic/Variant values as strings,
    // discarding the runtime type (CH.Native's typed ClickHouseDynamic/ClickHouseVariant
    // roundtrips are covered in CliTypeRoundTripSmokeTests).
    [Fact]
    public async Task DynamicVariant_DriverReturnsStrings()
    {
        var dyn = await QueryDriverAsync("SELECT 42::Dynamic SETTINGS allow_experimental_dynamic_type=1");
        Assert.Equal("42", Assert.IsType<string>(dyn[0][0]));

        var var_ = await QueryDriverAsync("SELECT 'hi'::Variant(Int64, String) SETTINGS allow_experimental_variant_type=1");
        Assert.Equal("hi", Assert.IsType<string>(var_[0][0]));
    }

    // Representation differences (neither wrong, but easy to trip over when porting
    // code between the clients):
    //   Enum8       — driver: member name (string); CH.Native: numeric value (sbyte)
    //   FixedString — driver: string incl. NUL padding; CH.Native: byte[]
    //   Date32      — driver: DateTime (Utc); CH.Native: DateOnly
    [Fact]
    public async Task RepresentationDifferences_Enum_FixedString_Date32()
    {
        const string enumSql = "SELECT CAST('neg', 'Enum8(\\'neg\\' = -1, \\'pos\\' = 1)')";
        Assert.Equal("neg", (await QueryDriverAsync(enumSql))[0][0]);
        Assert.Equal((sbyte)-1, (await QueryNativeAsync(enumSql))[0][0]);

        const string fsSql = "SELECT 'abc'::FixedString(8)";
        Assert.Equal("abc\0\0\0\0\0", Assert.IsType<string>((await QueryDriverAsync(fsSql))[0][0]));
        Assert.Equal("6162630000000000",
            Convert.ToHexString(Assert.IsType<byte[]>((await QueryNativeAsync(fsSql))[0][0])));

        const string dateSql = "SELECT toDate32('2299-12-31')";
        Assert.Equal(new DateTime(2299, 12, 31, 0, 0, 0, DateTimeKind.Utc),
            Assert.IsType<DateTime>((await QueryDriverAsync(dateSql))[0][0]));
        Assert.Equal(new DateOnly(2299, 12, 31),
            Assert.IsType<DateOnly>((await QueryNativeAsync(dateSql))[0][0]));
    }

    private class FloatRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "val", Order = 1)] public double Val { get; set; }
    }
}
