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
// ClickHouse.Driver do today. Driver-side findings are documented in the comments on
// each test; if an upstream driver release changes a pinned behavior, the test fails
// loudly. User-facing impedance mismatches are documented in the Gotchas section of
// docs/data-types.md.
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

    // Driver limitation: CH.Native now decodes Interval columns to ClickHouseInterval
    // (was CH.Native limitation #3, fixed); the official driver still throws. The CLI
    // remains the canonical ground truth.
    [Fact]
    public async Task Interval_NativeAndCliSupport_DriverThrows()
    {
        var cli = await CliQueryHelper.QueryCanonicalAsync(_fixture, "SELECT INTERVAL 3 DAY");
        Assert.Equal("3", cli[0][0]);

        var native = await QueryNativeAsync("SELECT INTERVAL 3 DAY");
        Assert.Equal(
            new CH.Native.Data.ClickHouseInterval(3, CH.Native.Data.IntervalUnit.Day),
            native[0][0]);

        var driverEx = await Assert.ThrowsAsync<ArgumentException>(
            () => QueryDriverAsync("SELECT INTERVAL 3 DAY"));
        Assert.Contains("IntervalDay", driverEx.Message);
    }

    // The Nothing type (bare SELECT NULL / SELECT []) is answered by all three clients.
    // (CH.Native threw on these until the NothingColumnReader landed, 2026-06-12.)
    [Fact]
    public async Task NothingType_AllClientsHandle()
    {
        Assert.Null((await QueryDriverAsync("SELECT NULL"))[0][0]);
        Assert.Empty((System.Collections.IEnumerable)(await QueryDriverAsync("SELECT []"))[0][0]!);

        Assert.Null((await QueryNativeAsync("SELECT NULL"))[0][0]);
        Assert.Empty((System.Collections.IEnumerable)(await QueryNativeAsync("SELECT []"))[0][0]!);
    }

    // Shared CLR limitation: both .NET clients truncate the DateTime view of
    // DateTime64(9) to 100ns ticks — a shared System.DateTime resolution limit. The CLI
    // shows the server kept all nine digits. CH.Native additionally exposes the exact
    // raw Int64 via GetFieldValue<long> (asserted below); the official driver has no
    // equivalent escape hatch.
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

            // CH.Native escape hatch: the exact wire value survives as a long.
            await using var conn = new ClickHouseConnection(_fixture.NativeConnectionString);
            await conn.OpenAsync();
            var raw = await conn.ExecuteScalarAsync<long>($"SELECT val FROM {table}");
            Assert.Equal(1_704_067_200_123_456_789L, raw);
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    // Shared limitation: both .NET clients decode String as UTF-8 with U+FFFD
    // replacement by default. CH.Native can now recover the exact bytes via
    // StringMaterialization=Lazy + GetFieldValue<byte[]> (asserted below); the official
    // driver has no equivalent escape hatch.
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

            // CH.Native escape hatch: lazy materialization recovers the stored bytes.
            await using var lazyConn = new ClickHouseConnection(_fixture.NativeLazyConnectionString);
            await lazyConn.OpenAsync();
            await foreach (var row in lazyConn.QueryStreamAsync($"SELECT val FROM {table}"))
            {
                Assert.Equal(new byte[] { 0xFF, 0x61 }, row.GetFieldValue<byte[]>(0));
            }
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    // Driver limitation — the .NET analogue of the clickhouse-jdbc 0.9.0 DST bug:
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

    // Shared limitation — parameter binding travels as text in BOTH clients, and the
    // server's default fast float parser flushes the minimum denormal to zero (the
    // server-side issue jvm-clickhouse-native documented; SETTINGS
    // precise_float_parsing=1 is the upstream workaround). CH.Native's binary bulk
    // insert carries the exact bit pattern. Read paths are NOT affected in either
    // client: a binary-stored denormal reads back bit-exact through both.
    [Fact]
    public async Task Float64MinDenormal_TextParamsFlushToZeroInBothClients_BinaryInsertExact()
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

            // CH.Native's parameter path is text too — same flush-to-zero (id 2).
            await using (var nconn = new ClickHouseConnection(_fixture.NativeConnectionString))
            {
                await nconn.OpenAsync();
                await using var ncmd = nconn.CreateCommand();
                ncmd.CommandText =
                    $"INSERT INTO {table} (id, val) VALUES ({{id:Int32}}, {{v:Float64}})";
                ncmd.Parameters.Add("id", 2);
                ncmd.Parameters.Add("v", minDenormal);
                await ncmd.ExecuteNonQueryAsync();
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
            Assert.Equal("0", cli[2][0]);       // native text param: flushed to zero too

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

    // Driver limitation: the driver materializes Dynamic/Variant values as strings,
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

    // Representation differences: 128-bit integers — CH.Native uses the CLR Int128 /
    // UInt128 types; the driver boxes BigInteger.
    [Fact]
    public async Task Int128_NativeClrTypes_DriverBigInteger()
    {
        const string sql = "SELECT 170141183460469231731687303715884105727::Int128";

        Assert.Equal(Int128.MaxValue, Assert.IsType<Int128>((await QueryNativeAsync(sql))[0][0]));
        Assert.Equal(
            System.Numerics.BigInteger.Parse("170141183460469231731687303715884105727"),
            Assert.IsType<System.Numerics.BigInteger>((await QueryDriverAsync(sql))[0][0]));
    }

    // Representation differences: Time — CH.Native returns TimeOnly (a time-of-day);
    // the driver returns TimeSpan.
    [Fact]
    public async Task Time_NativeTimeOnly_DriverTimeSpan()
    {
        const string sql = "SELECT '12:34:56'::Time SETTINGS enable_time_time64_type=1";

        Assert.Equal(new TimeOnly(12, 34, 56), Assert.IsType<TimeOnly>((await QueryNativeAsync(sql))[0][0]));
        Assert.Equal(new TimeSpan(12, 34, 56), Assert.IsType<TimeSpan>((await QueryDriverAsync(sql))[0][0]));
    }

    // Representation differences: geo — CH.Native returns Point structs; the driver
    // returns Tuple<double, double>.
    [Fact]
    public async Task GeoRing_NativePoints_DriverTuples()
    {
        const string sql = "SELECT [(0.,0.),(1.,0.),(1.,1.)]::Ring";

        var native = Assert.IsType<CH.Native.Data.Geo.Point[]>((await QueryNativeAsync(sql))[0][0]);
        Assert.Equal(new CH.Native.Data.Geo.Point(1, 0), native[1]);

        var driver = Assert.IsType<Tuple<double, double>[]>((await QueryDriverAsync(sql))[0][0]);
        Assert.Equal(Tuple.Create(1.0, 0.0), driver[1]);
    }

    // Representation differences: decimals — CH.Native returns System.Decimal for
    // Decimal32/64/128; the driver returns its own ClickHouse.Driver.Numerics
    // ClickHouseDecimal for every decimal width.
    [Fact]
    public async Task Decimal64_NativeSystemDecimal_DriverOwnDecimalType()
    {
        const string sql = "SELECT toDecimal64(-0.0001, 4)";

        Assert.Equal(-0.0001m, Assert.IsType<decimal>((await QueryNativeAsync(sql))[0][0]));

        var driverValue = (await QueryDriverAsync(sql))[0][0];
        Assert.Equal("ClickHouse.Driver.Numerics.ClickHouseDecimal", driverValue!.GetType().FullName);
        Assert.Equal(-0.0001m, Convert.ToDecimal(driverValue));
    }

    // Representation differences: Enum16 with a negative member — CH.Native returns the
    // number (short), the driver returns the member name.
    [Fact]
    public async Task Enum16Negative_NativeNumber_DriverName()
    {
        const string sql = "SELECT CAST('neg', 'Enum16(\\'neg\\' = -300, \\'pos\\' = 300)')";

        Assert.Equal((short)-300, (await QueryNativeAsync(sql))[0][0]);
        Assert.Equal("neg", (await QueryDriverAsync(sql))[0][0]);
    }

    // Write-semantics parity: both clients agree on DateTime.Kind handling for
    // parameter inserts into a zoned column — Utc and Unspecified are taken as the
    // UTC instant verbatim; Local is converted to UTC first. The Unspecified case is
    // the one worth knowing: it is NOT interpreted in the column's or host's zone.
    [Fact]
    public async Task DateTimeKindOnParameterWrite_BothClientsAgree()
    {
        var utcInstant = new DateTimeOffset(2024, 6, 15, 12, 0, 0, TimeSpan.Zero).ToUnixTimeSeconds();
        var local = new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Local);
        var localInstant = new DateTimeOffset(local).ToUnixTimeSeconds();

        foreach (var (kind, expectedUnix) in new[]
        {
            (DateTimeKind.Utc, utcInstant),
            (DateTimeKind.Unspecified, utcInstant),
            (DateTimeKind.Local, localInstant),
        })
        {
            var dt = new DateTime(2024, 6, 15, 12, 0, 0, kind);
            var table = $"smoke_cmp_{Guid.NewGuid():N}";
            try
            {
                await NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString,
                    $"CREATE TABLE {table} (id Int32, val DateTime('UTC')) ENGINE = Memory");

                await using (var nconn = new ClickHouseConnection(_fixture.NativeConnectionString))
                {
                    await nconn.OpenAsync();
                    await using var ncmd = nconn.CreateCommand();
                    ncmd.CommandText = $"INSERT INTO {table} VALUES ({{id:Int32}}, {{v:DateTime('UTC')}})";
                    ncmd.Parameters.Add("id", 0);
                    ncmd.Parameters.Add("v", dt);
                    await ncmd.ExecuteNonQueryAsync();
                }

                using (var dconn = new DriverConnection(_fixture.DriverConnectionString))
                {
                    await dconn.OpenAsync();
                    using var dcmd = dconn.CreateCommand();
                    dcmd.CommandText = $"INSERT INTO {table} VALUES ({{id:Int32}}, {{v:DateTime('UTC')}})";
                    dcmd.AddParameter("id", 1);
                    dcmd.AddParameter("v", dt);
                    await dcmd.ExecuteNonQueryAsync();
                }

                var unix = await CliQueryHelper.QueryCanonicalAsync(_fixture,
                    $"SELECT toUnixTimestamp(val) FROM {table} ORDER BY id");
                Assert.Equal(expectedUnix.ToString(), unix[0][0]); // CH.Native
                Assert.Equal(expectedUnix.ToString(), unix[1][0]); // driver
            }
            finally
            {
                await NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString,
                    $"DROP TABLE IF EXISTS {table}");
            }
        }
    }

    // Write-semantics parity: bulk-inserting a decimal with more fractional digits than
    // the column's scale truncates silently in BOTH clients (1.23456 -> 1.2345 in
    // Decimal64(4)) — neither rounds nor throws.
    [Fact]
    public async Task DecimalScaleOverflowOnWrite_BothClientsTruncateSilently()
    {
        var table = $"smoke_cmp_{Guid.NewGuid():N}";
        try
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString,
                $"CREATE TABLE {table} (id Int32, val Decimal64(4)) ENGINE = Memory");

            await using (var conn = new ClickHouseConnection(_fixture.NativeConnectionString))
            {
                await conn.OpenAsync();
                await using var ins = conn.CreateBulkInserter<DecimalRow>(table);
                await ins.InitAsync();
                await ins.AddAsync(new DecimalRow { Id = 0, Val = 1.23456m });
                await ins.CompleteAsync();
            }

            using (var conn = new DriverConnection(_fixture.DriverConnectionString))
            {
                await conn.OpenAsync();
                using var bulk = new ClickHouse.Driver.Copy.ClickHouseBulkCopy(conn)
                {
                    DestinationTableName = table,
                    ColumnNames = new[] { "id", "val" },
                };
                await bulk.WriteToServerAsync(new List<object?[]> { new object?[] { 1, 1.23456m } });
            }

            var cli = await CliQueryHelper.QueryCanonicalAsync(_fixture,
                $"SELECT toString(val) FROM {table} ORDER BY id");
            Assert.Equal("1.2345", cli[0][0]); // CH.Native bulk: truncated
            Assert.Equal("1.2345", cli[1][0]); // driver bulk: truncated
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    private class FloatRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "val", Order = 1)] public double Val { get; set; }
    }

    private class DecimalRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "val", Order = 1)] public decimal Val { get; set; }
    }
}
