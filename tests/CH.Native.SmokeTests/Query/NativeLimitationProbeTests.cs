using System.Numerics;
using System.Runtime.CompilerServices;
using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Mapping;
using CH.Native.SmokeTests.Fixtures;
using CH.Native.SmokeTests.Helpers;
using Xunit;

namespace CH.Native.SmokeTests.Query;

// Limitation probes for CH.Native itself, with clickhouse-client (and the official
// ClickHouse.Driver) as sources of truth. Each PINNED LIMITATION test asserts the
// CURRENT limited behavior — if one starts failing, the limitation has changed:
// promote the scenario into regular matrix coverage, delete the probe, and update
// the Gotchas section in docs/data-types.md. The remaining tests pin behaviors that
// probing PROVED correct, guarding against regression and stale "known gap" folklore.
//
// Probes that expect a reader throw each use their own connection: a column reader
// failing mid-block leaves partial response bytes in the pipe and poisons the
// connection for any subsequent query.
[Collection("SmokeTest")]
public class NativeLimitationProbeTests
{
    private readonly SmokeTestFixture _fixture;

    public NativeLimitationProbeTests(SmokeTestFixture fixture)
    {
        _fixture = fixture;
    }

    private async Task<List<object?[]>> QueryNativeAsync(string sql, bool lazy = false)
    {
        await using var conn = new ClickHouseConnection(
            lazy ? _fixture.NativeLazyConnectionString : _fixture.NativeConnectionString);
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

    private async Task<string> CreateTableAsync(string columns, [CallerMemberName] string caller = "")
    {
        var table = $"smoke_probe_{Guid.NewGuid():N}";
        await NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString,
            $"CREATE TABLE {table} ({columns}) ENGINE = Memory");
        return table;
    }

    private Task DropTableAsync(string table) =>
        NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString,
            $"DROP TABLE IF EXISTS {table}");

    #region Pinned limitations

    // ESCAPE HATCH ADDED 2026-06-12: the DateTime view of DateTime64(8/9) is
    // still truncated to .NET's 100ns tick resolution (pinned default — a CLR ceiling),
    // but the exact wire value is now reachable via GetFieldValue<long> /
    // ExecuteScalarAsync<long>. The CLI canonical leg proves the server kept all nine
    // fractional digits.
    [Fact]
    public async Task DateTime64_9_DateTimeViewTruncates_RawLongExact()
    {
        var table = await CreateTableAsync("val DateTime64(9, 'UTC')");
        try
        {
            await CliQueryHelper.ExecuteNonQueryAsync(_fixture,
                $"INSERT INTO {table} VALUES ('2024-01-01 00:00:00.123456789')");

            // Source of truth: the server kept all nine fractional digits.
            var cli = await CliQueryHelper.QueryCanonicalAsync(_fixture,
                $"SELECT toString(val) FROM {table}");
            Assert.Equal("2024-01-01 00:00:00.123456789", cli[0][0]);

            // CH.Native DateTime view: .123456789 collapses to .1234567 (100ns ticks).
            var native = await QueryNativeAsync($"SELECT val FROM {table}");
            var dt = Assert.IsType<DateTime>(native[0][0]);
            var expected = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddTicks(1234567);
            Assert.Equal(expected, dt);

            // Escape hatch: the raw Int64 wire value keeps the sub-tick digits.
            await using var conn = new ClickHouseConnection(_fixture.NativeConnectionString);
            await conn.OpenAsync();
            await foreach (var row in conn.QueryStreamAsync($"SELECT val FROM {table}"))
            {
                Assert.Equal(1_704_067_200_123_456_789L, row.GetFieldValue<long>(0));
            }
        }
        finally
        {
            await DropTableAsync(table);
        }
    }

    // PARTIALLY FIXED 2026-06-12: the *string* view of invalid UTF-8 still
    // applies U+FFFD replacement under both materialization modes (pinned default), but
    // under Lazy materialization the exact bytes are now recoverable via
    // GetFieldValue<byte[]>. The CLI hex read proves the server stored 0xFF 0x61.
    [Fact]
    public async Task String_InvalidUtf8_StringViewReplaces_LazyBytesRecover()
    {
        var table = await CreateTableAsync("val String");
        try
        {
            await CliQueryHelper.ExecuteNonQueryAsync(_fixture,
                $"INSERT INTO {table} VALUES (unhex('FF61'))");

            var cliHex = await CliQueryHelper.QueryCanonicalAsync(_fixture,
                $"SELECT hex(val) FROM {table}");
            Assert.Equal("FF61", cliHex[0][0]);

            foreach (var lazy in new[] { false, true })
            {
                var native = await QueryNativeAsync($"SELECT val FROM {table}", lazy);
                Assert.Equal("�a", Assert.IsType<string>(native[0][0]));
            }

            await using var conn = new ClickHouseConnection(_fixture.NativeLazyConnectionString);
            await conn.OpenAsync();
            await foreach (var row in conn.QueryStreamAsync($"SELECT val FROM {table}"))
            {
                Assert.Equal(new byte[] { 0xFF, 0x61 }, row.GetFieldValue<byte[]>(0));
            }
        }
        finally
        {
            await DropTableAsync(table);
        }
    }

    // FIXED 2026-06-12: Interval columns now decode to ClickHouseInterval
    // (count + unit), matching the CLI's canonical rendering. Full unit coverage lives
    // in tests/CH.Native.Tests/Integration/IntervalTypeTests.cs; this keeps the
    // cross-client parity pinned.
    [Fact]
    public async Task Interval_Supported()
    {
        var cli = await CliQueryHelper.QueryCanonicalAsync(_fixture, "SELECT INTERVAL 3 DAY");
        Assert.Equal("3", cli[0][0]);

        var native = await QueryNativeAsync("SELECT INTERVAL 3 DAY");
        Assert.Equal(
            new CH.Native.Data.ClickHouseInterval(3, CH.Native.Data.IntervalUnit.Day),
            native[0][0]);
    }

    // FIXED 2026-06-12: the Nothing type — produced by a bare SELECT NULL
    // (Nullable(Nothing)) or SELECT [] (Array(Nothing)) — is now read natively, matching
    // the CLI and the official driver. Full coverage lives in
    // tests/CH.Native.Tests/Integration/NothingTypeTests.cs; this keeps the cross-client
    // parity pinned.
    [Fact]
    public async Task SelectNull_NothingType_Supported()
    {
        var cli = await CliQueryHelper.QueryCanonicalAsync(_fixture, "SELECT NULL");
        Assert.Null(cli[0][0]);

        var native = await QueryNativeAsync("SELECT NULL");
        Assert.Null(Assert.Single(Assert.Single(native)));

        var nativeArr = await QueryNativeAsync("SELECT []");
        var arr = Assert.IsAssignableFrom<System.Collections.IEnumerable>(nativeArr[0][0]);
        Assert.Empty(arr.Cast<object?>());
    }

    // FIXED 2026-06-12: the direct extractor used to write 4-byte Float32
    // payloads into BFloat16 columns, desyncing the block stream (server error 261
    // "Unknown BlockInfo field number"). It now writes the truncated 2-byte form; the
    // CLI canonical read is the ground truth that the stored values are correct.
    [Fact]
    public async Task BulkInsert_BFloat16_RoundTrips()
    {
        var table = await CreateTableAsync("id Int32, val BFloat16");
        try
        {
            await using (var conn = new ClickHouseConnection(_fixture.NativeConnectionString))
            {
                await conn.OpenAsync();
                await using var ins = conn.CreateBulkInserter<BFloat16Row>(table);
                await ins.InitAsync();
                await ins.AddAsync(new BFloat16Row { Id = 0, Val = -0.5f });
                await ins.AddAsync(new BFloat16Row { Id = 1, Val = 1.5f });
                await ins.AddAsync(new BFloat16Row { Id = 2, Val = 2.5f });
                await ins.CompleteAsync();
            }

            var cli = await CliQueryHelper.QueryCanonicalAsync(_fixture,
                $"SELECT toString(val) FROM {table} ORDER BY id");
            Assert.Equal(new[] { "-0.5", "1.5", "2.5" }, cli.Select(r => r[0]).ToArray());

            var native = await QueryNativeAsync($"SELECT val FROM {table} ORDER BY id");
            Assert.Equal(new[] { -0.5f, 1.5f, 2.5f }, native.Select(r => (float)r[0]!).ToArray());
        }
        finally
        {
            await DropTableAsync(table);
        }
    }

    #endregion

    #region Verified correct (suspected gaps disproven by probing)

    // Disproves the "no Int256/UInt256 extractor" note in TypeBulkRoundTripSmokeTests'
    // header: BigInteger bulk insert works for both. (The CLI matrix covers the read
    // side; this pins the writer.)
    [Fact]
    public async Task BulkInsert_Int256_UInt256_BigInteger_Works()
    {
        foreach (var type in new[] { "Int256", "UInt256" })
        {
            var table = await CreateTableAsync($"id Int32, val {type}");
            try
            {
                await using var conn = new ClickHouseConnection(_fixture.NativeConnectionString);
                await conn.OpenAsync();
                await using var ins = conn.CreateBulkInserter<BigIntegerRow>(table);
                await ins.InitAsync();
                await ins.AddAsync(new BigIntegerRow
                {
                    Id = 0,
                    Val = BigInteger.Parse("12345678901234567890123456789012345678"),
                });
                await ins.CompleteAsync();

                var cli = await CliQueryHelper.QueryCanonicalAsync(_fixture,
                    $"SELECT toString(val) FROM {table}");
                Assert.Equal("12345678901234567890123456789012345678", cli[0][0]);
            }
            finally
            {
                await DropTableAsync(table);
            }
        }
    }

    // Decimal256 keeps all 76 digits through ClickHouseDecimal (beyond System.Decimal's
    // 28-29 significant digits).
    [Fact]
    public async Task Decimal256_76Digits_FullPrecision()
    {
        const string digits76 = "1234567890123456789012345678901234567890123456789012345678901234567890123456";
        var native = await QueryNativeAsync($"SELECT toDecimal256('{digits76}', 0)");
        Assert.Equal(digits76, native[0][0]!.ToString());
    }

    // Date32 covers the full server range — the 2283-11-11 ceiling used elsewhere in the
    // test suite is not a CH.Native limit.
    [Fact]
    public async Task Date32_FullRange_1900_To_2299()
    {
        var native = await QueryNativeAsync("SELECT toDate32('1900-01-01'), toDate32('2299-12-31')");
        Assert.Equal(new DateOnly(1900, 1, 1), native[0][0]);
        Assert.Equal(new DateOnly(2299, 12, 31), native[0][1]);
    }

    // The clickhouse-jdbc 0.9.0 DST fall-back collapse (documented in
    // jvm-clickhouse-native's CrossClientTimezoneDstIT) does NOT affect CH.Native: the
    // two instants sharing the 01:30 Europe/London wall clock on 2024-10-27 stay
    // distinct, returned as DateTimeOffset with the correct pre/post-transition offsets.
    [Fact]
    public async Task DateTime_FallBackOverlap_DistinctInstants()
    {
        var native = await QueryNativeAsync(
            "SELECT toDateTime(1729989000, 'Europe/London'), toDateTime(1729992600, 'Europe/London')");

        var first = Assert.IsType<DateTimeOffset>(native[0][0]);
        var second = Assert.IsType<DateTimeOffset>(native[0][1]);
        Assert.Equal(TimeSpan.FromHours(1), first.Offset);
        Assert.Equal(TimeSpan.Zero, second.Offset);
        Assert.Equal(first.LocalDateTime, second.LocalDateTime); // same wall clock…
        Assert.Equal(TimeSpan.FromHours(1), second.UtcDateTime - first.UtcDateTime); // …distinct instants
    }

    // -0.0 keeps its sign bit through the binary protocol.
    [Fact]
    public async Task Float64_NegativeZero_SignPreserved()
    {
        var table = await CreateTableAsync("val Float64");
        try
        {
            await CliQueryHelper.ExecuteNonQueryAsync(_fixture, $"INSERT INTO {table} VALUES (-0.0)");
            var native = await QueryNativeAsync($"SELECT val FROM {table}");
            var d = Assert.IsType<double>(native[0][0]);
            Assert.Equal(0.0, d);
            Assert.True(double.IsNegative(d), "-0.0 lost its sign bit");
        }
        finally
        {
            await DropTableAsync(table);
        }
    }

    // Embedded NUL bytes in String survive (unlike invalid UTF-8, see #2).
    [Fact]
    public async Task String_EmbeddedNul_Preserved()
    {
        var table = await CreateTableAsync("val String");
        try
        {
            await CliQueryHelper.ExecuteNonQueryAsync(_fixture, $"INSERT INTO {table} VALUES ('a\\0b')");

            var cliHex = await CliQueryHelper.QueryCanonicalAsync(_fixture,
                $"SELECT hex(val) FROM {table}");
            Assert.Equal("610062", cliHex[0][0]);

            var native = await QueryNativeAsync($"SELECT val FROM {table}");
            Assert.Equal("a\0b", native[0][0]);
        }
        finally
        {
            await DropTableAsync(table);
        }
    }

    // Server rejection of an unknown Enum value surfaces as ClickHouseServerException.
    [Fact]
    public async Task Enum8_UnknownValue_ServerRejects()
    {
        var table = await CreateTableAsync("val Enum8('neg' = -1, 'pos' = 1)");
        try
        {
            // Negative member roundtrips; raw read is the numeric value (see
            // Gotchas section of docs/data-types.md).
            await CliQueryHelper.ExecuteNonQueryAsync(_fixture, $"INSERT INTO {table} VALUES ('neg')");
            var native = await QueryNativeAsync($"SELECT val FROM {table}");
            Assert.Equal((sbyte)-1, native[0][0]);

            await Assert.ThrowsAsync<ClickHouseServerException>(() =>
                NativeQueryHelper.ExecuteNonQueryAsync(_fixture.NativeConnectionString,
                    $"INSERT INTO {table} VALUES ('zzz')"));
        }
        finally
        {
            await DropTableAsync(table);
        }
    }

    // FixedString reads back as byte[] including trailing NUL padding — a faithful
    // (if raw) representation; toString(val) provides the trimmed text form.
    [Fact]
    public async Task FixedString_TrailingNulPadding_ByteArray()
    {
        var table = await CreateTableAsync("val FixedString(8)");
        try
        {
            await CliQueryHelper.ExecuteNonQueryAsync(_fixture, $"INSERT INTO {table} VALUES ('abc')");
            var native = await QueryNativeAsync($"SELECT val FROM {table}");
            var bytes = Assert.IsType<byte[]>(native[0][0]);
            Assert.Equal("6162630000000000", Convert.ToHexString(bytes));
        }
        finally
        {
            await DropTableAsync(table);
        }
    }

    // Deeply nested composite decodes without recursion issues.
    [Fact]
    public async Task DeepComposite_ArrayMapTupleArrayNullable()
    {
        var native = await QueryNativeAsync(
            "SELECT [map('k', (1, ['x', NULL]))]::Array(Map(String, Tuple(Int32, Array(Nullable(String)))))");

        var arr = Assert.IsAssignableFrom<System.Collections.IEnumerable>(native[0][0])
            .Cast<object?>().ToList();
        var map = Assert.IsAssignableFrom<System.Collections.IDictionary>(Assert.Single(arr));
        var tuple = Assert.IsAssignableFrom<ITuple>(map["k"]);
        Assert.Equal(1, tuple[0]);
        Assert.Equal(new[] { "x", null }, Assert.IsType<string?[]>(tuple[1]));
    }

    // Reading a JSON column does NOT require allow_experimental_json_type on the reading
    // session (the flag gates DDL/INSERT, not the wire format).
    [Fact]
    public async Task Json_ReadWithoutSessionFlag_Works()
    {
        var table = $"smoke_probe_{Guid.NewGuid():N}";
        try
        {
            await CliQueryHelper.ExecuteNonQueryAsync(_fixture,
                $"CREATE TABLE {table} (val JSON) ENGINE = Memory",
                new[] { "--allow_experimental_json_type=1" });
            await CliQueryHelper.ExecuteNonQueryAsync(_fixture,
                $"INSERT INTO {table} VALUES ('{{\"a\":1}}')",
                new[] { "--allow_experimental_json_type=1" });

            var native = await QueryNativeAsync($"SELECT val FROM {table}");
            var doc = Assert.IsType<System.Text.Json.JsonDocument>(native[0][0]);
            Assert.Equal(1, doc.RootElement.GetProperty("a").GetInt32());
        }
        finally
        {
            await DropTableAsync(table);
        }
    }

    // Zero-row result sets stream cleanly.
    [Fact]
    public async Task ZeroRowResult_Empty()
    {
        var native = await QueryNativeAsync("SELECT 1 WHERE 0");
        Assert.Empty(native);
    }

    #endregion

    private class BFloat16Row
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "val", Order = 1)] public float Val { get; set; }
    }

    private class BigIntegerRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "val", Order = 1)] public BigInteger Val { get; set; }
    }
}
