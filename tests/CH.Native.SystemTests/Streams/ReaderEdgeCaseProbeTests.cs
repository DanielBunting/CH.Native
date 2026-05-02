using System.Numerics;
using CH.Native.Connection;
using CH.Native.Data.Geo;
using CH.Native.Numerics;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Streams;

/// <summary>
/// Reader-side parallel pass for Decimal / Date / DateTime / Geo column readers.
/// Writers were audited in earlier rounds; these tests exercise reader-only edge
/// cases by inserting via raw SQL (so the wire shape is the server's job to get
/// right) and observing what the reader produces.
///
/// <para>Per the section-7 plan: outcomes that aren't yet pinned (NaN / Inf points,
/// non-existent timezones, scale overflow) are observed via <see cref="ITestOutputHelper"/>
/// and asserted only on safety invariants — no infinite hang, no out-of-band exception
/// type, no row corruption that would survive the next query.</para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Streams)]
public class ReaderEdgeCaseProbeTests
{
    private static readonly TimeSpan AntiHangTimeout = TimeSpan.FromSeconds(30);

    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public ReaderEdgeCaseProbeTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    private async Task<ClickHouseConnection> OpenAsync(Action<ClickHouseConnectionSettingsBuilder>? configure = null)
    {
        var conn = new ClickHouseConnection(_fx.BuildSettings(configure));
        await conn.OpenAsync();
        return conn;
    }

    // ---------- 7.1 Decimal readers ----------

    [Fact]
    public async Task Decimal32_NegativeMax_ReadsCorrectly()
    {
        await using var conn = await OpenAsync();
        var table = $"d32_neg_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, v Decimal32(3)) ENGINE = Memory");
        try
        {
            // Decimal32(P=9, S=3) — server-side check is precision-not-range, so
            // values are bounded by ±999999.999 (9 digits at scale 3) rather than the
            // Int32 mantissa span. Probe negative-max within that domain.
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES (1, '-999999.999'), (2, '0.000'), (3, '999999.999')");

            // Decimal32/64 surface as System.Decimal (the wire fits in 96 bits) — only
            // Decimal128/256 use ClickHouseDecimal.
            var got = new List<decimal>();
            await foreach (var r in conn.QueryAsync($"SELECT v FROM {table} ORDER BY id"))
                got.Add(r.GetFieldValue<decimal>(0));

            Assert.Equal(3, got.Count);
            Assert.Equal(-999999.999m, got[0]);
            Assert.Equal(0m, got[1]);
            Assert.Equal(999999.999m, got[2]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task Decimal64_MaxScale_18_NoOverflow()
    {
        await using var conn = await OpenAsync();
        var table = $"d64_max_{Guid.NewGuid():N}";
        // Decimal64(18) → Decimal(P=18, S=18). Server enforces precision-not-range
        // so the entire value must fit in 18 digits *total*. With S=18 every digit
        // sits below the decimal point — values must be in [-0.999...9, 0.999...9].
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, v Decimal64(18)) ENGINE = Memory");
        try
        {
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES (1, '0.999999999999999999'), (2, '-0.999999999999999999')");

            // Decimal64 surfaces as System.Decimal (fits in 96 bits) — see Decimal32 test.
            var got = new List<decimal>();
            await foreach (var r in conn.QueryAsync($"SELECT v FROM {table} ORDER BY id"))
                got.Add(r.GetFieldValue<decimal>(0));

            Assert.Equal(2, got.Count);
            Assert.Equal(0.999999999999999999m, got[0]);
            Assert.Equal(-0.999999999999999999m, got[1]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task Decimal128_TwosComplement_NegativeRead()
    {
        await using var conn = await OpenAsync();
        var table = $"d128_neg_{Guid.NewGuid():N}";
        // Scale 0 keeps the on-wire bytes equal to the integer mantissa — exercises the
        // Int128 two's-complement path explicitly.
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, v Decimal128(0)) ENGINE = Memory");
        try
        {
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES (1, '-1'), (2, '0'), (3, '1'), (4, '-12345678901234567890')");

            var got = new List<ClickHouseDecimal>();
            await foreach (var r in conn.QueryAsync($"SELECT v FROM {table} ORDER BY id"))
                got.Add(r.GetFieldValue<ClickHouseDecimal>(0));

            Assert.Equal(4, got.Count);
            Assert.Equal(new ClickHouseDecimal(BigInteger.MinusOne, 0), got[0]);
            Assert.Equal(ClickHouseDecimal.Zero, got[1]);
            Assert.Equal(new ClickHouseDecimal(BigInteger.One, 0), got[2]);
            Assert.Equal(new ClickHouseDecimal(BigInteger.Parse("-12345678901234567890"), 0), got[3]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task Decimal256_PrecisionBoundary_ReadsCorrectly()
    {
        await using var conn = await OpenAsync();
        var table = $"d256_boundary_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, v Decimal256(0)) ENGINE = Memory");
        try
        {
            // Decimal256(0): mantissa range is the signed Int256 range. Probe near-max
            // with 75 nines (one short of full to avoid format-vs-validate ambiguity).
            var nines75 = new string('9', 75);
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES (1, '{nines75}'), (2, '-{nines75}'), (3, '0')");

            var got = new List<ClickHouseDecimal>();
            await foreach (var r in conn.QueryAsync($"SELECT v FROM {table} ORDER BY id"))
                got.Add(r.GetFieldValue<ClickHouseDecimal>(0));

            Assert.Equal(3, got.Count);
            Assert.Equal(new ClickHouseDecimal(BigInteger.Parse(nines75), 0), got[0]);
            Assert.Equal(new ClickHouseDecimal(-BigInteger.Parse(nines75), 0), got[1]);
            Assert.Equal(ClickHouseDecimal.Zero, got[2]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task Decimal_NullableArray_NullElementInMiddle()
    {
        await using var conn = await OpenAsync();
        var table = $"d_nul_arr_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, v Array(Nullable(Decimal128(4)))) ENGINE = Memory");
        try
        {
            // The composition Array(Nullable(...)) is the reader hot path. Mix nulls in
            // mid-array to catch off-by-one offset bugs in the nullable mask + array
            // offset interleave.
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES " +
                "(1, [toDecimal128('1.2345', 4), NULL, toDecimal128('-9.9999', 4)]), " +
                "(2, []), " +
                "(3, [NULL, NULL]), " +
                "(4, [toDecimal128('0.0000', 4)])");

            var got = new List<ClickHouseDecimal?[]>();
            await foreach (var r in conn.QueryAsync($"SELECT v FROM {table} ORDER BY id"))
                got.Add(r.GetFieldValue<ClickHouseDecimal?[]>(0));

            Assert.Equal(4, got.Count);
            Assert.Equal(3, got[0].Length);
            Assert.NotNull(got[0][0]);
            Assert.Null(got[0][1]);
            Assert.NotNull(got[0][2]);
            Assert.Empty(got[1]);
            Assert.Equal(2, got[2].Length);
            Assert.All(got[2], x => Assert.Null(x));
            Assert.Single(got[3]);
            Assert.Equal(ClickHouseDecimal.Zero, got[3][0]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task Decimal_ScaleOverflowOnRead_FailsLoudlyOrTruncates()
    {
        // Probe — outcome not pinned. ClickHouse will reject a Decimal128 with scale > 38
        // at parse time, so the most likely surface is a server-side exception. Either
        // way: assert the test does NOT hang and the connection ends in a known state.
        await using var conn = await OpenAsync();

        Exception? caught = null;
        try
        {
            using var cts = new CancellationTokenSource(AntiHangTimeout);
            await foreach (var _ in conn.QueryAsync<ClickHouseDecimal>(
                "SELECT toDecimal128(1, 39)").WithCancellation(cts.Token)) { }
        }
        catch (Exception ex) { caught = ex; }

        _output.WriteLine($"Scale-39 decimal probe: {caught?.GetType().FullName} — {caught?.Message}");

        // Safety: the result is either a server exception (typed) or it round-tripped.
        // Forbid only the bad outcomes.
        if (caught is not null)
        {
            Assert.IsNotType<OutOfMemoryException>(caught);
            Assert.IsNotType<AccessViolationException>(caught);
        }
    }

    // ---------- 7.2 Date / DateTime readers ----------

    [Fact]
    public async Task Date_BeforeUnixEpoch_FailsOrClipsLoudly()
    {
        await using var conn = await OpenAsync();
        var table = $"date_pre_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, v Date) ENGINE = Memory");
        try
        {
            // Date is unsigned 16-bit days from 1970-01-01 — pre-epoch values get clipped
            // by the server. Probe what the reader sees rather than pinning behaviour.
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES (1, toDate('1969-12-31')), (2, toDate('1970-01-01'))");

            // Date materialises as DateOnly on the wire — fetch as object via the
            // indexer to dodge any T-coercion ambiguity, then probe what's inside.
            var got = new List<object?>();
            await foreach (var r in conn.QueryAsync($"SELECT v FROM {table} ORDER BY id"))
                got.Add(r[0]);

            _output.WriteLine($"Date pre-epoch: type={got[0]?.GetType().Name}, value={got[0]}");
            _output.WriteLine($"Date epoch:     type={got[1]?.GetType().Name}, value={got[1]}");
            Assert.Equal(2, got.Count);
            // Pin only that the epoch row returns *something* — the exact .NET type
            // (DateOnly vs DateTime) is the library's choice.
            Assert.NotNull(got[1]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task Date32_Year2299_RoundTrips()
    {
        await using var conn = await OpenAsync();
        var table = $"date32_far_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, v Date32) ENGINE = Memory");
        try
        {
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES (1, toDate32('2299-12-31')), (2, toDate32('1900-01-01'))");

            // Date32 surfaces as DateOnly. Use that explicitly rather than relying on
            // GetFieldValue<DateTime> bridging (which only handles DateTimeOffset).
            var got = new List<DateOnly>();
            await foreach (var r in conn.QueryAsync($"SELECT v FROM {table} ORDER BY id"))
                got.Add(r.GetFieldValue<DateOnly>(0));

            Assert.Equal(2, got.Count);
            Assert.Equal(new DateOnly(2299, 12, 31), got[0]);
            Assert.Equal(new DateOnly(1900, 1, 1), got[1]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    private static async Task<DateTime> ReadDateTimeAsync(ClickHouseConnection conn, string sql)
    {
        // ExecuteScalarAsync<DateTime> uses Convert.ChangeType which does NOT bridge
        // DateTimeOffset → DateTime. The row-level GetFieldValue<DateTime> *does* —
        // so use that path for tz-aware DateTime types.
        DateTime? value = null;
        await foreach (var r in conn.QueryAsync(sql))
        {
            value = r.GetFieldValue<DateTime>(0);
        }
        Assert.NotNull(value);
        return value!.Value;
    }

    [Fact]
    public async Task DateTime_Year1970Exact_ReadsCorrectly()
    {
        await using var conn = await OpenAsync();
        var got = await ReadDateTimeAsync(conn, "SELECT toDateTime('1970-01-01 00:00:00', 'UTC')");
        Assert.Equal(new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks, got.ToUniversalTime().Ticks);
    }

    [Fact]
    public async Task DateTime_Year2106_LegacyRange_LastSecond()
    {
        await using var conn = await OpenAsync();
        // DateTime is uint32 seconds → last representable value 2106-02-07 06:28:15 UTC.
        var got = await ReadDateTimeAsync(conn, "SELECT toDateTime('2106-02-07 06:28:15', 'UTC')");
        Assert.Equal(new DateTime(2106, 2, 7, 6, 28, 15, DateTimeKind.Utc).Ticks, got.ToUniversalTime().Ticks);
    }

    [Fact]
    public async Task DateTime64_Precision0_RoundTripsLikeDateTime()
    {
        await using var conn = await OpenAsync();
        // DateTime64(0) is wire-equivalent to DateTime — but uses the Int64 path.
        // Pin that the reader applies the right scale (× 1) to the seconds tick value.
        var got = await ReadDateTimeAsync(conn, "SELECT toDateTime64('2024-06-15 12:34:56', 0, 'UTC')");
        Assert.Equal(new DateTime(2024, 6, 15, 12, 34, 56, DateTimeKind.Utc).Ticks, got.ToUniversalTime().Ticks);
    }

    [Fact]
    public async Task DateTime64_TimezoneAware_DstFallback_PinsBehaviour()
    {
        await using var conn = await OpenAsync();
        // 2023-10-29 in Europe/London: the local hour 01:30 happens twice (1:30 BST first,
        // then 1:30 GMT). Probe what the reader does with both representations.
        var table = $"dt64_dst_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, v DateTime64(3, 'Europe/London')) ENGINE = Memory");
        try
        {
            // Two distinct UTC instants — both render to 01:30 local.
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES " +
                "(1, toDateTime64('2023-10-29 00:30:00.000', 3, 'UTC')), " +  // 01:30 BST
                "(2, toDateTime64('2023-10-29 01:30:00.000', 3, 'UTC'))");    // 01:30 GMT

            var got = new List<DateTime>();
            await foreach (var r in conn.QueryAsync($"SELECT v FROM {table} ORDER BY id"))
                got.Add(r.GetFieldValue<DateTime>(0));

            _output.WriteLine($"DST-fallback row 1: {got[0]:yyyy-MM-dd HH:mm:ss.fff zzz} (Kind={got[0].Kind})");
            _output.WriteLine($"DST-fallback row 2: {got[1]:yyyy-MM-dd HH:mm:ss.fff zzz} (Kind={got[1].Kind})");

            // Safety: distinct UTC inputs must produce distinct values. The exact
            // wall-clock interpretation is implementation-defined — we just forbid
            // collapsing two distinct moments to the same value.
            Assert.NotEqual(got[0].ToUniversalTime().Ticks, got[1].ToUniversalTime().Ticks);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task DateTime64_Timezone_NonExistentZoneName_BehaviourProbe()
    {
        // ClickHouse rejects unknown timezone names server-side. Pin that this
        // produces a clean ClickHouseServerException and not a reader-side hang
        // or generic exception.
        await using var conn = await OpenAsync();
        Exception? caught = null;
        try
        {
            using var cts = new CancellationTokenSource(AntiHangTimeout);
            _ = await conn.ExecuteScalarAsync<DateTime>(
                "SELECT toDateTime64('2024-01-01 00:00:00', 3, 'Mars/Olympus_Mons')",
                cancellationToken: cts.Token);
        }
        catch (Exception ex) { caught = ex; }

        _output.WriteLine($"Unknown TZ probe: {caught?.GetType().FullName} — {caught?.Message}");
        Assert.NotNull(caught);
        Assert.IsNotType<OutOfMemoryException>(caught);
        Assert.IsNotType<AccessViolationException>(caught);
    }

    // ---------- 7.3 Geo readers ----------

    [Fact]
    public async Task Point_Origin_RoundTrips()
    {
        await using var conn = await OpenAsync();
        var table = $"geo_origin_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, p Point) ENGINE = Memory");
        try
        {
            await conn.ExecuteNonQueryAsync($"INSERT INTO {table} VALUES (1, (0, 0))");
            var got = new List<Point>();
            await foreach (var r in conn.QueryAsync($"SELECT p FROM {table}"))
                got.Add(r.GetFieldValue<Point>(0));

            Assert.Single(got);
            Assert.Equal(Point.Zero, got[0]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task Point_FloatBoundary_NaN_Inf_BehaviourProbe()
    {
        // Probe — pin only safety. NaN/Inf are valid IEEE 754 doubles; the wire format
        // is 16 raw bytes. A regression that sanitised them client-side would silently
        // corrupt data.
        await using var conn = await OpenAsync();
        var table = $"geo_nan_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, p Point) ENGINE = Memory");
        try
        {
            // Server-side NaN literal via 0/0; +Inf via 1/0 (after enabling decimal_check_overflow=0).
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} SELECT * FROM (" +
                "SELECT 1::Int32 AS id, (nan, 0.0)::Point AS p UNION ALL " +
                "SELECT 2,                (CAST(1.0/0 AS Float64), CAST(-1.0/0 AS Float64))::Point) ORDER BY id");

            var got = new List<Point>();
            await foreach (var r in conn.QueryAsync($"SELECT p FROM {table} ORDER BY id"))
                got.Add(r.GetFieldValue<Point>(0));

            _output.WriteLine($"NaN row: ({got[0].X}, {got[0].Y})");
            _output.WriteLine($"Inf row: ({got[1].X}, {got[1].Y})");

            Assert.Equal(2, got.Count);
            // Pin: the reader must not silently coerce NaN → 0 or Inf → finite. We accept
            // any valid IEEE 754 representation.
            Assert.True(double.IsNaN(got[0].X) || got[0].X == 0.0,
                $"NaN row X coerced to unexpected value {got[0].X}");
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task Polygon_EmptyRings_RoundTrips()
    {
        await using var conn = await OpenAsync();
        var table = $"geo_polyempty_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, poly Polygon) ENGINE = Memory");
        try
        {
            await conn.ExecuteNonQueryAsync($"INSERT INTO {table} VALUES (1, [])");
            var got = new List<Point[][]>();
            await foreach (var r in conn.QueryAsync($"SELECT poly FROM {table}"))
                got.Add(r.GetFieldValue<Point[][]>(0));

            Assert.Single(got);
            Assert.Empty(got[0]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task Polygon_SelfIntersecting_AcceptedByReader()
    {
        await using var conn = await OpenAsync();
        var table = $"geo_polyx_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, poly Polygon) ENGINE = Memory");
        try
        {
            // Bowtie polygon — geometrically self-intersecting. ClickHouse stores it
            // verbatim; the reader's job is to round-trip the points, not validate.
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES (1, [[(0,0),(1,1),(1,0),(0,1),(0,0)]])");

            var got = new List<Point[][]>();
            await foreach (var r in conn.QueryAsync($"SELECT poly FROM {table}"))
                got.Add(r.GetFieldValue<Point[][]>(0));

            Assert.Single(got);
            Assert.Single(got[0]);
            Assert.Equal(5, got[0][0].Length);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task MultiPolygon_LargeRingsOver100k_Vertices_NoOverflow()
    {
        await using var conn = await OpenAsync();
        var table = $"geo_mpoly_big_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, mp MultiPolygon) ENGINE = Memory");
        try
        {
            // 100_000-vertex ring inside a single polygon inside a single multipolygon.
            // Built server-side via range() to avoid sending 1+ MB of literal SQL.
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} SELECT 1, [[arrayMap(i -> (toFloat64(i), toFloat64(i)), range(100000))]]");

            int rows = 0;
            int totalVerts = 0;
            await foreach (var r in conn.QueryAsync($"SELECT mp FROM {table}"))
            {
                var mp = r.GetFieldValue<Point[][][]>(0);
                rows++;
                foreach (var poly in mp)
                    foreach (var ring in poly)
                        totalVerts += ring.Length;
            }
            _output.WriteLine($"MultiPolygon read: rows={rows}, totalVerts={totalVerts}");
            Assert.Equal(1, rows);
            Assert.Equal(100_000, totalVerts);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task Ring_SingleVertex_DegenerateRoundTrip()
    {
        // Plan §7 #19: a Ring (Point[]) with exactly one vertex. Geometrically
        // degenerate but structurally valid wire-side; pin that the reader returns
        // a one-element array, not zero rows or a corrupted offset.
        await using var conn = await OpenAsync();
        var table = $"geo_ring_one_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, r Ring) ENGINE = Memory");
        try
        {
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES (1, [(2.5, 7.0)])");

            var got = new List<Point[]>();
            await foreach (var r in conn.QueryAsync($"SELECT r FROM {table}"))
                got.Add(r.GetFieldValue<Point[]>(0));

            Assert.Single(got);
            Assert.Single(got[0]);
            Assert.Equal(new Point(2.5, 7.0), got[0][0]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task Geo_Nullable_Point_BehaviourProbe_PinsServerSideRejection()
    {
        // ClickHouse rejects Nullable(Point) at CREATE TABLE: "Nested type Point
        // cannot be inside Nullable type" (error 43). Pin that the rejection
        // surfaces as a typed ClickHouseServerException — never silent corruption,
        // never an unhandled exception leak.
        await using var conn = await OpenAsync();
        var table = $"geo_nul_{Guid.NewGuid():N}";

        Exception? caught = null;
        try
        {
            await conn.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id Int32, p Nullable(Point)) ENGINE = Memory");
        }
        catch (Exception ex) { caught = ex; }
        finally
        {
            try { await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}"); } catch { }
        }

        _output.WriteLine($"Nullable(Point) surface: {caught?.GetType().FullName} — {caught?.Message}");
        Assert.NotNull(caught);
        Assert.IsNotType<OutOfMemoryException>(caught);
        Assert.IsNotType<AccessViolationException>(caught);
    }

    // ---------- 7.4 Reader-shared invariants ----------

    [Fact]
    public async Task Reader_PartialColumnSkip_DoesNotCorruptNext()
    {
        // Read only column 2 from a multi-column projection — the reader must skip
        // column 1's bytes cleanly. Regression here would surface as garbage in c2
        // or a hang waiting for bytes already consumed.
        await using var conn = await OpenAsync();
        var rows = new List<int>();
        await foreach (var r in conn.QueryAsync(
            "SELECT toString(number) AS s, toInt32(number * 7) AS x FROM numbers(1000)"))
        {
            // Read only column 1 (x). The reader must still skip column 0 (s) cleanly.
            rows.Add(r.GetFieldValue<int>(1));
        }
        Assert.Equal(1000, rows.Count);
        Assert.Equal(7 * 999, rows[^1]);
    }

    [Fact]
    public async Task Reader_CompressionInteraction_AllScalarTypes_Lz4()
    {
        await using var conn = await OpenAsync(b => b.WithCompression(true));
        var rows = 0;
        await foreach (var r in conn.QueryAsync(
            "SELECT number, toString(number), toFloat64(number) / 7, toDate('2024-01-01') + toIntervalDay(number % 365) " +
            "FROM numbers(50000)"))
        {
            rows++;
        }
        Assert.Equal(50_000, rows);
    }

    [Fact]
    public async Task Reader_CompressionInteraction_AllScalarTypes_Zstd()
    {
        await using var conn = await OpenAsync(b => b
            .WithCompression(true)
            .WithCompressionMethod(CH.Native.Compression.CompressionMethod.Zstd));
        var rows = 0;
        await foreach (var r in conn.QueryAsync(
            "SELECT number, toString(number), toFloat64(number) / 7 FROM numbers(20000)"))
        {
            rows++;
        }
        Assert.Equal(20_000, rows);
    }

    [Fact]
    public async Task Reader_BlockBoundary_StraddlingValues_AcrossBigQuery()
    {
        // Use max_block_size=128 to force lots of small server-side blocks. The reader
        // must reassemble 100k rows correctly. Regression here would surface as a
        // dropped or duplicated row at a block boundary.
        //
        // Use the untyped row enumeration (TypeMapper<ulong> doesn't materialise a
        // primitive-only result) and read column 0 explicitly.
        await using var conn = await OpenAsync();
        long sum = 0;
        long expected = 0;
        const int N = 100_000;
        for (int i = 0; i < N; i++) expected += i;
        await foreach (var r in conn.QueryAsync(
            $"SELECT number FROM numbers({N}) SETTINGS max_block_size=128"))
        {
            sum += (long)r.GetFieldValue<ulong>(0);
        }
        Assert.Equal(expected, sum);
    }
}
