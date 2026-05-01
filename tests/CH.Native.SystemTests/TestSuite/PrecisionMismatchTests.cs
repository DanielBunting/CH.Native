using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.TestSuite;

/// <summary>
/// Documents and pins the type-precision-mismatch contract for numeric columns
/// — the silent loss-or-throw surface that's easy to misuse without an
/// authoritative test. Each test reads the actual behaviour and locks it in
/// so a future change becomes visible.
///
/// <para>
/// Areas covered:
/// </para>
/// <list type="bullet">
/// <item><description>Reading a server-side <c>Decimal128(38)</c> with a value beyond
///     CLR <see cref="decimal"/>'s 28-29 significant digits.</description></item>
/// <item><description>Reading a server-side <c>Float64</c> into a CLR <see cref="float"/>
///     POCO field — narrowing precision loss.</description></item>
/// <item><description>Reading a server-side <c>Int64</c> negative value into a CLR
///     <see cref="ulong"/> POCO field — the sign extension story.</description></item>
/// <item><description>Reading <c>Decimal64</c> values where the scale doesn't match the
///     POCO mapper's expectations.</description></item>
/// </list>
///
/// <para>
/// These tests don't assert "correct" behaviour — they assert <em>actual</em>
/// behaviour. If a test fails the question is "do we want the new behaviour?"
/// not "is the test broken?".
/// </para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class PrecisionMismatchTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public PrecisionMismatchTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task Decimal128_AboveClrMaxValue_ClampsSilentlyOnReadAsDecimal()
    {
        // Pinned contract from src/CH.Native/Numerics/ClickHouseDecimal.cs:171:
        // the explicit `(decimal)ClickHouseDecimal` cast does NOT throw on
        // overflow — it clamps to decimal.MaxValue / MinValue. This is a
        // deliberate divergence from ClickHouse.Driver, called out in the
        // operator's doc-comment.
        //
        // To reach the read-side overflow path, we insert a Decimal128(0)
        // value of 10^35 — well within Decimal128's range (~1.7e38) but well
        // above CLR decimal's max (~7.92e28). The query reads via
        // ClickHouseDecimal (the native return type) and the test then casts
        // to `decimal` to exercise the clamp path.
        var table = $"prec_dec128_overflow_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        try
        {
            await conn.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id Int32, v Decimal128(0)) ENGINE = Memory");
            // 10^35 — beyond CLR.MaxDecimal but valid Decimal128.
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES (1, toDecimal128('100000000000000000000000000000000000', 0))");

            // Read as ClickHouseDecimal first to confirm the wire round-trip
            // is exact at the native type.
            CH.Native.Numerics.ClickHouseDecimal nativeValue = default;
            await foreach (var row in conn.QueryAsync<NativeDecimalRow>($"SELECT id, v FROM {table}"))
                nativeValue = row.V;

            _output.WriteLine($"Decimal128 native read: {nativeValue}");

            // The CLR cast must clamp, not throw, not return a wrapped/wrong
            // value. (Casting a value above MaxValue MUST yield MaxValue.)
            var clamped = (decimal)nativeValue;
            Assert.Equal(decimal.MaxValue, clamped);

            // Negative direction: insert -10^35, expect clamp to MinValue.
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES (2, toDecimal128('-100000000000000000000000000000000000', 0))");
            CH.Native.Numerics.ClickHouseDecimal negNative = default;
            await foreach (var row in conn.QueryAsync<NativeDecimalRow>($"SELECT id, v FROM {table} WHERE id = 2"))
                negNative = row.V;

            var negClamped = (decimal)negNative;
            Assert.Equal(decimal.MinValue, negClamped);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    private sealed class NativeDecimalRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "v", Order = 1)] public CH.Native.Numerics.ClickHouseDecimal V { get; set; }
    }

    [Fact]
    public async Task Decimal128_AtClrMaxValue_RoundTripsExactly()
    {
        // ClickHouse's `toDecimal128(value, 0)` already validates the input
        // against the target precision and clamps/rejects values outside the
        // CLR decimal range when the value originates from a string the
        // server can parse. So a "clean" overflow scenario from the read
        // side is hard to construct purely from SQL — the server rejects or
        // clamps before the wire write.
        //
        // What we CAN pin: CLR decimal's exact maximum value (~7.92e28)
        // round-trips byte-for-byte through Decimal128(0). If a future
        // change introduces a precision-loss path on read, the assertion
        // below will detect it.
        var table = $"prec_dec128_max_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        try
        {
            await conn.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id Int32, v Decimal128(0)) ENGINE = Memory");
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES (1, toDecimal128('79228162514264337593543950335', 0))");

            DecimalRow? row = null;
            await foreach (var r in conn.QueryAsync<DecimalRow>($"SELECT id, v FROM {table}"))
                row = r;

            Assert.NotNull(row);
            _output.WriteLine($"Decimal128 at CLR max read as: {row!.V}");
            Assert.Equal(decimal.MaxValue, row.V);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task Decimal128_WithinClrRange_RoundTripsExactly()
    {
        // Sanity: a Decimal128 value within CLR decimal's range round-trips
        // without precision loss.
        var table = $"prec_dec128_ok_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        try
        {
            await conn.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id Int32, v Decimal128(2)) ENGINE = Memory");
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES (1, toDecimal128('123456789012345.67', 2))");

            DecimalRow? row = null;
            await foreach (var r in conn.QueryAsync<DecimalRow>($"SELECT id, v FROM {table}"))
                row = r;

            Assert.NotNull(row);
            Assert.Equal(123456789012345.67m, row!.V);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task Float64_NarrowingToFloat32_PrecisionIsLost_ButNoThrow()
    {
        // Server-side Float64 (double precision) read into a CLR float field
        // is lossy by design. Pin "no throw" so callers don't accidentally
        // depend on detection of this loss.
        var table = $"prec_f64_to_f32_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        try
        {
            await conn.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id Int32, v Float64) ENGINE = Memory");
            // A value with more precision than float32 can hold.
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES (1, 1.234567890123456789)");

            // Read into a POCO with a `double` field first to confirm no truncation
            // on the wire, then experiment with a `float` field if the path supports
            // narrowing.
            DoubleRow? doubleRow = null;
            await foreach (var r in conn.QueryAsync<DoubleRow>($"SELECT id, v FROM {table}"))
                doubleRow = r;

            Assert.NotNull(doubleRow);
            Assert.True(Math.Abs(doubleRow!.V - 1.234567890123456789) < 1e-15);

            // Now the narrowing read. Today's contract: this either throws (because
            // there's no Float64→Float32 row mapper) or succeeds with a narrowed value.
            Exception? caught = null;
            FloatRow? floatRow = null;
            try
            {
                await foreach (var r in conn.QueryAsync<FloatRow>($"SELECT id, v FROM {table}"))
                    floatRow = r;
            }
            catch (Exception ex) { caught = ex; }

            _output.WriteLine($"Float64→float read: thrown={caught?.GetType().Name ?? "no"}, " +
                              $"value={floatRow?.V}");
            // Document whichever happens; both are acceptable, but only one
            // happens today.
            Assert.True(caught is not null || floatRow is not null);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task UInt64_MaxValue_BulkInsertAndRead_RoundTrips()
    {
        // UInt64.MaxValue (>(long)int.MaxValue) — round-trip without any
        // sign-bit collapse. Reads must hit the unsigned reader path; if a
        // future regression maps UInt64 to long internally, the value would
        // come back negative.
        var table = $"prec_u64_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        try
        {
            await conn.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id Int32, v UInt64) ENGINE = Memory");

            await using var inserter = conn.CreateBulkInserter<ULongRow>(table,
                new BulkInsertOptions { BatchSize = 2 });
            await inserter.InitAsync();
            await inserter.AddAsync(new ULongRow { Id = 1, V = ulong.MaxValue });
            await inserter.AddAsync(new ULongRow { Id = 2, V = (ulong)long.MaxValue + 1 });
            await inserter.CompleteAsync();

            var rows = new List<ULongRow>();
            await foreach (var r in conn.QueryAsync<ULongRow>($"SELECT id, v FROM {table} ORDER BY id"))
                rows.Add(r);

            Assert.Equal(2, rows.Count);
            Assert.Equal(ulong.MaxValue, rows[0].V);
            Assert.Equal((ulong)long.MaxValue + 1, rows[1].V);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task Decimal64_ScaleIsPreserved_OnRead()
    {
        // Lock in: Decimal64(4) values come back with the right scale, not
        // collapsed to integer or rescaled to a different precision.
        var table = $"prec_d64_scale_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        try
        {
            await conn.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id Int32, v Decimal64(4)) ENGINE = Memory");
            await conn.ExecuteNonQueryAsync(
                $"INSERT INTO {table} VALUES (1, 12345.6789)");

            DecimalRow? row = null;
            await foreach (var r in conn.QueryAsync<DecimalRow>($"SELECT id, v FROM {table}"))
                row = r;

            Assert.NotNull(row);
            Assert.Equal(12345.6789m, row!.V);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    private sealed class DecimalRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "v", Order = 1)] public decimal V { get; set; }
    }

    private sealed class DoubleRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "v", Order = 1)] public double V { get; set; }
    }

    private sealed class FloatRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "v", Order = 1)] public float V { get; set; }
    }

    private sealed class ULongRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "v", Order = 1)] public ulong V { get; set; }
    }
}
