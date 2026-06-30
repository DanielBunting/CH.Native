using System.Collections;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using CH.Native.Data.Geo;
using Dapper;
using Xunit;

namespace CH.Native.Adbc.Tests.Integration;

/// <summary>
/// Parity tests: read the SAME table through the ADBC driver (Arrow) and through Dapper (the
/// classic ADO.NET/row-mapper path) and assert the decoded values agree. This is the cross-check
/// that the new Arrow read path surfaces the same data a long-standing CH.Native+Dapper consumer
/// already gets, rather than only matching a hand-written expectation.
///
/// Both sides are reduced to a culture-invariant canonical string (see <see cref="Canonical"/>) so
/// representation differences that don't change the value — Guid vs. its string form, IPAddress vs.
/// its text, decimal vs. ClickHouseDecimal, a tz-aware DateTimeOffset vs. its instant — compare equal,
/// while a genuine value or precision difference would not.
/// </summary>
[Trait("Category", "Integration")]
[Collection("AdbcClickHouse")]
public class AdbcDapperParityTests : AdbcIntegrationTestBase
{
    public AdbcDapperParityTests(AdbcClickHouseFixture fixture) : base(fixture) { }

    private static string Table() => "adbc_parity_" + Guid.NewGuid().ToString("N");

    /// <summary>
    /// (case name, ClickHouse column type, VALUES literal). Mirrors the scalar matrix the round-trip
    /// tests cover. DateTime columns are pinned to UTC so the instant is server-timezone independent.
    /// </summary>
    public static TheoryData<string, string, string> ScalarCases() => new()
    {
        { "Int8", "Int8", "-8" },
        { "Int16", "Int16", "-16" },
        { "Int32", "Int32", "-32" },
        { "Int64", "Int64", "-64" },
        { "UInt8", "UInt8", "200" },
        { "UInt16", "UInt16", "40000" },
        { "UInt32", "UInt32", "3000000000" },
        { "UInt64", "UInt64", "18446744073709551615" },
        { "Float32", "Float32", "1.5" },
        { "Float64", "Float64", "-2.25" },
        { "Bool", "Bool", "true" },
        { "String", "String", "'hello'" },
        { "FixedString", "FixedString(4)", "'ab'" },
        { "UUID", "UUID", "'11111111-2222-3333-4444-555555555555'" },
        { "IPv4", "IPv4", "'192.168.0.1'" },
        { "IPv6", "IPv6", "'2001:db8::1'" },
        { "Date", "Date", "'2026-06-29'" },
        { "Date32", "Date32", "'2026-06-29'" },
        { "DateTime", "DateTime('UTC')", "'2026-06-29 12:00:00'" },
        { "DateTime64", "DateTime64(3, 'UTC')", "'2026-06-29 12:00:00.123'" },
        { "Decimal32", "Decimal32(2)", "12.34" },
        { "Decimal64", "Decimal64(4)", "12.3456" },
        { "Decimal128", "Decimal128(6)", "12.345678" },
        { "Decimal256", "Decimal256(8)", "12.34567891" },
        // Both tiers surface an enum as its underlying signed integer (-1 here), not the label.
        { "Enum8", "Enum8('a' = -1, 'b' = 2)", "'a'" },
        { "Enum16", "Enum16('x' = -1000, 'y' = 1000)", "'x'" },
        { "BFloat16", "BFloat16", "1.5" },
        { "Time", "Time", "'12:34:56'" },
        { "Time64", "Time64(3)", "'12:34:56.789'" },
        // Wide integers exceed Arrow's 64-bit ceiling; both tiers reduce to exact decimal text. Use
        // the type maxima so a narrowing or precision bug would surface.
        { "Int128", "Int128", "170141183460469231731687303715884105727" },
        { "UInt128", "UInt128", "340282366920938463463374607431768211455" },
        { "Int256", "Int256", "57896044618658097711785492504343953926634992332820282019728792003956564819967" },
        { "UInt256", "UInt256", "115792089237316195423570985008687907853269984665640564039457584007913129639935" },
    };

    [Theory]
    [MemberData(nameof(ScalarCases))]
    public async Task Scalar_AdbcMatchesDapper(string _, string columnType, string valueLiteral)
    {
        var t = Table();
        await ExecuteSetupAsync(
            $"CREATE TABLE {t} (v {columnType}) ENGINE = Memory",
            $"INSERT INTO {t} VALUES ({valueLiteral})");

        await AssertScalarParityAsync($"SELECT v FROM {t}");
    }

    // Types that aren't ordinary storable columns (or are clearest as an expression) are checked the
    // same way, but via a SELECT that aliases the single value as `v`.
    [Theory]
    [InlineData("Nothing", "SELECT NULL AS v")]
    [InlineData("Interval", "SELECT INTERVAL 3 DAY AS v")]
    [InlineData("JSON", "SELECT CAST('{\"a\":1,\"b\":\"x\"}' AS JSON) AS v")]
    [InlineData("Variant", "SELECT CAST('hello' AS Variant(Int32, String)) AS v")]
    [InlineData("Dynamic", "SELECT CAST(42 AS Dynamic) AS v")]
    public async Task Expression_AdbcMatchesDapper(string _, string sql)
    {
        await AssertScalarParityAsync(sql);
    }

    // Composite/nested types. Each canonicalizes to a recursive textual form (see Canonical) on both
    // sides. Array(Tuple(...)) is intentionally excluded: a ClickHouse Tuple and an Array of reference
    // types both decode to object[] in the row-mapper tier, which the generic CLR canonicalizer can't
    // tell apart — so the matrix keeps tuples top-level. Nested has its own structural test below.
    public static TheoryData<string, string, string> CompositeCases() => new()
    {
        { "Array(Int32)", "Array(Int32)", "[1, 2, 3]" },
        { "Array(String)", "Array(String)", "['a', 'b']" },
        { "Array(Nullable)", "Array(Nullable(Int32))", "[1, NULL, 3]" },
        { "Array(Array)", "Array(Array(Int32))", "[[1, 2], [3]]" },
        { "Map", "Map(String, Int32)", "{'a': 1, 'b': 2}" },
        { "Tuple", "Tuple(Int32, String)", "(1, 'x')" },
        { "Point", "Point", "(1.5, 2.5)" },
        { "Ring", "Ring", "[(0, 0), (1, 0), (1, 1)]" },
        { "Polygon", "Polygon", "[[(0, 0), (1, 0), (1, 1)]]" },
        { "MultiPolygon", "MultiPolygon", "[[[(0, 0), (1, 0), (1, 1)]]]" },
    };

    [Theory]
    [MemberData(nameof(CompositeCases))]
    public async Task Composite_AdbcMatchesDapper(string _, string columnType, string valueLiteral)
    {
        var t = Table();
        await ExecuteSetupAsync(
            $"CREATE TABLE {t} (v {columnType}) ENGINE = Memory",
            $"INSERT INTO {t} VALUES ({valueLiteral})");

        await AssertScalarParityAsync($"SELECT v FROM {t}");
    }

    // Nested decodes to List<Struct> in Arrow but to object[] of parallel field arrays in the row
    // mapper, so the generic canonicalizer can't compare them directly. Assert the Arrow shape and
    // values structurally instead — this still proves the driver builds Nested correctly.
    [Fact]
    public async Task Nested_ProducesListOfStructs()
    {
        var t = Table();
        await ExecuteSetupAsync(
            $"CREATE TABLE {t} (n Nested(id UInt32, name String)) ENGINE = Memory",
            $"INSERT INTO {t} VALUES ([1, 2], ['a', 'b'])");

        using var whole = await QueryOneBatchAsync($"SELECT n FROM {t}");
        var list = Assert.IsType<ListArray>(whole.Column(0));
        var entries = Assert.IsType<StructArray>(list.Values);

        Assert.Equal(2, list.GetValueLength(0));
        var ids = Assert.IsType<UInt32Array>(entries.Fields[0]);
        var names = Assert.IsType<StringArray>(entries.Fields[1]);
        Assert.Equal(1u, ids.GetValue(0));
        Assert.Equal(2u, ids.GetValue(1));
        Assert.Equal("a", names.GetString(0));
        Assert.Equal("b", names.GetString(1));
    }

    /// <summary>Runs <paramref name="sql"/> (single column aliased <c>v</c>) through both tiers and compares.</summary>
    private async Task AssertScalarParityAsync(string sql)
    {
        using var batch = await QueryOneBatchAsync(sql);
        var adbc = Canonical.FromArrow(batch.Column(0), 0);

        await using var ch = await OpenClickHouseAsync();
        var row = (IDictionary<string, object?>)await ch.QueryFirstAsync(sql);
        var dapper = Canonical.FromClr(row["v"]);

        Assert.Equal(dapper, adbc);
    }

    /// <summary>
    /// A nullable column over several ordered rows: the full decoded sequence (values and NULLs)
    /// must agree element-for-element between ADBC and Dapper.
    /// </summary>
    [Fact]
    public async Task NullableSequence_AdbcMatchesDapper()
    {
        var t = Table();
        await ExecuteSetupAsync(
            $"CREATE TABLE {t} (id UInt32, v Nullable(Int32)) ENGINE = Memory",
            $"INSERT INTO {t} VALUES (0, 10), (1, NULL), (2, -7), (3, NULL)");

        var sql = $"SELECT v FROM {t} ORDER BY id";

        using var batch = await QueryOneBatchAsync(sql);
        var arrowCol = batch.Column(0);
        var adbc = Enumerable.Range(0, batch.Length).Select(i => Canonical.FromArrow(arrowCol, i)).ToList();

        await using var ch = await OpenClickHouseAsync();
        var rows = (await ch.QueryAsync(sql)).Cast<IDictionary<string, object?>>().ToList();
        var dapper = rows.Select(r => Canonical.FromClr(r["v"])).ToList();

        Assert.Equal(dapper, adbc);
    }

    [Fact]
    public async Task NullableString_AdbcMatchesDapper()
    {
        var t = Table();
        await ExecuteSetupAsync(
            $"CREATE TABLE {t} (id UInt32, v Nullable(String)) ENGINE = Memory",
            $"INSERT INTO {t} VALUES (0, 'x'), (1, NULL), (2, '')");

        var sql = $"SELECT v FROM {t} ORDER BY id";

        using var batch = await QueryOneBatchAsync(sql);
        var arrowCol = batch.Column(0);
        var adbc = Enumerable.Range(0, batch.Length).Select(i => Canonical.FromArrow(arrowCol, i)).ToList();

        await using var ch = await OpenClickHouseAsync();
        var rows = (await ch.QueryAsync(sql)).Cast<IDictionary<string, object?>>().ToList();
        var dapper = rows.Select(r => Canonical.FromClr(r["v"])).ToList();

        Assert.Equal(dapper, adbc);
    }

    /// <summary>
    /// Reduces a value from either tier to a single culture-invariant string. The two tiers hand back
    /// different CLR/Arrow shapes for the same logical value; canonicalizing both removes the
    /// representation difference so only a real divergence fails the comparison.
    /// </summary>
    private static class Canonical
    {
        /// <summary>Canonicalizes the element at <paramref name="i"/> of an Arrow array (the ADBC side).</summary>
        public static string? FromArrow(IArrowArray array, int i)
        {
            if (array is Apache.Arrow.Array a && a.IsNull(i))
                return null;

            return array switch
            {
                NullArray => null,
                // Composite arms first (MapArray derives from ListArray, so it must precede it).
                MapArray m => CanonMap(m, i),
                ListArray l => CanonList(l, i),
                StructArray s => CanonStruct(s, i),
                Int8Array x => FromClr(x.GetValue(i)),
                Int16Array x => FromClr(x.GetValue(i)),
                Int32Array x => FromClr(x.GetValue(i)),
                Int64Array x => FromClr(x.GetValue(i)),
                UInt8Array x => FromClr(x.GetValue(i)),
                UInt16Array x => FromClr(x.GetValue(i)),
                UInt32Array x => FromClr(x.GetValue(i)),
                UInt64Array x => FromClr(x.GetValue(i)),
                FloatArray x => FromClr(x.GetValue(i)),
                DoubleArray x => FromClr(x.GetValue(i)),
                BooleanArray x => FromClr(x.GetValue(i)),
                StringArray x => x.GetString(i),
                BinaryArray x => FromClr(x.GetBytes(i).ToArray()),
                Date32Array x => FromClr(x.GetDateOnly(i)),
                TimestampArray x => FromClr(x.GetTimestamp(i)),
                Time32Array x => FromClr(TimeOnlyFrom(x, i)),
                Time64Array x => FromClr(TimeOnlyFrom(x, i)),
                Decimal128Array x => FromClr(x.GetValue(i)),
                Decimal256Array x => FromClr(x.GetValue(i)),
                _ => throw new NotSupportedException($"No canonicalizer for Arrow array '{array.GetType().Name}'."),
            };
        }

        /// <summary>Canonicalizes a CLR value as surfaced by the CH.Native data reader (the Dapper side).</summary>
        public static string? FromClr(object? value) => value switch
        {
            null => null,
            bool b => b ? "true" : "false",
            byte[] bytes => Convert.ToHexString(bytes),
            Guid g => g.ToString("D"),
            IPAddress ip => ip.ToString(),
            // JSON round-trips as its raw text (the ADBC driver emits GetRawText too).
            JsonDocument j => j.RootElement.GetRawText(),
            TimeOnly time => time.ToString("HH:mm:ss.fffffff", CultureInfo.InvariantCulture),
            DateOnly d => d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            // Pin instants to a single scale (ms) so a tz-aware DateTimeOffset and a UTC DateTime
            // describing the same moment collapse to the same key regardless of CLR shape.
            DateTimeOffset dto => dto.ToUniversalTime().ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture),
            DateTime dt => new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc))
                .ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture),
            float f => f.ToString("R", CultureInfo.InvariantCulture),
            double d => d.ToString("R", CultureInfo.InvariantCulture),
            // Composite CLR shapes — render to the same recursive grammar as the Arrow side.
            IDictionary dict => CanonDict(dict),
            Point p => "(" + FromClr(p.X) + "," + FromClr(p.Y) + ")",
            // A Tuple is a System.Tuple/ValueTuple (ITuple). This must precede IEnumerable, and we must
            // NOT match object[]: reference-type arrays (string[], Point[][]) are object[]-covariant and
            // would be mis-rendered as tuples — they belong on the IEnumerable (list) arm.
            ITuple tuple => "(" + string.Join(",", Enumerable.Range(0, tuple.Length)
                .Select(f => FromClr(tuple[f]) ?? "null")) + ")",
            string str => str,
            IEnumerable seq => "[" + string.Join(",", seq.Cast<object?>().Select(e => FromClr(e) ?? "null")) + "]",
            IFormattable fmt => fmt.ToString(null, CultureInfo.InvariantCulture),
            _ => value.ToString(),
        };

        private static string CanonList(ListArray l, int i)
        {
            int start = l.ValueOffsets[i], len = l.GetValueLength(i);
            var parts = new string[len];
            for (int k = 0; k < len; k++) parts[k] = FromArrow(l.Values, start + k) ?? "null";
            return "[" + string.Join(",", parts) + "]";
        }

        private static string CanonStruct(StructArray s, int i)
        {
            var parts = new string[s.Fields.Count];
            for (int f = 0; f < s.Fields.Count; f++) parts[f] = FromArrow(s.Fields[f], i) ?? "null";
            return "(" + string.Join(",", parts) + ")";
        }

        private static string CanonMap(MapArray m, int i)
        {
            // MapArray surfaces the flattened key/value child arrays directly (not the entries struct).
            var keys = m.Keys;
            var values = m.Values;
            int start = m.ValueOffsets[i], len = m.GetValueLength(i);
            var pairs = new List<string>(len);
            for (int k = 0; k < len; k++)
                pairs.Add((FromArrow(keys, start + k) ?? "null") + "=" + (FromArrow(values, start + k) ?? "null"));
            pairs.Sort(StringComparer.Ordinal);
            return "{" + string.Join(",", pairs) + "}";
        }

        // Map entry order isn't guaranteed across the two tiers, so sort entries before comparing.
        private static string CanonDict(IDictionary d)
        {
            var pairs = new List<string>(d.Count);
            foreach (DictionaryEntry e in d)
                pairs.Add((FromClr(e.Key) ?? "null") + "=" + (FromClr(e.Value) ?? "null"));
            pairs.Sort(StringComparer.Ordinal);
            return "{" + string.Join(",", pairs) + "}";
        }

        // Rebuild a TimeOnly from an Arrow time array so it canonicalizes identically to Dapper's TimeOnly.
        private static TimeOnly TimeOnlyFrom(Time32Array a, int i)
        {
            long perUnit = ((Time32Type)a.Data.DataType).Unit == TimeUnit.Second
                ? TimeSpan.TicksPerSecond : TimeSpan.TicksPerMillisecond;
            return new TimeOnly(a.GetValue(i)!.Value * perUnit);
        }

        private static TimeOnly TimeOnlyFrom(Time64Array a, int i)
        {
            long raw = a.GetValue(i)!.Value;
            long ticks = ((Time64Type)a.Data.DataType).Unit == TimeUnit.Microsecond ? raw * 10 : raw / 100;
            return new TimeOnly(ticks);
        }
    }
}
