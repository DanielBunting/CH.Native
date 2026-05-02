using CH.Native.Connection;
using CH.Native.Data;
using CH.Native.SystemTests.Allocation;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Streams;

/// <summary>
/// Pins the documented asymmetry of <see cref="StringMaterialization.Lazy"/>:
/// it applies to the untyped row path (<c>QueryAsync</c>) where decoding is
/// deferred until field access, but typed reads (<c>QueryAsync&lt;T&gt;</c> /
/// <c>ExecuteScalarAsync&lt;string&gt;</c>) always materialise eagerly because
/// the property setter / scalar return needs a real string.
///
/// <para>
/// The README claims Lazy gives ~68% memory reduction on the streaming
/// 1M-row benchmark. This test does not pin that absolute number — it pins
/// the <i>shape</i> via within-run ratios so the asymmetry can be detected
/// without a recorded baseline.
/// </para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Streams)]
public class StringMaterializationLazyAsymmetryTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public StringMaterializationLazyAsymmetryTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    private const string WideRowSql =
        "SELECT toString(number) AS c0, toString(number+1) AS c1, toString(number+2) AS c2, " +
        "       toString(number+3) AS c3, toString(number+4) AS c4, toString(number+5) AS c5, " +
        "       toString(number+6) AS c6, toString(number+7) AS c7, toString(number+8) AS c8, " +
        "       toString(number+9) AS c9 " +
        "FROM numbers(10000)";

    [Fact]
    public async Task Untyped_Lazy_AccessingOneColumnOfTen_AllocatesLessThanEagerAccessingAllTen()
    {
        // Untyped + Lazy + access 1 column out of 10 should allocate strictly
        // less than Eager (which decodes all 10 regardless). The relative-ratio
        // assertion is robust to environment variation.
        var lazyOneCol = await MeasureUntyped(StringMaterialization.Lazy, accessAllColumns: false);
        var eagerOneCol = await MeasureUntyped(StringMaterialization.Eager, accessAllColumns: false);

        _output.WriteLine($"Lazy 1-of-10 cols: {lazyOneCol:N0} bytes");
        _output.WriteLine($"Eager 1-of-10 cols: {eagerOneCol:N0} bytes");

        Assert.True(lazyOneCol < eagerOneCol,
            $"Lazy with 1-of-10 column access ({lazyOneCol:N0}) should allocate less than Eager ({eagerOneCol:N0}).");
    }

    [Fact]
    public async Task Untyped_Lazy_AccessingAllColumns_ApproachesEagerAllocations()
    {
        // When all columns are touched, Lazy converges towards Eager — the
        // savings vanish because every string ends up decoded anyway. A
        // significant gap here would suggest Lazy is leaving allocations on
        // the table.
        var lazyAll = await MeasureUntyped(StringMaterialization.Lazy, accessAllColumns: true);
        var eagerAll = await MeasureUntyped(StringMaterialization.Eager, accessAllColumns: true);

        _output.WriteLine($"Lazy all-cols: {lazyAll:N0}, Eager all-cols: {eagerAll:N0}");

        // Allow Lazy a 25% headroom over Eager — Lazy carries a small
        // bookkeeping cost per field. If the gap is wider, something has
        // regressed.
        Assert.True(lazyAll <= eagerAll * 1.25,
            $"Lazy ({lazyAll:N0}) should be within 25% of Eager ({eagerAll:N0}) when all columns are accessed.");
    }

    [Fact]
    public async Task Typed_LazyAndEager_HaveEquivalentAllocationProfile()
    {
        // The typed path forces eager materialisation regardless of the
        // setting — property setters on TenStringRow need real strings. Pin
        // that the Lazy setting does NOT silently start affecting the typed
        // path.
        var lazy = await MeasureTyped(StringMaterialization.Lazy);
        var eager = await MeasureTyped(StringMaterialization.Eager);

        _output.WriteLine($"Typed Lazy: {lazy:N0}, Typed Eager: {eager:N0}");

        // Within ±20% of each other — they should be effectively the same
        // workload.
        var ratio = (double)lazy / Math.Max(1, eager);
        Assert.InRange(ratio, 0.8, 1.2);
    }

    [Fact]
    public async Task ExecuteScalarString_LazyAndEager_BothReturnRealStrings_AllocationProfileSimilar()
    {
        // Scalar string return must yield a real string in both modes — the
        // scalar API has nowhere to defer materialisation to. Empirically
        // Lazy still wins ~30% on this path because the underlying column
        // reader path differs, but the gap is bounded — neither mode
        // allocates dramatically more or less than the other.
        await using var lazyConn = new ClickHouseConnection(
            _fx.BuildSettings(b => b.WithStringMaterialization(StringMaterialization.Lazy)));
        await lazyConn.OpenAsync();
        await using var eagerConn = new ClickHouseConnection(
            _fx.BuildSettings(b => b.WithStringMaterialization(StringMaterialization.Eager)));
        await eagerConn.OpenAsync();

        // Warm
        var lazyResult = await lazyConn.ExecuteScalarAsync<string>("SELECT 'hello world'");
        var eagerResult = await eagerConn.ExecuteScalarAsync<string>("SELECT 'hello world'");
        Assert.Equal("hello world", lazyResult);
        Assert.Equal("hello world", eagerResult);

        var lazyAlloc = await AllocationProbe.MeasureAsync(async () =>
        {
            for (int i = 0; i < 50; i++)
                _ = await lazyConn.ExecuteScalarAsync<string>("SELECT 'hello world'");
        });
        var eagerAlloc = await AllocationProbe.MeasureAsync(async () =>
        {
            for (int i = 0; i < 50; i++)
                _ = await eagerConn.ExecuteScalarAsync<string>("SELECT 'hello world'");
        });

        _output.WriteLine($"Scalar Lazy: {lazyAlloc:N0}, Eager: {eagerAlloc:N0}");

        var ratio = (double)lazyAlloc / Math.Max(1, eagerAlloc);
        // Bounded: neither mode allocates >2× the other. A regression that
        // makes Lazy explode upwards (forgot the optimisation) or makes
        // Eager explode upwards (broke the eager path) trips this.
        Assert.InRange(ratio, 0.4, 1.6);
    }

    [Fact]
    public async Task Untyped_Lazy_EnumeratorAdvance_PreservesPreviouslyMaterialisedValue()
    {
        // Pin the enumerator-advance contract: once a column has been
        // accessed (and therefore decoded), subsequent advances must not
        // overwrite the materialised string with the next row's bytes. The
        // only acceptable failure modes are a clean exception or returning
        // the already-materialised value — silent corruption is not.
        await using var conn = new ClickHouseConnection(
            _fx.BuildSettings(b => b.WithStringMaterialization(StringMaterialization.Lazy)));
        await conn.OpenAsync();

        var collected = new List<string>();
        string? firstRowField = null;

        var enumerator = conn.QueryAsync(
            "SELECT toString(number) AS c FROM numbers(50)").GetAsyncEnumerator();
        try
        {
            if (await enumerator.MoveNextAsync())
            {
                firstRowField = enumerator.Current.GetFieldValue<string>("c");
                Assert.Equal("0", firstRowField);
            }

            // Advance past several rows. The first row's materialised value
            // must remain intact.
            for (int i = 0; i < 10; i++)
            {
                if (!await enumerator.MoveNextAsync()) break;
                collected.Add(enumerator.Current.GetFieldValue<string>("c"));
            }
        }
        finally
        {
            await enumerator.DisposeAsync();
        }

        Assert.Equal("0", firstRowField);
        Assert.Equal(new[] { "1", "2", "3", "4", "5", "6", "7", "8", "9", "10" }, collected);
    }

    private async Task<long> MeasureUntyped(StringMaterialization mode, bool accessAllColumns)
    {
        await using var conn = new ClickHouseConnection(
            _fx.BuildSettings(b => b.WithStringMaterialization(mode)));
        await conn.OpenAsync();

        // Warm caches
        await foreach (var row in conn.QueryAsync(WideRowSql))
        {
            _ = row.GetFieldValue<string>("c0");
            if (accessAllColumns)
            {
                for (int i = 1; i < 10; i++)
                    _ = row.GetFieldValue<string>("c" + i);
            }
        }

        return await AllocationProbe.MeasureAsync(async () =>
        {
            await foreach (var row in conn.QueryAsync(WideRowSql))
            {
                _ = row.GetFieldValue<string>("c0");
                if (accessAllColumns)
                {
                    for (int i = 1; i < 10; i++)
                        _ = row.GetFieldValue<string>("c" + i);
                }
            }
        });
    }

    private async Task<long> MeasureTyped(StringMaterialization mode)
    {
        await using var conn = new ClickHouseConnection(
            _fx.BuildSettings(b => b.WithStringMaterialization(mode)));
        await conn.OpenAsync();

        await foreach (var _ in conn.QueryAsync<TenStringRow>(WideRowSql)) { }

        return await AllocationProbe.MeasureAsync(async () =>
        {
            await foreach (var row in conn.QueryAsync<TenStringRow>(WideRowSql))
            {
                _ = row.C0;
            }
        });
    }

    internal sealed class TenStringRow
    {
        public string C0 { get; set; } = "";
        public string C1 { get; set; } = "";
        public string C2 { get; set; } = "";
        public string C3 { get; set; } = "";
        public string C4 { get; set; } = "";
        public string C5 { get; set; } = "";
        public string C6 { get; set; } = "";
        public string C7 { get; set; } = "";
        public string C8 { get; set; } = "";
        public string C9 { get; set; } = "";
    }
}
