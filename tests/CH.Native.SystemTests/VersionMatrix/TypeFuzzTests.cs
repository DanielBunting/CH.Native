using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.VersionMatrix;

/// <summary>
/// Deterministic, seeded "fuzz" of representative type round-trips replayed across the
/// version matrix. Catches reader/writer divergence and per-version wire-format drift
/// without the maintenance burden of a full property-based generator.
/// </summary>
[Collection("VersionMatrix")]
[Trait(Categories.Name, Categories.VersionMatrix)]
public class TypeFuzzTests
{
    private readonly VersionedNodeCache _cache;

    public TypeFuzzTests(VersionedNodeCache cache)
    {
        _cache = cache;
    }

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task DecimalAndDateTime_RoundTrip(string image)
    {
        var settings = await _cache.GetSettingsAsync(image);
        await using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();

        var table = $"fuzz_decdt_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, " +
            "d32 Decimal(9,4), d64 Decimal(18,6), " +
            "ts DateTime64(6, 'UTC')) ENGINE = Memory");
        try
        {
            var rng = new Random(42); // deterministic
            var rows = new List<DecRow>();
            for (int i = 0; i < 50; i++)
            {
                // Build exact decimals from integer mantissa to avoid binary-float rounding
                // ambiguity at the column's declared scale.
                var d32 = new decimal(rng.Next(0, 10_000_000), 0, 0, false, scale: 4); // up to 999.9999
                var d64 = new decimal(rng.Next(0, 1_000_000_000), 0, 0, false, scale: 6); // up to 999.999999
                rows.Add(new DecRow
                {
                    Id = i,
                    D32 = d32,
                    D64 = d64,
                    Ts = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddSeconds(rng.Next(0, 86400)),
                });
            }

            await using (var ins = conn.CreateBulkInserter<DecRow>(table))
            {
                await ins.InitAsync();
                foreach (var r in rows) await ins.AddAsync(r);
                await ins.CompleteAsync();
            }

            var got = new List<DecRow>();
            await foreach (var r in conn.QueryAsync($"SELECT id, d32, d64, ts FROM {table} ORDER BY id"))
            {
                got.Add(new DecRow
                {
                    Id = r.GetFieldValue<int>(0),
                    D32 = r.GetFieldValue<decimal>(1),
                    D64 = r.GetFieldValue<decimal>(2),
                    Ts = r.GetFieldValue<DateTime>(3),
                });
            }
            Assert.Equal(rows.Count, got.Count);
            for (int i = 0; i < rows.Count; i++)
            {
                Assert.Equal(rows[i].Id, got[i].Id);
                Assert.Equal(rows[i].D32, got[i].D32);
                Assert.Equal(rows[i].D64, got[i].D64);
                var tsExpected = rows[i].Ts.ToUniversalTime();
                var tsGot = got[i].Ts.Kind == DateTimeKind.Utc ? got[i].Ts : got[i].Ts.ToUniversalTime();
                Assert.True(Math.Abs((tsExpected - tsGot).Ticks) < 100);
            }
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Theory]
    [MemberData(nameof(SupportedImages.All), MemberType = typeof(SupportedImages))]
    public async Task NestedArrays_RoundTrip(string image)
    {
        var settings = await _cache.GetSettingsAsync(image);
        await using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();

        await foreach (var r in conn.QueryAsync(
            "SELECT cast([[1,2],[3,4,5],[]] AS Array(Array(Int32)))"))
        {
            var outer = r.GetFieldValue<int[][]>(0);
            Assert.Equal(3, outer.Length);
            Assert.Equal(new[] { 1, 2 }, outer[0]);
            Assert.Equal(new[] { 3, 4, 5 }, outer[1]);
            Assert.Empty(outer[2]);
        }
    }

    private class DecRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "d32", Order = 1)] public decimal D32 { get; set; }
        [ClickHouseColumn(Name = "d64", Order = 2)] public decimal D64 { get; set; }
        [ClickHouseColumn(Name = "ts", Order = 3)] public DateTime Ts { get; set; }
    }
}
