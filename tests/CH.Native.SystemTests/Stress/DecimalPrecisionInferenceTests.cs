using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Stress;

/// <summary>
/// Pins the decimal-precision inference behaviour described in use-cases §5.5.
/// Decimal precision on the wire is inferred from the <i>value</i>, not the
/// CLR type. For columns wider than the inferred type, the explicit
/// <see cref="ClickHouseColumnAttribute.ClickHouseType"/> override is the fix.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Stress)]
public class DecimalPrecisionInferenceTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public DecimalPrecisionInferenceTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task SmallDecimalIntoDecimal128_WithExplicitTypeAttribute_Succeeds()
    {
        // Setup: target column is wide (Decimal128(18)), POCO uses
        // [ClickHouseColumn(ClickHouseType="Decimal128(18)")] to force the
        // wire type.
        var table = $"dec_explicit_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, amount Decimal128(18)) ENGINE = MergeTree ORDER BY id");
        try
        {
            await using var inserter = conn.CreateBulkInserter<DecimalRowExplicit>(table);
            await inserter.InitAsync();
            await inserter.AddAsync(new DecimalRowExplicit { Id = 1, Amount = 12.34m });
            await inserter.CompleteAsync();

            var sum = await conn.ExecuteScalarAsync<decimal>($"SELECT sum(amount) FROM {table}");
            Assert.Equal(12.34m, sum);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task SmallDecimalIntoNarrowColumn_RoundTripsWithoutAttribute()
    {
        // The simple case: column type matches what the inferred wire
        // type would represent. No attribute needed.
        var table = $"dec_narrow_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, amount Decimal64(2)) ENGINE = MergeTree ORDER BY id");
        try
        {
            await using var inserter = conn.CreateBulkInserter<DecimalRowInferred>(table);
            await inserter.InitAsync();
            await inserter.AddAsync(new DecimalRowInferred { Id = 1, Amount = 12.34m });
            await inserter.CompleteAsync();

            var sum = await conn.ExecuteScalarAsync<decimal>($"SELECT sum(amount) FROM {table}");
            Assert.Equal(12.34m, sum);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    internal sealed class DecimalRowInferred
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "amount", Order = 1)] public decimal Amount { get; set; }
    }

    internal sealed class DecimalRowExplicit
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }

        [ClickHouseColumn(Name = "amount", Order = 1, ClickHouseType = "Decimal128(18)")]
        public decimal Amount { get; set; }
    }
}
