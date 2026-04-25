using CH.Native.BulkInsert;
using CH.Native.Compression;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Allocation;

/// <summary>
/// Allocation regression tests. Each test exercises a hot path against a real ClickHouse
/// instance, measures bytes allocated on the calling thread, and compares against a
/// checked-in baseline (with a tolerance band). Re-record with CHNATIVE_ALLOC_RECORD=1
/// when an intentional change moves the budget.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Allocation)]
public class AllocationBudgetTests
{
    private readonly SingleNodeFixture _fixture;
    private readonly AllocationBudget _budget = AllocationBudget.Load();

    public AllocationBudgetTests(SingleNodeFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task ExecuteScalar_Int_StaysWithinBudget()
    {
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        // Warm caches (mappers, JIT) before measurement.
        _ = await conn.ExecuteScalarAsync<int>("SELECT 1");

        var bytes = await AllocationProbe.MeasureAsync(async () =>
        {
            for (int i = 0; i < 100; i++)
                _ = await conn.ExecuteScalarAsync<int>("SELECT 1");
        });

        _budget.Assert("ExecuteScalar.Int.x100", bytes);
    }

    [Fact]
    public async Task QueryAsync_StreamRows_StaysWithinBudget()
    {
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        const string sql = "SELECT number, toString(number) FROM numbers(10000)";

        // Warm.
        await foreach (var _ in conn.QueryAsync(sql)) { }

        var bytes = await AllocationProbe.MeasureAsync(async () =>
        {
            await foreach (var row in conn.QueryAsync(sql))
            {
                _ = row.GetFieldValue<ulong>(0);
                _ = row.GetFieldValue<string>(1);
            }
        });

        _budget.Assert("QueryAsync.NumbersAndStrings.10k", bytes);
    }

    [Fact]
    public async Task ExecuteNonQuery_NoOp_StaysWithinBudget()
    {
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        _ = await conn.ExecuteNonQueryAsync("SELECT 1");

        var bytes = await AllocationProbe.MeasureAsync(async () =>
        {
            for (int i = 0; i < 100; i++)
                _ = await conn.ExecuteNonQueryAsync("SELECT 1");
        });

        _budget.Assert("ExecuteNonQuery.Trivial.x100", bytes);
    }

    [Fact]
    public async Task ExecuteScalar_String_StaysWithinBudget()
    {
        // String materialization has a different allocation profile than int —
        // pin it so a regression in lazy-vs-eager string handling is visible.
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        _ = await conn.ExecuteScalarAsync<string>("SELECT 'hello'");

        var bytes = await AllocationProbe.MeasureAsync(async () =>
        {
            for (int i = 0; i < 100; i++)
                _ = await conn.ExecuteScalarAsync<string>("SELECT 'hello'");
        });

        _budget.Assert("ExecuteScalar.String.x100", bytes);
    }

    [Fact]
    public async Task BulkInsert_x10k_StaysWithinBudget()
    {
        // Bulk insert is the perf-headline path. Fills a missing budget.
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        var table = $"alloc_bi_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, name String) ENGINE = Memory");

        async Task RunAsync()
        {
            await using var ins = conn.CreateBulkInserter<Row>(table,
                new BulkInsertOptions { BatchSize = 1000 });
            await ins.InitAsync();
            for (int i = 0; i < 10_000; i++)
                await ins.AddAsync(new Row { Id = i, Name = "v" });
            await ins.CompleteAsync();
            await conn.ExecuteNonQueryAsync($"TRUNCATE TABLE {table}");
        }

        try
        {
            // Warm.
            await RunAsync();

            var bytes = await AllocationProbe.MeasureAsync(RunAsync);
            _budget.Assert("BulkInsert.10k", bytes);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task QueryAsync_Lz4Compressed_StaysWithinBudget()
    {
        // Compression toggles a different code path with its own buffer handling.
        var settings = _fixture.BuildSettings(b =>
        {
            b.WithCompression(true);
            b.WithCompressionMethod(CompressionMethod.Lz4);
        });
        await using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();

        const string sql = "SELECT number, toString(number) FROM numbers(10000)";

        // Warm.
        await foreach (var _ in conn.QueryAsync(sql)) { }

        var bytes = await AllocationProbe.MeasureAsync(async () =>
        {
            await foreach (var row in conn.QueryAsync(sql))
            {
                _ = row.GetFieldValue<ulong>(0);
                _ = row.GetFieldValue<string>(1);
            }
        });

        _budget.Assert("QueryAsync.Lz4.NumbersAndStrings.10k", bytes);
    }

    private class Row
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "name", Order = 1)] public string Name { get; set; } = "";
    }
}
