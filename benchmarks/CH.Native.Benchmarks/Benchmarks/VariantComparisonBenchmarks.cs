using BenchmarkDotNet.Attributes;
using CH.Native.Benchmarks.Infrastructure;
using CH.Native.BulkInsert;
using CH.Native.Data.Variant;
using CH.Native.Mapping;
using NativeConnection = CH.Native.Connection.ClickHouseConnection;
using DriverConnection = ClickHouse.Driver.ADO.ClickHouseConnection;

namespace CH.Native.Benchmarks.Benchmarks;

/// <summary>
/// End-to-end Variant(Int64, String) round-trip benchmarks comparing CH.Native (native TCP)
/// against ClickHouse.Driver (HTTP). Both drivers hit the same ClickHouse server, so the
/// numbers capture protocol + serialization + CLR-mapping cost combined.
///
/// CH.Native uses the new <see cref="ClickHouseVariant"/> struct.
/// ClickHouse.Driver exposes Variant as <c>object</c> and requires CAST expressions on insert.
/// </summary>
[Config(typeof(ProtocolComparisonConfig))]
public class VariantComparisonBenchmarks
{
    private const string VariantTable = "bench_variant";

    private string _nativeConnectionString = null!;
    private string _driverConnectionString = null!;

    private VariantRow[] _nativeRows = null!;
    private object?[][] _driverRows = null!;

    [Params(1_000, 10_000, 100_000)]
    public int RowCount { get; set; }

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        await BenchmarkContainerManager.Instance.EnsureInitializedAsync();
        var manager = BenchmarkContainerManager.Instance;
        _nativeConnectionString = manager.NativeConnectionString;
        _driverConnectionString = manager.DriverConnectionString;

        await using var connection = new NativeConnection(_nativeConnectionString);
        await connection.OpenAsync();

        // ClickHouse requires an explicit flag to create Variant columns.
        await connection.ExecuteNonQueryAsync("SET allow_experimental_variant_type = 1");
        await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {VariantTable}");
        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {VariantTable} (id Int32, v Variant(Int64, String)) ENGINE = Memory " +
            "SETTINGS allow_experimental_variant_type = 1");

        var rng = new Random(42);
        _nativeRows = new VariantRow[RowCount];
        _driverRows = new object?[RowCount][];

        for (int i = 0; i < RowCount; i++)
        {
            var roll = rng.Next(3);
            ClickHouseVariant nativeValue;
            object? driverValue;
            switch (roll)
            {
                case 0:
                    nativeValue = ClickHouseVariant.Null;
                    driverValue = null;
                    break;
                case 1:
                    var i64 = (long)rng.Next();
                    nativeValue = new ClickHouseVariant(0, i64);
                    driverValue = i64;
                    break;
                default:
                    var s = $"s{rng.Next():X}";
                    nativeValue = new ClickHouseVariant(1, s);
                    driverValue = s;
                    break;
            }

            _nativeRows[i] = new VariantRow { Id = i, V = nativeValue };
            _driverRows[i] = new object?[] { i, driverValue };
        }

        // Seed the table once for SELECT benchmarks.
        await connection.BulkInsertAsync(VariantTable, _nativeRows);
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        try
        {
            await using var connection = new NativeConnection(_nativeConnectionString);
            await connection.OpenAsync();
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {VariantTable}");
        }
        catch { /* best effort */ }
    }

    // --- SELECT benchmarks ---

    [Benchmark(Description = "SELECT 1K Variant - Native")]
    public async Task<int> Native_Select()
    {
        await using var connection = new NativeConnection(_nativeConnectionString);
        await connection.OpenAsync();
        int count = 0;
        await foreach (var row in connection.QueryAsync($"SELECT v FROM {VariantTable}"))
        {
            _ = row.GetFieldValue<ClickHouseVariant>("v");
            count++;
        }
        return count;
    }

    [Benchmark(Description = "SELECT 1K Variant - Native (typed)")]
    public async Task<int> Native_Select_Typed()
    {
        await using var connection = new NativeConnection(_nativeConnectionString);
        await connection.OpenAsync();
        int count = 0;
        await foreach (var row in connection.QueryAsync($"SELECT v FROM {VariantTable}"))
        {
            _ = row.GetFieldValue<VariantValue<long, string>>("v");
            count++;
        }
        return count;
    }

    [Benchmark(Description = "SELECT 1K Variant - Driver")]
    public async Task<int> Driver_Select()
    {
        await using var connection = new DriverConnection(_driverConnectionString);
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT v FROM {VariantTable}";
        await using var reader = await cmd.ExecuteReaderAsync();
        int count = 0;
        while (await reader.ReadAsync())
        {
            _ = reader.GetValue(0);
            count++;
        }
        return count;
    }

    // --- INSERT benchmarks ---

    [IterationSetup(Targets = new[] { nameof(Native_Insert), nameof(Driver_Insert) })]
    public void TruncateInsertTable()
    {
        TruncateAsync().GetAwaiter().GetResult();
    }

    private async Task TruncateAsync()
    {
        await using var connection = new NativeConnection(_nativeConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync($"TRUNCATE TABLE {VariantTable}");
    }

    [Benchmark(Description = "INSERT 1K Variant - Native")]
    public async Task Native_Insert()
    {
        await using var connection = new NativeConnection(_nativeConnectionString);
        await connection.OpenAsync();
        await connection.BulkInsertAsync(VariantTable, _nativeRows);
    }

    [Benchmark(Description = "INSERT 1K Variant - Native (no compress)")]
    public async Task Native_Insert_NoCompress()
    {
        var noCompressConn = _nativeConnectionString + ";Compress=false";
        await using var connection = new NativeConnection(noCompressConn);
        await connection.OpenAsync();
        await connection.BulkInsertAsync(VariantTable, _nativeRows);
    }

    [Benchmark(Description = "INSERT 1K Variant - Driver")]
    public async Task Driver_Insert()
    {
        using var client = new ClickHouse.Driver.ClickHouseClient(_driverConnectionString);
        await client.InsertBinaryAsync(VariantTable, new[] { "id", "v" }, _driverRows);
    }

    public sealed class VariantRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "v", Order = 1)]
        public ClickHouseVariant V { get; set; }
    }
}
