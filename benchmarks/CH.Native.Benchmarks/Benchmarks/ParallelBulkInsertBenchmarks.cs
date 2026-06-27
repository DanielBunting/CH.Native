using BenchmarkDotNet.Attributes;
using CH.Native.Benchmarks.Infrastructure;
using CH.Native.Benchmarks.Models;
using CH.Native.BulkInsert;
using CH.Native.Connection;

namespace CH.Native.Benchmarks.Benchmarks;

/// <summary>
/// Compares a single-connection bulk insert against the multi-connection
/// <see cref="ParallelBulkInserter{T}"/> at 1M and 10M rows on a wide,
/// value/date/numeric-heavy row. Shows where fanning out across pipes pays off
/// and where it hits diminishing returns.
/// </summary>
/// <remarks>
/// Rows are drawn from a pre-generated pool of <see cref="PoolSize"/> distinct
/// objects (cycled by index) so generation stays cheap relative to the insert —
/// the producer is never the bottleneck — and memory stays bounded even at 10M
/// rows. The target table is plain <c>MergeTree</c>, so reused row values are not
/// deduplicated.
/// </remarks>
[Config(typeof(ProtocolComparisonConfig))]
public class ParallelBulkInsertBenchmarks
{
    private const int PoolSize = 100_000;
    private const int BatchSize = 50_000;
    private const string Table = "benchmark_wide_insert";

    private ClickHouseDataSource _dataSource = null!;
    private string _connectionString = null!;
    private WideInsertRow[] _pool = null!;

    [Params(1_000_000, 10_000_000)]
    public int RowCount { get; set; }

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        await BenchmarkContainerManager.Instance.EnsureInitializedAsync();
        _connectionString = BenchmarkContainerManager.Instance.NativeConnectionString;

        await using (var connection = new ClickHouseConnection(_connectionString))
        {
            await connection.OpenAsync();
            await connection.ExecuteNonQueryAsync($@"
                CREATE TABLE IF NOT EXISTS {Table} (
                    id          Int64,
                    seq         Int32,
                    code        String,
                    price       Float64,
                    ratio       Float32,
                    amount      Decimal(18, 4),
                    fee         Decimal(9, 2),
                    qty         Int32,
                    big         UInt64,
                    flag        UInt8,
                    created     DateTime,
                    event_time  DateTime64(3)
                ) ENGINE = MergeTree() ORDER BY id");
        }

        // The data source is the entry point for multi-connection scenarios. The
        // default MaxPoolSize (100) comfortably covers DegreeOfParallelism up to 8.
        _dataSource = new ClickHouseDataSource(_connectionString);

        var codes = new[] { "AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "META", "NVDA", "NFLX" };
        var rnd = new Random(12345);
        var baseTime = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        _pool = new WideInsertRow[PoolSize];
        for (int i = 0; i < PoolSize; i++)
        {
            _pool[i] = new WideInsertRow
            {
                Id = i,
                Seq = i % 1000,
                Code = codes[i % codes.Length],
                Price = rnd.NextDouble() * 1000.0,
                Ratio = (float)rnd.NextDouble(),
                Amount = Math.Round((decimal)(rnd.NextDouble() * 100_000.0), 4),
                Fee = Math.Round((decimal)(rnd.NextDouble() * 100.0), 2),
                Qty = rnd.Next(1, 1000),
                Big = (ulong)rnd.NextInt64(),
                Flag = (byte)(i % 2),
                Created = baseTime.AddSeconds(i),
                EventTime = baseTime.AddMilliseconds(i),
            };
        }
    }

    [GlobalCleanup]
    public async Task GlobalCleanup() => await _dataSource.DisposeAsync();

    [IterationSetup]
    public void IterationSetup()
    {
        using var connection = new ClickHouseConnection(_connectionString);
        connection.OpenAsync().GetAwaiter().GetResult();
        connection.ExecuteNonQueryAsync($"TRUNCATE TABLE {Table}").GetAwaiter().GetResult();
    }

    // Cycles the pre-generated pool to produce RowCount rows without allocating
    // per row or holding the full set in memory.
    private IEnumerable<WideInsertRow> Rows()
    {
        for (int i = 0; i < RowCount; i++)
            yield return _pool[i % PoolSize];
    }

    // --- Baseline: one connection, one pipe ---

    [Benchmark(Baseline = true, Description = "Single connection")]
    public async Task SingleConnection()
    {
        await using var connection = new ClickHouseConnection(_connectionString);
        await connection.OpenAsync();
        await connection.BulkInsertAsync(Table, Rows(), new BulkInsertOptions { BatchSize = BatchSize });
    }

    // --- Parallel fan-out across N pooled connections ---

    [Benchmark(Description = "Parallel x2")]
    public Task ParallelX2() => InsertParallel(2);

    [Benchmark(Description = "Parallel x3")]
    public Task ParallelX3() => InsertParallel(3);

    [Benchmark(Description = "Parallel x4")]
    public Task ParallelX4() => InsertParallel(4);

    private async Task InsertParallel(int degreeOfParallelism)
    {
        await _dataSource.BulkInsertAsync(
            Table,
            Rows(),
            new ParallelBulkInsertOptions
            {
                DegreeOfParallelism = degreeOfParallelism,
                BatchSize = BatchSize,
            });
    }
}
