using Apache.Arrow;
using Apache.Arrow.Adbc;
using BenchmarkDotNet.Attributes;
using CH.Native.Adbc;
using CH.Native.Benchmarks.Infrastructure;
using CH.Native.Benchmarks.Models;
using NativeConnection = CH.Native.Connection.ClickHouseConnection;
using ChNativeDapper = CH.Native.Dapper.ClickHouseConnectionDapperExtensions;

namespace CH.Native.Benchmarks.Benchmarks;

/// <summary>
/// Compares reading a result set as Arrow columns through the ADBC driver against the row-oriented
/// paths on the same data:
///   - ADBC (Arrow) — decode straight into Arrow RecordBatches; aggregate via column spans
///   - CH.Native <c>QueryStreamAsync&lt;T&gt;</c> — typed POCO streaming
///   - CH.Native Dapper <c>QueryAsync&lt;T&gt;</c> — row mapper
///   - CH.Native <c>ClickHouseDataReader</c> — typed accessors, no mapper
///
/// All paths read the four common columns of the existing benchmark tables (100-row
/// <c>SimpleTable</c> and 1M-row <c>LargeTable</c>) and aggregate the two numeric columns, so the
/// comparison is "read a result set and reduce it" — the columnar path's sweet spot. The ADBC
/// "drain" benchmark additionally isolates pure decode-to-Arrow throughput with no per-value work.
/// </summary>
[Config(typeof(ProtocolComparisonConfig))]
[MemoryDiagnoser]
public class AdbcReadBenchmarks
{
    private AdbcConnection _adbc = null!;
    private NativeConnection _native = null!;

    private const string SmallSql =
        "SELECT id, name, value, created FROM " + TestDataGenerator.SimpleTable;

    private const string LargeSql =
        "SELECT id, name, value, created FROM " + TestDataGenerator.LargeTable;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        await BenchmarkContainerManager.Instance.EnsureInitializedAsync();
        await TestDataGenerator.EnsureTablesCreatedAsync();
        CH.Native.Dapper.ClickHouseDapperIntegration.Register();

        var connectionString = BenchmarkContainerManager.Instance.NativeConnectionString;

        var driver = new ClickHouseAdbcDriver();
        var database = driver.Open(new Dictionary<string, string>
        {
            [AdbcOptionKeys.ConnectionString] = connectionString,
        });
        _adbc = database.Connect(null);

        _native = new NativeConnection(connectionString);
        await _native.OpenAsync();
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        _adbc.Dispose();
        await _native.DisposeAsync();
    }

    // --- ADBC: pure decode throughput (no per-value work) ---

    [Benchmark(Description = "100 rows - ADBC drain RecordBatches")]
    public Task<long> Adbc_Drain_Small() => AdbcDrainAsync(SmallSql);

    [Benchmark(Description = "1M rows - ADBC drain RecordBatches")]
    public Task<long> Adbc_Drain_Large() => AdbcDrainAsync(LargeSql);

    // --- ADBC: aggregate numeric columns via Arrow spans ---

    [Benchmark(Description = "100 rows - ADBC sum columns (Arrow)")]
    public Task<double> Adbc_Sum_Small() => AdbcSumAsync(SmallSql);

    [Benchmark(Description = "1M rows - ADBC sum columns (Arrow)")]
    public Task<double> Adbc_Sum_Large() => AdbcSumAsync(LargeSql);

    // --- Row-oriented equivalents ---

    [Benchmark(Description = "100 rows - Native QueryStream<T> sum")]
    public Task<double> Native_Stream_Small() => NativeStreamSumAsync(SmallSql);

    [Benchmark(Description = "1M rows - Native QueryStream<T> sum")]
    public Task<double> Native_Stream_Large() => NativeStreamSumAsync(LargeSql);

    [Benchmark(Description = "100 rows - Native Dapper QueryAsync<T> sum")]
    public Task<double> NativeDapper_Small() => NativeDapperSumAsync(SmallSql);

    [Benchmark(Description = "1M rows - Native Dapper QueryAsync<T> sum")]
    public Task<double> NativeDapper_Large() => NativeDapperSumAsync(LargeSql);

    [Benchmark(Description = "100 rows - Native DataReader typed sum")]
    public Task<double> NativeReader_Small() => NativeReaderSumAsync(SmallSql);

    [Benchmark(Description = "1M rows - Native DataReader typed sum")]
    public Task<double> NativeReader_Large() => NativeReaderSumAsync(LargeSql);

    // --- Implementations ---

    private async Task<long> AdbcDrainAsync(string sql)
    {
        using var statement = _adbc.CreateStatement();
        statement.SqlQuery = sql;
        var result = statement.ExecuteQuery();
        using var stream = result.Stream!;

        long rows = 0;
        while (await stream.ReadNextRecordBatchAsync() is { } batch)
        {
            using (batch)
                rows += batch.Length;
        }
        return rows;
    }

    private async Task<double> AdbcSumAsync(string sql)
    {
        using var statement = _adbc.CreateStatement();
        statement.SqlQuery = sql;
        var result = statement.ExecuteQuery();
        using var stream = result.Stream!;

        long idSum = 0;
        double valueSum = 0;
        while (await stream.ReadNextRecordBatchAsync() is { } batch)
        {
            using (batch)
            {
                // Columnar consumption: read the backing spans, no per-row boxing or POCO.
                var ids = ((Int64Array)batch.Column(0)).Values;
                var values = ((DoubleArray)batch.Column(2)).Values;
                for (int i = 0; i < ids.Length; i++)
                {
                    idSum += ids[i];
                    valueSum += values[i];
                }
            }
        }
        return idSum + valueSum;
    }

    private async Task<double> NativeStreamSumAsync(string sql)
    {
        long idSum = 0;
        double valueSum = 0;
        await foreach (var row in _native.QueryStreamAsync<SimpleRow>(sql))
        {
            idSum += row.Id;
            valueSum += row.Value;
        }
        return idSum + valueSum;
    }

    private async Task<double> NativeDapperSumAsync(string sql)
    {
        long idSum = 0;
        double valueSum = 0;
        foreach (var row in await ChNativeDapper.QueryAsync<SimpleRow>(_native, sql))
        {
            idSum += row.Id;
            valueSum += row.Value;
        }
        return idSum + valueSum;
    }

    private async Task<double> NativeReaderSumAsync(string sql)
    {
        using var cmd = _native.CreateCommand();
        cmd.CommandText = sql;
        await using var reader = await cmd.ExecuteReaderAsync();

        long idSum = 0;
        double valueSum = 0;
        while (await reader.ReadAsync())
        {
            idSum += reader.GetInt64(0);
            valueSum += reader.GetDouble(2);
        }
        return idSum + valueSum;
    }
}
