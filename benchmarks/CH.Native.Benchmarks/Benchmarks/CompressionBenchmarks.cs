using BenchmarkDotNet.Attributes;
using CH.Native.Benchmarks.Infrastructure;
using ClickHouse.Client.ADO;
using NativeConnection = CH.Native.Connection.ClickHouseConnection;
using HttpConnection = ClickHouse.Client.ADO.ClickHouseConnection;

namespace CH.Native.Benchmarks.Benchmarks;

/// <summary>
/// Benchmarks comparing compression options.
/// Native: LZ4/Zstd native protocol compression
/// HTTP: gzip HTTP compression
/// </summary>
[Config(typeof(ProtocolComparisonConfig))]
public class CompressionBenchmarks
{
    private NativeConnection _nativeNoCompression = null!;
    private NativeConnection _nativeLz4 = null!;
    private NativeConnection _nativeZstd = null!;
    private HttpConnection _httpNoCompression = null!;
    private HttpConnection _httpGzip = null!;

    private string _query = null!;

    [Params(100_000, 1_000_000)]
    public int RowCount { get; set; }

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        await BenchmarkContainerManager.Instance.EnsureInitializedAsync();
        await TestDataGenerator.EnsureTablesCreatedAsync();

        var manager = BenchmarkContainerManager.Instance;

        // Native connections
        _nativeNoCompression = new NativeConnection(
            $"{manager.NativeConnectionString};Compress=false");
        await _nativeNoCompression.OpenAsync();

        _nativeLz4 = new NativeConnection(
            $"{manager.NativeConnectionString};Compress=true;CompressionMethod=LZ4");
        await _nativeLz4.OpenAsync();

        _nativeZstd = new NativeConnection(
            $"{manager.NativeConnectionString};Compress=true;CompressionMethod=Zstd");
        await _nativeZstd.OpenAsync();

        // HTTP connections
        _httpNoCompression = new HttpConnection(manager.HttpConnectionString);
        await _httpNoCompression.OpenAsync();

        _httpGzip = new HttpConnection(
            $"{manager.HttpConnectionString};Compression=true");
        await _httpGzip.OpenAsync();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _query = $"SELECT * FROM {TestDataGenerator.LargeTable} LIMIT {RowCount}";
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        await _nativeNoCompression.DisposeAsync();
        await _nativeLz4.DisposeAsync();
        await _nativeZstd.DisposeAsync();
        _httpNoCompression.Dispose();
        _httpGzip.Dispose();
    }

    // --- Native without compression ---

    [Benchmark(Description = "Native (no compression)")]
    public async Task<int> Native_NoCompression()
    {
        int count = 0;
        await foreach (var row in _nativeNoCompression.QueryAsync(_query))
        {
            count++;
        }
        return count;
    }

    // --- Native with LZ4 ---

    [Benchmark(Description = "Native (LZ4)")]
    public async Task<int> Native_LZ4()
    {
        int count = 0;
        await foreach (var row in _nativeLz4.QueryAsync(_query))
        {
            count++;
        }
        return count;
    }

    // --- Native with Zstd ---

    [Benchmark(Description = "Native (Zstd)")]
    public async Task<int> Native_Zstd()
    {
        int count = 0;
        await foreach (var row in _nativeZstd.QueryAsync(_query))
        {
            count++;
        }
        return count;
    }

    // --- HTTP without compression ---

    [Benchmark(Description = "HTTP (no compression)")]
    public async Task<int> Http_NoCompression()
    {
        using var cmd = _httpNoCompression.CreateCommand();
        cmd.CommandText = _query;
        using var reader = await cmd.ExecuteReaderAsync();

        int count = 0;
        while (await reader.ReadAsync())
        {
            count++;
        }
        return count;
    }

    // --- HTTP with gzip ---

    [Benchmark(Description = "HTTP (gzip)")]
    public async Task<int> Http_Gzip()
    {
        using var cmd = _httpGzip.CreateCommand();
        cmd.CommandText = _query;
        using var reader = await cmd.ExecuteReaderAsync();

        int count = 0;
        while (await reader.ReadAsync())
        {
            count++;
        }
        return count;
    }
}
