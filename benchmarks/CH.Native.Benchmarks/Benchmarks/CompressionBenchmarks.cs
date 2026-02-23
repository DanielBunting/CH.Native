using BenchmarkDotNet.Attributes;
using CH.Native.Benchmarks.Infrastructure;
using NativeConnection = CH.Native.Connection.ClickHouseConnection;
using OctonicaConnection = Octonica.ClickHouseClient.ClickHouseConnection;

namespace CH.Native.Benchmarks.Benchmarks;

/// <summary>
/// Benchmarks comparing compression options across drivers.
/// CH.Native: LZ4/Zstd native protocol compression
/// Octonica: LZ4 native protocol compression
/// Driver: HTTP (no native compression control)
/// </summary>
[Config(typeof(ProtocolComparisonConfig))]
public class CompressionBenchmarks
{
    private NativeConnection _nativeNoCompression = null!;
    private NativeConnection _nativeLz4 = null!;
    private NativeConnection _nativeZstd = null!;
    private OctonicaConnection _octonicaNoCompression = null!;
    private OctonicaConnection _octonicaLz4 = null!;
    private ClickHouse.Driver.ADO.ClickHouseConnection _driverConnection = null!;

    private string _query = null!;

    [Params(100_000, 1_000_000)]
    public int RowCount { get; set; }

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        await BenchmarkContainerManager.Instance.EnsureInitializedAsync();
        await TestDataGenerator.EnsureTablesCreatedAsync();

        var manager = BenchmarkContainerManager.Instance;

        // CH.Native connections
        _nativeNoCompression = new NativeConnection(
            $"{manager.NativeConnectionString};Compress=false");
        await _nativeNoCompression.OpenAsync();

        _nativeLz4 = new NativeConnection(
            $"{manager.NativeConnectionString};Compress=true;CompressionMethod=LZ4");
        await _nativeLz4.OpenAsync();

        _nativeZstd = new NativeConnection(
            $"{manager.NativeConnectionString};Compress=true;CompressionMethod=Zstd");
        await _nativeZstd.OpenAsync();

        // Octonica connections
        _octonicaNoCompression = new OctonicaConnection(
            $"{manager.OctonicaConnectionString};Compress=false");
        await _octonicaNoCompression.OpenAsync();

        _octonicaLz4 = new OctonicaConnection(
            $"{manager.OctonicaConnectionString};Compress=true");
        await _octonicaLz4.OpenAsync();

        // ClickHouse.Driver connection (HTTP)
        _driverConnection = new ClickHouse.Driver.ADO.ClickHouseConnection(
            manager.DriverConnectionString);
        await _driverConnection.OpenAsync();
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
        await _octonicaNoCompression.DisposeAsync();
        await _octonicaLz4.DisposeAsync();
        await _driverConnection.DisposeAsync();
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

    // --- Octonica without compression ---

    [Benchmark(Description = "Octonica (no compression)")]
    public async Task<int> Octonica_NoCompression()
    {
        using var cmd = _octonicaNoCompression.CreateCommand(_query);
        await using var reader = await cmd.ExecuteReaderAsync();

        int count = 0;
        while (await reader.ReadAsync())
        {
            count++;
        }
        return count;
    }

    // --- Octonica with LZ4 ---

    [Benchmark(Description = "Octonica (LZ4)")]
    public async Task<int> Octonica_LZ4()
    {
        using var cmd = _octonicaLz4.CreateCommand(_query);
        await using var reader = await cmd.ExecuteReaderAsync();

        int count = 0;
        while (await reader.ReadAsync())
        {
            count++;
        }
        return count;
    }

    // --- Driver (HTTP, no native compression) ---

    [Benchmark(Description = "Driver (HTTP)")]
    public async Task<int> Driver_Http()
    {
        using var cmd = _driverConnection.CreateCommand();
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
