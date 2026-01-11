using System.Text;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using CH.Native.Benchmarks.Infrastructure;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using NativeConnection = CH.Native.Connection.ClickHouseConnection;
using HttpConnection = ClickHouse.Client.ADO.ClickHouseConnection;

namespace CH.Native.Benchmarks.Benchmarks;

/// <summary>
/// Benchmarks comparing CH.Native vs ClickHouse.Client for JSON bulk insert operations.
/// Requires ClickHouse 25.6+ for JSON type support.
/// </summary>
[Config(typeof(ProtocolComparisonConfig))]
public class JsonBulkInsertBenchmarks
{
    private NativeConnection _nativeConnection = null!;
    private HttpConnection _httpConnection = null!;
    private bool _jsonSupported;

    // Pre-generated test data
    private string[] _simpleJsonStrings = null!;
    private string[] _complexJsonStrings = null!;

    [Params(100, 1000)]
    public int RowCount { get; set; }

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        await JsonBenchmarkContainerManager.Instance.EnsureInitializedAsync();

        var manager = JsonBenchmarkContainerManager.Instance;
        _jsonSupported = manager.SupportsJson;

        if (!_jsonSupported)
        {
            Console.WriteLine("[JsonBulkInsertBenchmarks] JSON not supported - benchmarks will be skipped");
            return;
        }

        await JsonTestDataGenerator.EnsureTablesCreatedAsync();

        _nativeConnection = new NativeConnection(manager.NativeConnectionString);
        await _nativeConnection.OpenAsync();

        _httpConnection = new HttpConnection(manager.HttpConnectionString);
        await _httpConnection.OpenAsync();

        // Generate test data
        GenerateTestData();
    }

    private void GenerateTestData()
    {
        // Simple JSON strings
        _simpleJsonStrings = new string[RowCount];
        for (int i = 0; i < RowCount; i++)
        {
            _simpleJsonStrings[i] = $"{{\"name\":\"User_{i}\",\"age\":{20 + (i % 50)},\"active\":{(i % 2 == 0 ? "true" : "false")}}}";
        }

        // Complex JSON strings
        _complexJsonStrings = new string[RowCount];
        var eventTypes = new[] { "click", "view", "purchase", "signup" };
        var browsers = new[] { "Chrome", "Firefox", "Safari", "Edge" };
        for (int i = 0; i < RowCount; i++)
        {
            _complexJsonStrings[i] = $@"{{""event_type"":""{eventTypes[i % 4]}"",""user_id"":{i % 1000},""user_name"":""User_{i % 1000}"",""browser"":""{browsers[i % 4]}"",""version"":""{100 + (i % 50)}""}}";
        }
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        if (_nativeConnection != null)
            await _nativeConnection.DisposeAsync();

        _httpConnection?.Dispose();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        if (!_jsonSupported) return;

        // Truncate insert table before each iteration
        JsonTestDataGenerator.TruncateInsertTableAsync().GetAwaiter().GetResult();
    }

    // ============================================================
    // INSERT ... VALUES with simple JSON (batched)
    // ============================================================

    [Benchmark(Description = "INSERT simple JSON - Native")]
    public async Task<int> Native_InsertSimpleJson()
    {
        if (!_jsonSupported) return 0;

        // Insert in batches of 100
        const int batchSize = 100;
        int inserted = 0;

        for (int batch = 0; batch < RowCount / batchSize; batch++)
        {
            var sb = new StringBuilder();
            sb.Append($"INSERT INTO {JsonTestDataGenerator.JsonInsertTable} VALUES ");

            for (int i = 0; i < batchSize; i++)
            {
                int idx = batch * batchSize + i;
                if (i > 0) sb.Append(',');
                sb.Append($"({idx}, '{EscapeJson(_simpleJsonStrings[idx])}')");
            }

            await _nativeConnection.ExecuteNonQueryAsync(sb.ToString());
            inserted += batchSize;
        }

        return inserted;
    }

    [Benchmark(Description = "INSERT simple JSON - HTTP")]
    public async Task<int> Http_InsertSimpleJson()
    {
        if (!_jsonSupported) return 0;

        const int batchSize = 100;
        int inserted = 0;

        for (int batch = 0; batch < RowCount / batchSize; batch++)
        {
            var sb = new StringBuilder();
            sb.Append($"INSERT INTO {JsonTestDataGenerator.JsonInsertTable} VALUES ");

            for (int i = 0; i < batchSize; i++)
            {
                int idx = batch * batchSize + i;
                if (i > 0) sb.Append(',');
                sb.Append($"({idx}, '{EscapeJson(_simpleJsonStrings[idx])}')");
            }

            using var cmd = _httpConnection.CreateCommand();
            cmd.CommandText = sb.ToString();
            await cmd.ExecuteNonQueryAsync();
            inserted += batchSize;
        }

        return inserted;
    }

    // ============================================================
    // INSERT ... VALUES with complex JSON
    // ============================================================

    [Benchmark(Description = "INSERT complex JSON - Native")]
    public async Task<int> Native_InsertComplexJson()
    {
        if (!_jsonSupported) return 0;

        const int batchSize = 100;
        int inserted = 0;

        for (int batch = 0; batch < RowCount / batchSize; batch++)
        {
            var sb = new StringBuilder();
            sb.Append($"INSERT INTO {JsonTestDataGenerator.JsonInsertTable} VALUES ");

            for (int i = 0; i < batchSize; i++)
            {
                int idx = batch * batchSize + i;
                if (i > 0) sb.Append(',');
                sb.Append($"({idx}, '{EscapeJson(_complexJsonStrings[idx])}')");
            }

            await _nativeConnection.ExecuteNonQueryAsync(sb.ToString());
            inserted += batchSize;
        }

        return inserted;
    }

    [Benchmark(Description = "INSERT complex JSON - HTTP")]
    public async Task<int> Http_InsertComplexJson()
    {
        if (!_jsonSupported) return 0;

        const int batchSize = 100;
        int inserted = 0;

        for (int batch = 0; batch < RowCount / batchSize; batch++)
        {
            var sb = new StringBuilder();
            sb.Append($"INSERT INTO {JsonTestDataGenerator.JsonInsertTable} VALUES ");

            for (int i = 0; i < batchSize; i++)
            {
                int idx = batch * batchSize + i;
                if (i > 0) sb.Append(',');
                sb.Append($"({idx}, '{EscapeJson(_complexJsonStrings[idx])}')");
            }

            using var cmd = _httpConnection.CreateCommand();
            cmd.CommandText = sb.ToString();
            await cmd.ExecuteNonQueryAsync();
            inserted += batchSize;
        }

        return inserted;
    }

    // ============================================================
    // ClickHouseBulkCopy comparison (HTTP only has this API)
    // ============================================================

    [Benchmark(Description = "BulkCopy JSON - HTTP")]
    public async Task<int> Http_BulkCopyJson()
    {
        if (!_jsonSupported) return 0;

        using var bulkCopy = new ClickHouseBulkCopy(_httpConnection)
        {
            DestinationTableName = JsonTestDataGenerator.JsonInsertTable,
            ColumnNames = new[] { "id", "data" }
        };

        var rows = new List<object[]>(RowCount);
        for (int i = 0; i < RowCount; i++)
        {
            rows.Add(new object[] { (ulong)i, _simpleJsonStrings[i] });
        }

        await bulkCopy.WriteToServerAsync(rows);
        return RowCount;
    }

    // ============================================================
    // Single row INSERT (latency test)
    // ============================================================

    [Benchmark(Description = "INSERT single JSON - Native")]
    public async Task Native_InsertSingleJson()
    {
        if (!_jsonSupported) return;

        await _nativeConnection.ExecuteNonQueryAsync(
            $"INSERT INTO {JsonTestDataGenerator.JsonInsertTable} VALUES (0, '{{\"test\": true}}')");
    }

    [Benchmark(Description = "INSERT single JSON - HTTP")]
    public async Task Http_InsertSingleJson()
    {
        if (!_jsonSupported) return;

        using var cmd = _httpConnection.CreateCommand();
        cmd.CommandText = $"INSERT INTO {JsonTestDataGenerator.JsonInsertTable} VALUES (0, '{{\"test\": true}}')";
        await cmd.ExecuteNonQueryAsync();
    }

    private static string EscapeJson(string json)
    {
        // Escape single quotes for SQL by doubling them
        return json.Replace("'", "''");
    }
}
