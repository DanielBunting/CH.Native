using System.Text.Json;
using BenchmarkDotNet.Attributes;
using CH.Native.Benchmarks.Infrastructure;
using ClickHouse.Client.ADO;
using NativeConnection = CH.Native.Connection.ClickHouseConnection;
using HttpConnection = ClickHouse.Client.ADO.ClickHouseConnection;

namespace CH.Native.Benchmarks.Benchmarks;

/// <summary>
/// Benchmarks comparing CH.Native (Native TCP) vs ClickHouse.Client (HTTP) for JSON operations.
/// Requires ClickHouse 25.6+ for JSON type support.
/// </summary>
[Config(typeof(ProtocolComparisonConfig))]
public class JsonQueryBenchmarks
{
    private NativeConnection _nativeConnection = null!;
    private HttpConnection _httpConnection = null!;
    private bool _jsonSupported;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        await JsonBenchmarkContainerManager.Instance.EnsureInitializedAsync();

        var manager = JsonBenchmarkContainerManager.Instance;
        _jsonSupported = manager.SupportsJson;

        if (!_jsonSupported)
        {
            Console.WriteLine("[JsonQueryBenchmarks] JSON not supported - benchmarks will return dummy values");
            return;
        }

        await JsonTestDataGenerator.EnsureTablesCreatedAsync();

        _nativeConnection = new NativeConnection(manager.NativeConnectionString);
        await _nativeConnection.OpenAsync();

        _httpConnection = new HttpConnection(manager.HttpConnectionString);
        await _httpConnection.OpenAsync();
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        if (_nativeConnection != null)
            await _nativeConnection.DisposeAsync();

        _httpConnection?.Dispose();
    }

    // ============================================================
    // SELECT single JSON value (latency baseline)
    // ============================================================

    [Benchmark(Description = "SELECT JSON literal - Native")]
    public async Task<string?> Native_SelectJsonLiteral()
    {
        if (!_jsonSupported) return "{}";

        var result = await _nativeConnection.ExecuteScalarAsync<JsonDocument>(
            "SELECT '{\"name\":\"test\",\"value\":42}'::JSON SETTINGS output_format_native_write_json_as_string=1");
        var name = result.RootElement.GetProperty("name").GetString();
        result.Dispose();
        return name;
    }

    [Benchmark(Description = "SELECT JSON literal - HTTP")]
    public async Task<string?> Http_SelectJsonLiteral()
    {
        if (!_jsonSupported) return "{}";

        using var cmd = _httpConnection.CreateCommand();
        cmd.CommandText = "SELECT '{\"name\":\"test\",\"value\":42}'::JSON";
        var result = await cmd.ExecuteScalarAsync();

        // HTTP client returns JSON as string
        if (result is string jsonStr)
        {
            using var doc = JsonDocument.Parse(jsonStr);
            return doc.RootElement.GetProperty("name").GetString();
        }
        return null;
    }

    // ============================================================
    // SELECT 100 rows with simple JSON (small result set)
    // ============================================================

    [Benchmark(Description = "SELECT 100 JSON rows - Native")]
    public async Task<int> Native_Select100JsonRows()
    {
        if (!_jsonSupported) return 0;

        int count = 0;
        await foreach (var row in _nativeConnection.QueryAsync(
            $"SELECT id, profile FROM {JsonTestDataGenerator.SimpleJsonTable} SETTINGS output_format_native_write_json_as_string=1"))
        {
            var profile = row.GetFieldValue<JsonDocument>("profile");
            _ = profile.RootElement.GetProperty("name").GetString();
            profile.Dispose();
            count++;
        }
        return count;
    }

    [Benchmark(Description = "SELECT 100 JSON rows - HTTP")]
    public async Task<int> Http_Select100JsonRows()
    {
        if (!_jsonSupported) return 0;

        using var cmd = _httpConnection.CreateCommand();
        cmd.CommandText = $"SELECT id, profile FROM {JsonTestDataGenerator.SimpleJsonTable}";
        using var reader = await cmd.ExecuteReaderAsync();

        int count = 0;
        while (await reader.ReadAsync())
        {
            var jsonStr = reader.GetString(1);
            using var doc = JsonDocument.Parse(jsonStr);
            _ = doc.RootElement.GetProperty("name").GetString();
            count++;
        }
        return count;
    }

    // ============================================================
    // SELECT 1000 rows with complex JSON
    // ============================================================

    [Benchmark(Description = "SELECT 1K complex JSON - Native")]
    public async Task<int> Native_Select1KComplexJson()
    {
        if (!_jsonSupported) return 0;

        int count = 0;
        await foreach (var row in _nativeConnection.QueryAsync(
            $"SELECT id, event_data FROM {JsonTestDataGenerator.ComplexJsonTable} SETTINGS output_format_native_write_json_as_string=1"))
        {
            var eventData = row.GetFieldValue<JsonDocument>("event_data");
            _ = eventData.RootElement.GetProperty("event_type").GetString();
            _ = eventData.RootElement.GetProperty("user_name").GetString();
            _ = eventData.RootElement.GetProperty("browser").GetString();
            eventData.Dispose();
            count++;
        }
        return count;
    }

    [Benchmark(Description = "SELECT 1K complex JSON - HTTP")]
    public async Task<int> Http_Select1KComplexJson()
    {
        if (!_jsonSupported) return 0;

        using var cmd = _httpConnection.CreateCommand();
        cmd.CommandText = $"SELECT id, event_data FROM {JsonTestDataGenerator.ComplexJsonTable}";
        using var reader = await cmd.ExecuteReaderAsync();

        int count = 0;
        while (await reader.ReadAsync())
        {
            var jsonStr = reader.GetString(1);
            using var doc = JsonDocument.Parse(jsonStr);
            _ = doc.RootElement.GetProperty("event_type").GetString();
            _ = doc.RootElement.GetProperty("user_name").GetString();
            _ = doc.RootElement.GetProperty("browser").GetString();
            count++;
        }
        return count;
    }

    // ============================================================
    // SELECT large result set with JSON (100K rows)
    // ============================================================

    [Benchmark(Description = "SELECT 100K JSON rows - Native")]
    public async Task<long> Native_Select100KJsonRows()
    {
        if (!_jsonSupported) return 0;

        long sum = 0;
        await foreach (var row in _nativeConnection.QueryAsync(
            $"SELECT id, data FROM {JsonTestDataGenerator.LargeJsonTable} SETTINGS output_format_native_write_json_as_string=1"))
        {
            var data = row.GetFieldValue<JsonDocument>("data");
            // x is stored as string in our simplified schema
            if (data.RootElement.TryGetProperty("x", out var xProp))
            {
                if (xProp.ValueKind == JsonValueKind.String && int.TryParse(xProp.GetString(), out var x))
                    sum += x;
                else if (xProp.ValueKind == JsonValueKind.Number)
                    sum += xProp.GetInt32();
            }
            data.Dispose();
        }
        return sum;
    }

    [Benchmark(Description = "SELECT 100K JSON rows - HTTP")]
    public async Task<long> Http_Select100KJsonRows()
    {
        if (!_jsonSupported) return 0;

        using var cmd = _httpConnection.CreateCommand();
        cmd.CommandText = $"SELECT id, data FROM {JsonTestDataGenerator.LargeJsonTable}";
        using var reader = await cmd.ExecuteReaderAsync();

        long sum = 0;
        while (await reader.ReadAsync())
        {
            var jsonStr = reader.GetString(1);
            using var doc = JsonDocument.Parse(jsonStr);
            if (doc.RootElement.TryGetProperty("x", out var xProp))
            {
                if (xProp.ValueKind == JsonValueKind.String && int.TryParse(xProp.GetString(), out var x))
                    sum += x;
                else if (xProp.ValueKind == JsonValueKind.Number)
                    sum += xProp.GetInt32();
            }
        }
        return sum;
    }

    // ============================================================
    // SELECT with JSON path extraction (server-side)
    // ============================================================

    [Benchmark(Description = "JSON path extraction - Native")]
    public async Task<int> Native_JsonPathExtraction()
    {
        if (!_jsonSupported) return 0;

        int count = 0;
        await foreach (var row in _nativeConnection.QueryAsync(
            $"SELECT id, event_data.event_type::String as event_type, event_data.user_name::String as user_name FROM {JsonTestDataGenerator.ComplexJsonTable} SETTINGS output_format_native_write_json_as_string=1"))
        {
            count++;
        }
        return count;
    }

    [Benchmark(Description = "JSON path extraction - HTTP")]
    public async Task<int> Http_JsonPathExtraction()
    {
        if (!_jsonSupported) return 0;

        using var cmd = _httpConnection.CreateCommand();
        cmd.CommandText = $"SELECT id, event_data.event_type::String as event_type, event_data.user_name::String as user_name FROM {JsonTestDataGenerator.ComplexJsonTable}";
        using var reader = await cmd.ExecuteReaderAsync();

        int count = 0;
        while (await reader.ReadAsync())
        {
            count++;
        }
        return count;
    }

    // ============================================================
    // Aggregation on JSON fields
    // ============================================================

    [Benchmark(Description = "COUNT by JSON field - Native")]
    public async Task<int> Native_CountByJsonField()
    {
        if (!_jsonSupported) return 0;

        int count = 0;
        await foreach (var row in _nativeConnection.QueryAsync(
            $"SELECT event_data.event_type::String as event_type, count() as cnt FROM {JsonTestDataGenerator.ComplexJsonTable} GROUP BY event_type SETTINGS output_format_native_write_json_as_string=1"))
        {
            count++;
        }
        return count;
    }

    [Benchmark(Description = "COUNT by JSON field - HTTP")]
    public async Task<int> Http_CountByJsonField()
    {
        if (!_jsonSupported) return 0;

        using var cmd = _httpConnection.CreateCommand();
        cmd.CommandText = $"SELECT event_data.event_type::String as event_type, count() as cnt FROM {JsonTestDataGenerator.ComplexJsonTable} GROUP BY event_type";
        using var reader = await cmd.ExecuteReaderAsync();

        int count = 0;
        while (await reader.ReadAsync())
        {
            count++;
        }
        return count;
    }

    // ============================================================
    // Nested JSON path extraction (3-level deep)
    // ============================================================

    [Benchmark(Description = "Nested 3-level path - Native")]
    public async Task<int> Native_Nested3LevelPath()
    {
        if (!_jsonSupported) return 0;

        int count = 0;
        await foreach (var row in _nativeConnection.QueryAsync(
            $"SELECT id, data.user.profile.address.city::String as city FROM {JsonTestDataGenerator.NestedJsonTable} SETTINGS output_format_native_write_json_as_string=1"))
        {
            count++;
        }
        return count;
    }

    [Benchmark(Description = "Nested 3-level path - HTTP")]
    public async Task<int> Http_Nested3LevelPath()
    {
        if (!_jsonSupported) return 0;

        using var cmd = _httpConnection.CreateCommand();
        cmd.CommandText = $"SELECT id, data.user.profile.address.city::String as city FROM {JsonTestDataGenerator.NestedJsonTable}";
        using var reader = await cmd.ExecuteReaderAsync();

        int count = 0;
        while (await reader.ReadAsync())
        {
            count++;
        }
        return count;
    }

    // ============================================================
    // Nested JSON client-side traversal
    // ============================================================

    [Benchmark(Description = "SELECT 1K nested JSON - Native")]
    public async Task<int> Native_Select1KNestedJson()
    {
        if (!_jsonSupported) return 0;

        int count = 0;
        await foreach (var row in _nativeConnection.QueryAsync(
            $"SELECT id, data FROM {JsonTestDataGenerator.NestedJsonTable} SETTINGS output_format_native_write_json_as_string=1"))
        {
            var data = row.GetFieldValue<JsonDocument>("data");
            // Traverse 3 levels deep client-side
            _ = data.RootElement
                .GetProperty("user")
                .GetProperty("profile")
                .GetProperty("address")
                .GetProperty("city")
                .GetString();
            data.Dispose();
            count++;
        }
        return count;
    }

    [Benchmark(Description = "SELECT 1K nested JSON - HTTP")]
    public async Task<int> Http_Select1KNestedJson()
    {
        if (!_jsonSupported) return 0;

        using var cmd = _httpConnection.CreateCommand();
        cmd.CommandText = $"SELECT id, data FROM {JsonTestDataGenerator.NestedJsonTable}";
        using var reader = await cmd.ExecuteReaderAsync();

        int count = 0;
        while (await reader.ReadAsync())
        {
            var jsonStr = reader.GetString(1);
            using var doc = JsonDocument.Parse(jsonStr);
            _ = doc.RootElement
                .GetProperty("user")
                .GetProperty("profile")
                .GetProperty("address")
                .GetProperty("city")
                .GetString();
            count++;
        }
        return count;
    }

    // ============================================================
    // Filter by nested field
    // ============================================================

    [Benchmark(Description = "Filter nested field - Native")]
    public async Task<int> Native_FilterNestedField()
    {
        if (!_jsonSupported) return 0;

        int count = 0;
        await foreach (var row in _nativeConnection.QueryAsync(
            $"SELECT id FROM {JsonTestDataGenerator.NestedJsonTable} WHERE data.user.profile.address.city::String = 'NYC' SETTINGS output_format_native_write_json_as_string=1"))
        {
            count++;
        }
        return count;
    }

    [Benchmark(Description = "Filter nested field - HTTP")]
    public async Task<int> Http_FilterNestedField()
    {
        if (!_jsonSupported) return 0;

        using var cmd = _httpConnection.CreateCommand();
        cmd.CommandText = $"SELECT id FROM {JsonTestDataGenerator.NestedJsonTable} WHERE data.user.profile.address.city::String = 'NYC'";
        using var reader = await cmd.ExecuteReaderAsync();

        int count = 0;
        while (await reader.ReadAsync())
        {
            count++;
        }
        return count;
    }

    // ============================================================
    // Aggregation on nested field
    // ============================================================

    [Benchmark(Description = "GROUP BY nested field - Native")]
    public async Task<int> Native_GroupByNestedField()
    {
        if (!_jsonSupported) return 0;

        int count = 0;
        await foreach (var row in _nativeConnection.QueryAsync(
            $"SELECT data.user.profile.address.city::String as city, count() as cnt FROM {JsonTestDataGenerator.NestedJsonTable} GROUP BY city SETTINGS output_format_native_write_json_as_string=1"))
        {
            count++;
        }
        return count;
    }

    [Benchmark(Description = "GROUP BY nested field - HTTP")]
    public async Task<int> Http_GroupByNestedField()
    {
        if (!_jsonSupported) return 0;

        using var cmd = _httpConnection.CreateCommand();
        cmd.CommandText = $"SELECT data.user.profile.address.city::String as city, count() as cnt FROM {JsonTestDataGenerator.NestedJsonTable} GROUP BY city";
        using var reader = await cmd.ExecuteReaderAsync();

        int count = 0;
        while (await reader.ReadAsync())
        {
            count++;
        }
        return count;
    }
}
