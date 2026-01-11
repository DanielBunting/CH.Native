using CH.Native.Connection;

namespace CH.Native.Benchmarks.Infrastructure;

/// <summary>
/// Generates and manages JSON test data for benchmarks.
/// Creates tables with JSON columns for comparing Native vs HTTP performance.
/// </summary>
public static class JsonTestDataGenerator
{
    private static bool _tablesCreated;
    private static readonly SemaphoreSlim _createLock = new(1, 1);

    /// <summary>
    /// Simple JSON table with basic JSON objects.
    /// </summary>
    public const string SimpleJsonTable = "benchmark_json_simple";

    /// <summary>
    /// Table with complex nested JSON objects.
    /// </summary>
    public const string ComplexJsonTable = "benchmark_json_complex";

    /// <summary>
    /// Large table for JSON result set benchmarks.
    /// </summary>
    public const string LargeJsonTable = "benchmark_json_large";

    /// <summary>
    /// Insert table for JSON bulk insert benchmarks (truncated each iteration).
    /// </summary>
    public const string JsonInsertTable = "benchmark_json_insert";

    /// <summary>
    /// Table with deeply nested JSON structures for testing nested path access.
    /// </summary>
    public const string NestedJsonTable = "benchmark_json_nested";

    public static async Task EnsureTablesCreatedAsync()
    {
        if (_tablesCreated) return;

        await _createLock.WaitAsync();
        try
        {
            if (_tablesCreated) return;

            var manager = JsonBenchmarkContainerManager.Instance;
            await manager.EnsureInitializedAsync();

            if (!manager.SupportsJson)
            {
                Console.WriteLine("[JsonTestData] Warning: Server does not support JSON type (requires 25.6+). JSON benchmarks will be skipped.");
                _tablesCreated = true;
                return;
            }

            await using var connection = new ClickHouseConnection(manager.NativeConnectionString);
            await connection.OpenAsync();

            // Simple JSON table (100 rows) - basic user profiles
            Console.WriteLine("[JsonTestData] Creating simple JSON table...");
            await connection.ExecuteNonQueryAsync($@"
                CREATE TABLE IF NOT EXISTS {SimpleJsonTable} (
                    id UInt64,
                    profile JSON
                ) ENGINE = MergeTree() ORDER BY id");

            // Complex JSON table (1000 rows) - nested objects with arrays
            Console.WriteLine("[JsonTestData] Creating complex JSON table...");
            await connection.ExecuteNonQueryAsync($@"
                CREATE TABLE IF NOT EXISTS {ComplexJsonTable} (
                    id UInt64,
                    event_data JSON
                ) ENGINE = MergeTree() ORDER BY id");

            // Large JSON table (100K rows) - for result set benchmarks
            Console.WriteLine("[JsonTestData] Creating large JSON table...");
            await connection.ExecuteNonQueryAsync($@"
                CREATE TABLE IF NOT EXISTS {LargeJsonTable} (
                    id UInt64,
                    data JSON
                ) ENGINE = MergeTree() ORDER BY id");

            // Insert table for bulk benchmarks
            await connection.ExecuteNonQueryAsync($@"
                CREATE TABLE IF NOT EXISTS {JsonInsertTable} (
                    id UInt64,
                    data JSON
                ) ENGINE = MergeTree() ORDER BY id");

            // Populate simple JSON table (100 rows)
            Console.WriteLine("[JsonTestData] Populating simple JSON table (100 rows)...");
            await connection.ExecuteNonQueryAsync($@"
                INSERT INTO {SimpleJsonTable}
                SELECT
                    number AS id,
                    toJSONString(map(
                        'name', concat('User_', toString(number)),
                        'age', toString(20 + (number % 50)),
                        'email', concat('user', toString(number), '@example.com'),
                        'active', if(number % 2 = 0, 'true', 'false')
                    ))::JSON AS profile
                FROM numbers(100)");

            // Populate complex JSON table (1000 rows) - using simpler approach
            Console.WriteLine("[JsonTestData] Populating complex JSON table (1000 rows)...");
            await connection.ExecuteNonQueryAsync($@"
                INSERT INTO {ComplexJsonTable}
                SELECT
                    number AS id,
                    toJSONString(map(
                        'event_type', arrayElement(['click', 'view', 'purchase', 'signup'], toInt32(number % 4) + 1),
                        'user_id', toString(number % 1000),
                        'user_name', concat('User_', toString(number % 1000)),
                        'browser', arrayElement(['Chrome', 'Firefox', 'Safari', 'Edge'], toInt32(number % 4) + 1),
                        'os', arrayElement(['Windows', 'macOS', 'Linux', 'iOS'], toInt32(number % 4) + 1),
                        'version', toString(100 + (number % 50)),
                        'tag1', concat('tag_', toString(number % 10)),
                        'tag2', concat('tag_', toString((number + 1) % 10))
                    ))::JSON AS event_data
                FROM numbers(1000)");

            // Populate large JSON table (100K rows)
            Console.WriteLine("[JsonTestData] Populating large JSON table (100K rows)...");
            await connection.ExecuteNonQueryAsync($@"
                INSERT INTO {LargeJsonTable}
                SELECT
                    number AS id,
                    toJSONString(map(
                        'key', concat('item_', toString(number)),
                        'value', toString(number * 1.5),
                        'category', arrayElement(['A', 'B', 'C', 'D', 'E'], toInt32(number % 5) + 1),
                        'x', toString(number % 100),
                        'y', toString(number % 200)
                    ))::JSON AS data
                FROM numbers(100000)");

            // Nested JSON table (1000 rows) - deeply nested objects
            Console.WriteLine("[JsonTestData] Creating nested JSON table...");
            await connection.ExecuteNonQueryAsync($@"
                CREATE TABLE IF NOT EXISTS {NestedJsonTable} (
                    id UInt64,
                    data JSON
                ) ENGINE = MergeTree() ORDER BY id");

            Console.WriteLine("[JsonTestData] Populating nested JSON table (1000 rows)...");
            await connection.ExecuteNonQueryAsync($@"
                INSERT INTO {NestedJsonTable}
                SELECT
                    number AS id,
                    concat(
                        '{{""user"":{{""name"":""User_', toString(number), '"",',
                        '""profile"":{{""age"":', toString(20 + number % 50), ',',
                        '""address"":{{""street"":""', toString(100 + number), ' Main St"",',
                        '""city"":""', arrayElement(['NYC', 'LA', 'Chicago', 'Houston', 'Phoenix'], toInt32(number % 5) + 1), '"",',
                        '""country"":""USA"",""zip"":""', toString(10000 + number % 90000), '""}},',
                        '""preferences"":{{""theme"":""', arrayElement(['dark', 'light'], toInt32(number % 2) + 1), '"",',
                        '""notifications"":', if(number % 2 = 0, 'true', 'false'), '}}}}}},',
                        '""tags"":[""', arrayElement(['vip', 'standard', 'basic'], toInt32(number % 3) + 1), '"",""user_', toString(number), '""],',
                        '""order_count"":', toString(number % 10), '}}'
                    )::JSON AS data
                FROM numbers(1000)");

            _tablesCreated = true;
            Console.WriteLine("[JsonTestData] JSON tables created and populated successfully");
        }
        finally
        {
            _createLock.Release();
        }
    }

    public static async Task TruncateInsertTableAsync()
    {
        var manager = JsonBenchmarkContainerManager.Instance;
        if (!manager.SupportsJson) return;

        await using var connection = new ClickHouseConnection(manager.NativeConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync($"TRUNCATE TABLE {JsonInsertTable}");
    }
}
