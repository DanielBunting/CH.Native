using CH.Native.Connection;

namespace CH.Native.Benchmarks.Infrastructure;

/// <summary>
/// Generates and manages test data for benchmarks.
/// Ensures tables are created and populated once before benchmarks run.
/// </summary>
public static class TestDataGenerator
{
    private static bool _tablesCreated;
    private static readonly SemaphoreSlim _createLock = new(1, 1);

    public const string SimpleTable = "benchmark_simple";
    public const string LargeTable = "benchmark_large";
    public const string ComplexTable = "benchmark_complex";
    public const string InsertTable = "benchmark_insert";

    public static async Task EnsureTablesCreatedAsync()
    {
        if (_tablesCreated) return;

        await _createLock.WaitAsync();
        try
        {
            if (_tablesCreated) return;

            var manager = BenchmarkContainerManager.Instance;
            await manager.EnsureInitializedAsync();

            // Use CH.Native to create tables (native protocol)
            await using var connection = new ClickHouseConnection(manager.NativeConnectionString);
            await connection.OpenAsync();

            // Simple table for basic queries (100 rows)
            await connection.ExecuteNonQueryAsync($@"
                CREATE TABLE IF NOT EXISTS {SimpleTable} (
                    id Int64,
                    name String,
                    value Float64,
                    created DateTime
                ) ENGINE = MergeTree() ORDER BY id");

            // Large table for result set benchmarks (1M rows)
            await connection.ExecuteNonQueryAsync($@"
                CREATE TABLE IF NOT EXISTS {LargeTable} (
                    id Int64,
                    category String,
                    name String,
                    value Float64,
                    quantity Int32,
                    created DateTime
                ) ENGINE = MergeTree() ORDER BY (category, id)");

            // Complex table for aggregation benchmarks (1M rows)
            await connection.ExecuteNonQueryAsync($@"
                CREATE TABLE IF NOT EXISTS {ComplexTable} (
                    id Int64,
                    user_id Int64,
                    product_id Int64,
                    category LowCardinality(String),
                    region LowCardinality(String),
                    amount Decimal(18, 2),
                    quantity Int32,
                    created DateTime64(3)
                ) ENGINE = MergeTree() ORDER BY (category, region, created)");

            // Insert table (for bulk insert benchmarks - truncated each iteration)
            await connection.ExecuteNonQueryAsync($@"
                CREATE TABLE IF NOT EXISTS {InsertTable} (
                    id Int64,
                    name String,
                    value Float64
                ) ENGINE = MergeTree() ORDER BY id");

            // Populate simple table (100 rows)
            await connection.ExecuteNonQueryAsync($@"
                INSERT INTO {SimpleTable}
                SELECT
                    number AS id,
                    concat('Item_', toString(number)) AS name,
                    number * 1.5 AS value,
                    now() - toIntervalSecond(number) AS created
                FROM numbers(100)");

            // Populate large table (1M rows)
            Console.WriteLine("[TestData] Populating large table (1M rows)...");
            await connection.ExecuteNonQueryAsync($@"
                INSERT INTO {LargeTable}
                SELECT
                    number AS id,
                    concat('Category_', toString(number % 100)) AS category,
                    concat('Product_', toString(number)) AS name,
                    rand() / 1000000.0 AS value,
                    toInt32(rand() % 1000) AS quantity,
                    now() - toIntervalSecond(number % 86400) AS created
                FROM numbers(1000000)");

            // Populate complex table (1M rows)
            Console.WriteLine("[TestData] Populating complex table (1M rows)...");
            await connection.ExecuteNonQueryAsync($@"
                INSERT INTO {ComplexTable}
                SELECT
                    number AS id,
                    number % 10000 AS user_id,
                    number % 5000 AS product_id,
                    arrayElement(['Electronics', 'Clothing', 'Food', 'Books', 'Sports'],
                                 toInt32(rand() % 5) + 1) AS category,
                    arrayElement(['North', 'South', 'East', 'West', 'Central'],
                                 toInt32(rand() % 5) + 1) AS region,
                    toDecimal64(rand() / 100.0, 2) AS amount,
                    toInt32(rand() % 100) + 1 AS quantity,
                    now64(3) - toIntervalMillisecond(number % 86400000) AS created
                FROM numbers(1000000)");

            _tablesCreated = true;
            Console.WriteLine("[TestData] Tables created and populated successfully");
        }
        finally
        {
            _createLock.Release();
        }
    }

    public static async Task TruncateInsertTableAsync()
    {
        var manager = BenchmarkContainerManager.Instance;
        await using var connection = new ClickHouseConnection(manager.NativeConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync($"TRUNCATE TABLE {InsertTable}");
    }
}
