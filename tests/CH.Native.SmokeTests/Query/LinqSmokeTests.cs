using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.Linq;
using CH.Native.SmokeTests.Fixtures;
using CH.Native.SmokeTests.Helpers;
using Xunit;

namespace CH.Native.SmokeTests.Query;

[Collection("SmokeTest")]
public class LinqSmokeTests
{
    private readonly SmokeTestFixture _fixture;

    public LinqSmokeTests(SmokeTestFixture fixture)
    {
        _fixture = fixture;
    }

    public class LinqTestRecord
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "name", Order = 1)]
        public string Name { get; set; } = "";

        [ClickHouseColumn(Name = "amount", Order = 2)]
        public decimal Amount { get; set; }

        [ClickHouseColumn(Name = "category", Order = 3)]
        public string Category { get; set; } = "";
    }

    private async Task<string> SetupLinqTable()
    {
        var table = $"smoke_linq_{Guid.NewGuid():N}";

        await NativeQueryHelper.ExecuteNonQueryAsync(
            _fixture.NativeConnectionString,
            $@"CREATE TABLE {table} (
                id Int32,
                name String,
                amount Decimal64(2),
                category String
            ) ENGINE = Memory");

        await NativeQueryHelper.ExecuteNonQueryAsync(
            _fixture.NativeConnectionString,
            $@"INSERT INTO {table} VALUES
               (1, 'alice', 100.50, 'electronics'),
               (2, 'bob', 200.00, 'books'),
               (3, 'charlie', 50.25, 'electronics'),
               (4, 'diana', 300.75, 'clothing'),
               (5, 'eve', 150.00, 'books'),
               (6, 'frank', 75.50, 'electronics'),
               (7, 'grace', 225.00, 'clothing'),
               (8, 'hank', 10.00, 'books')");

        return table;
    }

    [Fact]
    public async Task WhereAndOrderBy()
    {
        var table = await SetupLinqTable();
        try
        {
            await using var connection = new ClickHouseConnection(_fixture.NativeConnectionString);
            await connection.OpenAsync();

            var linqResults = await connection.Table<LinqTestRecord>(table)
                .Where(r => r.Amount > 100)
                .OrderBy(r => r.Name)
                .ToListAsync();

            var driverResults = await DriverQueryHelper.QueryAsync(
                _fixture.DriverConnectionString,
                $"SELECT id, name, amount, category FROM {table} WHERE amount > 100 ORDER BY name");

            Assert.Equal(driverResults.Count, linqResults.Count);
            for (int i = 0; i < linqResults.Count; i++)
            {
                ResultComparer.AssertValuesEqual(linqResults[i].Id, driverResults[i][0], $"LINQ Where row {i} id");
                ResultComparer.AssertValuesEqual(linqResults[i].Name, driverResults[i][1], $"LINQ Where row {i} name");
                ResultComparer.AssertValuesEqual(linqResults[i].Amount, driverResults[i][2], $"LINQ Where row {i} amount");
                ResultComparer.AssertValuesEqual(linqResults[i].Category, driverResults[i][3], $"LINQ Where row {i} category");
            }
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task Contains()
    {
        var table = await SetupLinqTable();
        try
        {
            await using var connection = new ClickHouseConnection(_fixture.NativeConnectionString);
            await connection.OpenAsync();

            var linqResults = await connection.Table<LinqTestRecord>(table)
                .Where(r => r.Name.Contains("li"))
                .OrderBy(r => r.Id)
                .ToListAsync();

            var driverResults = await DriverQueryHelper.QueryAsync(
                _fixture.DriverConnectionString,
                $"SELECT id, name, amount, category FROM {table} WHERE name LIKE '%li%' ORDER BY id");

            Assert.Equal(driverResults.Count, linqResults.Count);
            for (int i = 0; i < linqResults.Count; i++)
            {
                ResultComparer.AssertValuesEqual(linqResults[i].Name, driverResults[i][1], $"LINQ Contains row {i} name");
            }
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task StartsWith()
    {
        var table = await SetupLinqTable();
        try
        {
            await using var connection = new ClickHouseConnection(_fixture.NativeConnectionString);
            await connection.OpenAsync();

            var linqResults = await connection.Table<LinqTestRecord>(table)
                .Where(r => r.Category.StartsWith("elec"))
                .OrderBy(r => r.Id)
                .ToListAsync();

            var driverResults = await DriverQueryHelper.QueryAsync(
                _fixture.DriverConnectionString,
                $"SELECT id, name, amount, category FROM {table} WHERE category LIKE 'elec%' ORDER BY id");

            Assert.Equal(driverResults.Count, linqResults.Count);
            for (int i = 0; i < linqResults.Count; i++)
            {
                ResultComparer.AssertValuesEqual(linqResults[i].Id, driverResults[i][0], $"LINQ StartsWith row {i} id");
            }
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task SkipTake()
    {
        var table = await SetupLinqTable();
        try
        {
            await using var connection = new ClickHouseConnection(_fixture.NativeConnectionString);
            await connection.OpenAsync();

            var linqResults = await connection.Table<LinqTestRecord>(table)
                .OrderBy(r => r.Id)
                .Skip(2)
                .Take(3)
                .ToListAsync();

            var driverResults = await DriverQueryHelper.QueryAsync(
                _fixture.DriverConnectionString,
                $"SELECT id, name, amount, category FROM {table} ORDER BY id LIMIT 3 OFFSET 2");

            Assert.Equal(driverResults.Count, linqResults.Count);
            for (int i = 0; i < linqResults.Count; i++)
            {
                ResultComparer.AssertValuesEqual(linqResults[i].Id, driverResults[i][0], $"LINQ Skip/Take row {i} id");
                ResultComparer.AssertValuesEqual(linqResults[i].Name, driverResults[i][1], $"LINQ Skip/Take row {i} name");
            }
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task Count()
    {
        var table = await SetupLinqTable();
        try
        {
            await using var connection = new ClickHouseConnection(_fixture.NativeConnectionString);
            await connection.OpenAsync();

            var linqCount = await connection.Table<LinqTestRecord>(table)
                .Where(r => r.Category == "electronics")
                .CountAsync();

            var driverResult = await DriverQueryHelper.ExecuteScalarAsync(
                _fixture.DriverConnectionString,
                $"SELECT count() FROM {table} WHERE category = 'electronics'");

            Assert.Equal(Convert.ToInt32(driverResult), linqCount);
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task OrderByThenBy()
    {
        var table = await SetupLinqTable();
        try
        {
            await using var connection = new ClickHouseConnection(_fixture.NativeConnectionString);
            await connection.OpenAsync();

            var linqResults = await connection.Table<LinqTestRecord>(table)
                .OrderBy(r => r.Category)
                .ThenBy(r => r.Amount)
                .ToListAsync();

            var driverResults = await DriverQueryHelper.QueryAsync(
                _fixture.DriverConnectionString,
                $"SELECT id, name, amount, category FROM {table} ORDER BY category, amount");

            Assert.Equal(driverResults.Count, linqResults.Count);
            for (int i = 0; i < linqResults.Count; i++)
            {
                ResultComparer.AssertValuesEqual(linqResults[i].Id, driverResults[i][0], $"LINQ OrderByThenBy row {i} id");
                ResultComparer.AssertValuesEqual(linqResults[i].Category, driverResults[i][3], $"LINQ OrderByThenBy row {i} category");
            }
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task EmptyTable()
    {
        var table = $"smoke_linq_{Guid.NewGuid():N}";
        try
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $@"CREATE TABLE {table} (
                    id Int32,
                    name String,
                    amount Decimal64(2),
                    category String
                ) ENGINE = Memory");

            await using var connection = new ClickHouseConnection(_fixture.NativeConnectionString);
            await connection.OpenAsync();

            var linqResults = await connection.Table<LinqTestRecord>(table)
                .ToListAsync();

            var driverResults = await DriverQueryHelper.QueryAsync(
                _fixture.DriverConnectionString,
                $"SELECT id, name, amount, category FROM {table}");

            Assert.Empty(linqResults);
            Assert.Empty(driverResults);
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task SelectProjection()
    {
        var table = await SetupLinqTable();
        try
        {
            await using var connection = new ClickHouseConnection(_fixture.NativeConnectionString);
            await connection.OpenAsync();

            // Use ToSql to get the generated SQL and run both sides as raw SQL
            var sql = connection.Table<LinqTestRecord>(table)
                .Where(r => r.Amount > 50)
                .OrderBy(r => r.Id)
                .ToSql();

            var native = await NativeQueryHelper.QueryAsync(
                _fixture.NativeConnectionString, sql);

            // Equivalent raw SQL for driver
            var driver = await DriverQueryHelper.QueryAsync(
                _fixture.DriverConnectionString,
                $"SELECT id, name, amount, category FROM {table} WHERE amount > 50 ORDER BY id");

            Assert.Equal(driver.Count, native.Count);
            for (int i = 0; i < native.Count; i++)
            {
                for (int col = 0; col < native[i].Length && col < driver[i].Length; col++)
                {
                    ResultComparer.AssertValuesEqual(native[i][col], driver[i][col],
                        $"LINQ SelectProjection row {i} col {col}");
                }
            }
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }
}
