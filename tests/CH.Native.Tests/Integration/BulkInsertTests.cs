using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

[Collection("ClickHouse")]
public class BulkInsertTests
{
    private readonly ClickHouseFixture _fixture;

    public BulkInsertTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    #region Basic Insert Tests

    [Fact]
    public async Task BulkInsert_SimpleRow_RoundTrips()
    {
        var tableName = $"test_bulk_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Create table
        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Name String
            ) ENGINE = Memory");

        try
        {
            // Insert data
            await using var inserter = connection.CreateBulkInserter<SimpleRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new SimpleRow { Id = 1, Name = "Alice" });
            await inserter.AddAsync(new SimpleRow { Id = 2, Name = "Bob" });
            await inserter.AddAsync(new SimpleRow { Id = 3, Name = "Charlie" });

            await inserter.CompleteAsync();

            // Verify
            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(3, count);

            var names = new List<string>();
            await foreach (var row in connection.QueryAsync($"SELECT Name FROM {tableName} ORDER BY Id"))
            {
                names.Add(row.GetFieldValue<string>("Name"));
            }

            Assert.Equal(new[] { "Alice", "Bob", "Charlie" }, names);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_100Rows_AllInserted()
    {
        var tableName = $"test_bulk_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Value Int64
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<IntRow>(tableName);
            await inserter.InitAsync();

            for (int i = 0; i < 100; i++)
            {
                await inserter.AddAsync(new IntRow { Id = i, Value = i * 100L });
            }

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(100, count);

            var sum = await connection.ExecuteScalarAsync<long>($"SELECT sum(Value) FROM {tableName}");
            Assert.Equal(495000L, sum); // 0 + 100 + 200 + ... + 9900
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_10000Rows_WithAutoBatch()
    {
        var tableName = $"test_bulk_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Name String
            ) ENGINE = Memory");

        try
        {
            var options = new BulkInsertOptions { BatchSize = 1000 };
            await using var inserter = connection.CreateBulkInserter<SimpleRow>(tableName, options);
            await inserter.InitAsync();

            for (int i = 0; i < 10000; i++)
            {
                await inserter.AddAsync(new SimpleRow { Id = i, Name = $"Item_{i}" });
            }

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(10000, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_AddRange_Works()
    {
        var tableName = $"test_bulk_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Name String
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<SimpleRow>(tableName);
            await inserter.InitAsync();

            var items = Enumerable.Range(0, 500)
                .Select(i => new SimpleRow { Id = i, Name = $"Batch_{i}" });

            await inserter.AddRangeAsync(items);
            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(500, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Nullable Type Tests

    [Fact]
    public async Task BulkInsert_NullableInt_RoundTrips()
    {
        var tableName = $"test_bulk_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Value Nullable(Int64)
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<NullableRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new NullableRow { Id = 1, Value = 100 });
            await inserter.AddAsync(new NullableRow { Id = 2, Value = null });
            await inserter.AddAsync(new NullableRow { Id = 3, Value = 300 });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(3, count);

            var notNullCount = await connection.ExecuteScalarAsync<long>(
                $"SELECT count() FROM {tableName} WHERE Value IS NOT NULL");
            Assert.Equal(2, notNullCount);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_NullableString_RoundTrips()
    {
        var tableName = $"test_bulk_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Name Nullable(String)
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<NullableStringRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new NullableStringRow { Id = 1, Name = "Hello" });
            await inserter.AddAsync(new NullableStringRow { Id = 2, Name = null });
            await inserter.AddAsync(new NullableStringRow { Id = 3, Name = "World" });

            await inserter.CompleteAsync();

            var nullCount = await connection.ExecuteScalarAsync<long>(
                $"SELECT count() FROM {tableName} WHERE Name IS NULL");
            Assert.Equal(1, nullCount);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Date/Time Type Tests

    [Fact]
    public async Task BulkInsert_DateTime_RoundTrips()
    {
        var tableName = $"test_bulk_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Created DateTime
            ) ENGINE = Memory");

        try
        {
            var now = new DateTime(2024, 6, 15, 10, 30, 0, DateTimeKind.Utc);

            await using var inserter = connection.CreateBulkInserter<DateTimeRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new DateTimeRow { Id = 1, Created = now });
            await inserter.AddAsync(new DateTimeRow { Id = 2, Created = now.AddHours(1) });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(2, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_Date_RoundTrips()
    {
        var tableName = $"test_bulk_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                DateValue Date
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<DateRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new DateRow { Id = 1, DateValue = new DateOnly(2024, 6, 15) });
            await inserter.AddAsync(new DateRow { Id = 2, DateValue = new DateOnly(2024, 12, 25) });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(2, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region String/Unicode Tests

    [Fact]
    public async Task BulkInsert_UnicodeStrings_RoundTrip()
    {
        var tableName = $"test_bulk_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Name String
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<SimpleRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new SimpleRow { Id = 1, Name = "Hello 世界" });
            await inserter.AddAsync(new SimpleRow { Id = 2, Name = "Привет мир" });
            await inserter.AddAsync(new SimpleRow { Id = 3, Name = "こんにちは" });
            await inserter.AddAsync(new SimpleRow { Id = 4, Name = "🎉🎊🎁" });

            await inserter.CompleteAsync();

            var names = new List<string>();
            await foreach (var row in connection.QueryAsync($"SELECT Name FROM {tableName} ORDER BY Id"))
            {
                names.Add(row.GetFieldValue<string>("Name"));
            }

            Assert.Equal("Hello 世界", names[0]);
            Assert.Equal("Привет мир", names[1]);
            Assert.Equal("こんにちは", names[2]);
            Assert.Equal("🎉🎊🎁", names[3]);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_EmptyString_RoundTrips()
    {
        var tableName = $"test_bulk_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Name String
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<SimpleRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new SimpleRow { Id = 1, Name = "" });
            await inserter.AddAsync(new SimpleRow { Id = 2, Name = "Not Empty" });

            await inserter.CompleteAsync();

            var emptyCount = await connection.ExecuteScalarAsync<long>(
                $"SELECT count() FROM {tableName} WHERE Name = ''");
            Assert.Equal(1, emptyCount);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Numeric Type Tests

    [Fact]
    public async Task BulkInsert_AllIntegerTypes_RoundTrip()
    {
        var tableName = $"test_bulk_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Int8Val Int8,
                Int16Val Int16,
                Int64Val Int64,
                UInt8Val UInt8,
                UInt16Val UInt16,
                UInt32Val UInt32,
                UInt64Val UInt64
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<AllIntegerRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new AllIntegerRow
            {
                Id = 1,
                Int8Val = sbyte.MaxValue,
                Int16Val = short.MaxValue,
                Int64Val = long.MaxValue,
                UInt8Val = byte.MaxValue,
                UInt16Val = ushort.MaxValue,
                UInt32Val = uint.MaxValue,
                UInt64Val = ulong.MaxValue
            });

            await inserter.AddAsync(new AllIntegerRow
            {
                Id = 2,
                Int8Val = sbyte.MinValue,
                Int16Val = short.MinValue,
                Int64Val = long.MinValue,
                UInt8Val = 0,
                UInt16Val = 0,
                UInt32Val = 0,
                UInt64Val = 0
            });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(2, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_FloatTypes_RoundTrip()
    {
        var tableName = $"test_bulk_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Float32Val Float32,
                Float64Val Float64
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<FloatRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new FloatRow { Id = 1, Float32Val = 3.14f, Float64Val = 3.14159265358979 });
            await inserter.AddAsync(new FloatRow { Id = 2, Float32Val = 0f, Float64Val = 0 });
            await inserter.AddAsync(new FloatRow { Id = 3, Float32Val = -1.5f, Float64Val = -1.5 });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(3, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region UUID/GUID Tests

    [Fact]
    public async Task BulkInsert_Uuid_RoundTrips()
    {
        var tableName = $"test_bulk_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                UniqueId UUID
            ) ENGINE = Memory");

        try
        {
            var guid1 = Guid.NewGuid();
            var guid2 = Guid.NewGuid();

            await using var inserter = connection.CreateBulkInserter<UuidRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new UuidRow { Id = 1, UniqueId = guid1 });
            await inserter.AddAsync(new UuidRow { Id = 2, UniqueId = guid2 });
            await inserter.AddAsync(new UuidRow { Id = 3, UniqueId = Guid.Empty });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(3, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Column Mapping Tests

    [Fact]
    public async Task BulkInsert_ColumnAttribute_MapsCorrectly()
    {
        var tableName = $"test_bulk_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                user_id Int32,
                user_name String
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<MappedRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new MappedRow { Id = 1, Name = "Alice" });
            await inserter.AddAsync(new MappedRow { Id = 2, Name = "Bob" });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(2, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_IgnoredColumn_NotSent()
    {
        var tableName = $"test_bulk_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Name String
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<RowWithIgnored>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new RowWithIgnored { Id = 1, Name = "Test", Ignored = "Should not be sent" });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(1, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Error Handling Tests

    [Fact]
    public async Task BulkInsert_MissingColumn_ThrowsException()
    {
        var tableName = $"test_bulk_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<SimpleRow>(tableName);

            // Should throw because 'Name' column doesn't exist in table
            // Server returns ClickHouseServerException with "No such column" error
            var ex = await Assert.ThrowsAsync<ClickHouseServerException>(() => inserter.InitAsync());
            Assert.Contains("No such column", ex.Message);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_NotInitialized_ThrowsException()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await using var inserter = connection.CreateBulkInserter<SimpleRow>("any_table");

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => inserter.AddAsync(new SimpleRow()).AsTask());
    }

    [Fact]
    public async Task BulkInsert_AddAfterComplete_ThrowsException()
    {
        var tableName = $"test_bulk_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Name String
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<SimpleRow>(tableName);
            await inserter.InitAsync();
            await inserter.AddAsync(new SimpleRow { Id = 1, Name = "Test" });
            await inserter.CompleteAsync();

            await Assert.ThrowsAsync<InvalidOperationException>(
                () => inserter.AddAsync(new SimpleRow { Id = 2, Name = "After" }).AsTask());
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Compression Tests

    [Fact]
    public async Task BulkInsert_WithLZ4Compression_Works()
    {
        var tableName = $"test_bulk_{Guid.NewGuid():N}";
        var connectionString = $"{_fixture.ConnectionString};Compress=true;CompressionMethod=LZ4";
        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Name String
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<SimpleRow>(tableName);
            await inserter.InitAsync();

            for (int i = 0; i < 1000; i++)
            {
                await inserter.AddAsync(new SimpleRow { Id = i, Name = $"Compressed_{i}" });
            }

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(1000, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Extended Type and Performance Tests

    [Fact]
    public async Task BulkInsert_100000Rows_CompletesSuccessfully()
    {
        var tableName = $"test_bulk_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Name String,
                Value Float64
            ) ENGINE = Memory");

        try
        {
            var options = new BulkInsertOptions { BatchSize = 10000 };
            await using var inserter = connection.CreateBulkInserter<PerformanceRow>(tableName, options);
            await inserter.InitAsync();

            for (int i = 0; i < 100000; i++)
            {
                await inserter.AddAsync(new PerformanceRow
                {
                    Id = i,
                    Name = $"Item_{i}",
                    Value = i * 1.5
                });
            }

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(100000, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact(Skip = "Array column bulk insert not yet implemented - requires ColumnWriterRegistry integration")]
    public async Task BulkInsert_ArrayColumn_RoundTrips()
    {
        var tableName = $"test_bulk_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Tags Array(String)
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<ArrayRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new ArrayRow { Id = 1, Tags = new[] { "a", "b", "c" } });
            await inserter.AddAsync(new ArrayRow { Id = 2, Tags = Array.Empty<string>() });
            await inserter.AddAsync(new ArrayRow { Id = 3, Tags = new[] { "x" } });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(3, count);

            // Verify array contents
            var totalTags = await connection.ExecuteScalarAsync<long>(
                $"SELECT sum(length(Tags)) FROM {tableName}");
            Assert.Equal(4, totalTags); // 3 + 0 + 1
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact(Skip = "Map column bulk insert not yet implemented - requires ColumnWriterRegistry integration")]
    public async Task BulkInsert_MapColumn_RoundTrips()
    {
        var tableName = $"test_bulk_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Metadata Map(String, String)
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<MapRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new MapRow
            {
                Id = 1,
                Metadata = new Dictionary<string, string> { ["key1"] = "value1", ["key2"] = "value2" }
            });
            await inserter.AddAsync(new MapRow { Id = 2, Metadata = new Dictionary<string, string>() });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(2, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_DateTime64_PreservesPrecision()
    {
        var tableName = $"test_bulk_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Timestamp DateTime64(6)
            ) ENGINE = Memory");

        try
        {
            var timestamp = new DateTime(2024, 6, 15, 10, 30, 45, 123, DateTimeKind.Utc)
                .AddTicks(4567); // Add microseconds

            await using var inserter = connection.CreateBulkInserter<DateTime64Row>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new DateTime64Row { Id = 1, Timestamp = timestamp });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(1, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_WithZstdCompression_Works()
    {
        var tableName = $"test_bulk_{Guid.NewGuid():N}";
        var connectionString = $"{_fixture.ConnectionString};Compress=true;CompressionMethod=Zstd";
        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Name String
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<SimpleRow>(tableName);
            await inserter.InitAsync();

            for (int i = 0; i < 1000; i++)
            {
                await inserter.AddAsync(new SimpleRow { Id = i, Name = $"Zstd_{i}" });
            }

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(1000, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_FromAsyncEnumerable_Works()
    {
        var tableName = $"test_bulk_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Name String
            ) ENGINE = Memory");

        try
        {
            await connection.BulkInsertAsync(tableName, GenerateRowsAsync(500));

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(500, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }

        static async IAsyncEnumerable<SimpleRow> GenerateRowsAsync(int count)
        {
            for (int i = 0; i < count; i++)
            {
                await Task.Yield(); // Simulate async source
                yield return new SimpleRow { Id = i, Name = $"Async_{i}" };
            }
        }
    }

    #endregion

    #region Test POCOs

    private class SimpleRow
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private class IntRow
    {
        public int Id { get; set; }
        public long Value { get; set; }
    }

    private class NullableRow
    {
        public int Id { get; set; }
        public long? Value { get; set; }
    }

    private class NullableStringRow
    {
        public int Id { get; set; }
        public string? Name { get; set; }
    }

    private class DateTimeRow
    {
        public int Id { get; set; }
        public DateTime Created { get; set; }
    }

    private class DateRow
    {
        public int Id { get; set; }
        public DateOnly DateValue { get; set; }
    }

    private class AllIntegerRow
    {
        public int Id { get; set; }
        public sbyte Int8Val { get; set; }
        public short Int16Val { get; set; }
        public long Int64Val { get; set; }
        public byte UInt8Val { get; set; }
        public ushort UInt16Val { get; set; }
        public uint UInt32Val { get; set; }
        public ulong UInt64Val { get; set; }
    }

    private class FloatRow
    {
        public int Id { get; set; }
        public float Float32Val { get; set; }
        public double Float64Val { get; set; }
    }

    private class UuidRow
    {
        public int Id { get; set; }
        public Guid UniqueId { get; set; }
    }

    private class MappedRow
    {
        [Column(Name = "user_id")]
        public int Id { get; set; }

        [Column(Name = "user_name")]
        public string Name { get; set; } = string.Empty;
    }

    private class RowWithIgnored
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;

        [Column(Ignore = true)]
        public string Ignored { get; set; } = string.Empty;
    }

    private class PerformanceRow
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public double Value { get; set; }
    }

    private class ArrayRow
    {
        public int Id { get; set; }
        public string[] Tags { get; set; } = Array.Empty<string>();
    }

    private class MapRow
    {
        public int Id { get; set; }
        public Dictionary<string, string> Metadata { get; set; } = new();
    }

    private class DateTime64Row
    {
        public int Id { get; set; }
        public DateTime Timestamp { get; set; }
    }

    #endregion
}
