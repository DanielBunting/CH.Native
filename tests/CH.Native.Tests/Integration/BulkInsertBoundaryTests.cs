using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

[Collection("ClickHouse")]
public class BulkInsertBoundaryTests
{
    private readonly ClickHouseFixture _fixture;

    public BulkInsertBoundaryTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    #region Float64 Boundary Tests

    [Fact]
    public async Task BulkInsert_Float64_NaN_RoundTrips()
    {
        var tableName = $"test_boundary_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Value Float64
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<FloatRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new FloatRow { Id = 1, Value = double.NaN });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(1, count);

            var result = await connection.ExecuteScalarAsync<double>($"SELECT Value FROM {tableName} WHERE Id = 1");
            Assert.True(double.IsNaN(result));
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_Float64_Infinity_RoundTrips()
    {
        var tableName = $"test_boundary_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Value Float64
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<FloatRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new FloatRow { Id = 1, Value = double.PositiveInfinity });
            await inserter.AddAsync(new FloatRow { Id = 2, Value = double.NegativeInfinity });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(2, count);

            var posInf = await connection.ExecuteScalarAsync<double>($"SELECT Value FROM {tableName} WHERE Id = 1");
            Assert.Equal(double.PositiveInfinity, posInf);

            var negInf = await connection.ExecuteScalarAsync<double>($"SELECT Value FROM {tableName} WHERE Id = 2");
            Assert.Equal(double.NegativeInfinity, negInf);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_Float64_NegativeZero_RoundTrips()
    {
        var tableName = $"test_boundary_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Value Float64
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<FloatRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new FloatRow { Id = 1, Value = -0.0 });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(1, count);

            var result = await connection.ExecuteScalarAsync<double>($"SELECT Value FROM {tableName} WHERE Id = 1");
            Assert.Equal(0.0, result);
            Assert.True(double.IsNegative(result));
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Float32 Boundary Tests

    [Fact]
    public async Task BulkInsert_Float32_Subnormal_RoundTrips()
    {
        var tableName = $"test_boundary_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Value Float32
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<Float32Row>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new Float32Row { Id = 1, Value = float.Epsilon });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(1, count);

            // ClickHouse Float32 maps to C# float; read back as double and cast
            var result = await connection.ExecuteScalarAsync<double>($"SELECT toFloat64(Value) FROM {tableName} WHERE Id = 1");
            Assert.Equal(float.Epsilon, (float)result);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region String Boundary Tests

    [Fact]
    public async Task BulkInsert_String_1MB_RoundTrips()
    {
        var tableName = $"test_boundary_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Value String
            ) ENGINE = Memory");

        try
        {
            var largeString = new string('A', 1_048_576);

            await using var inserter = connection.CreateBulkInserter<StringRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new StringRow { Id = 1, Value = largeString });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(1, count);

            var length = await connection.ExecuteScalarAsync<long>($"SELECT length(Value) FROM {tableName} WHERE Id = 1");
            Assert.Equal(1_048_576, length);

            var result = await connection.ExecuteScalarAsync<long>(
                $"SELECT countSubstrings(Value, 'A') FROM {tableName} WHERE Id = 1");
            Assert.Equal(1_048_576, result);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_String_NullByte_RoundTrips()
    {
        var tableName = $"test_boundary_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Value String
            ) ENGINE = Memory");

        try
        {
            var stringWithNull = "hello\0world";

            await using var inserter = connection.CreateBulkInserter<StringRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new StringRow { Id = 1, Value = stringWithNull });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(1, count);

            var length = await connection.ExecuteScalarAsync<long>($"SELECT length(Value) FROM {tableName} WHERE Id = 1");
            Assert.Equal(11, length);

            string readBack = null!;
            await foreach (var row in connection.QueryAsync($"SELECT Value FROM {tableName} WHERE Id = 1"))
            {
                readBack = row.GetFieldValue<string>("Value");
            }

            Assert.Equal("hello\0world", readBack);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_String_AllUnicodeBlocks_RoundTrips()
    {
        var tableName = $"test_boundary_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Value String
            ) ENGINE = Memory");

        try
        {
            var unicodeString = "Hello \u4f60\u597d \u0645\u0631\u062d\u0628\u0627 \ud83c\udf89 caf\u00e9";

            await using var inserter = connection.CreateBulkInserter<StringRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new StringRow { Id = 1, Value = unicodeString });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(1, count);

            string readBack = null!;
            await foreach (var row in connection.QueryAsync($"SELECT Value FROM {tableName} WHERE Id = 1"))
            {
                readBack = row.GetFieldValue<string>("Value");
            }

            Assert.Equal(unicodeString, readBack);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region DateTime Boundary Tests

    [Fact]
    public async Task BulkInsert_DateTime_Epoch_RoundTrips()
    {
        var tableName = $"test_boundary_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Created DateTime
            ) ENGINE = Memory");

        try
        {
            var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            await using var inserter = connection.CreateBulkInserter<DateTimeRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new DateTimeRow { Id = 1, Created = epoch });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(1, count);

            var result = await connection.ExecuteScalarAsync<DateTime>($"SELECT Created FROM {tableName} WHERE Id = 1");
            Assert.Equal(epoch.Year, result.Year);
            Assert.Equal(epoch.Month, result.Month);
            Assert.Equal(epoch.Day, result.Day);
            Assert.Equal(epoch.Hour, result.Hour);
            Assert.Equal(epoch.Minute, result.Minute);
            Assert.Equal(epoch.Second, result.Second);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_DateTime_MaxValue_RoundTrips()
    {
        var tableName = $"test_boundary_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Created DateTime
            ) ENGINE = Memory");

        try
        {
            // Max UInt32 seconds from epoch: 2106-02-07 06:28:15
            var maxDateTime = new DateTime(2106, 2, 7, 6, 28, 15, DateTimeKind.Utc);

            await using var inserter = connection.CreateBulkInserter<DateTimeRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new DateTimeRow { Id = 1, Created = maxDateTime });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(1, count);

            var result = await connection.ExecuteScalarAsync<DateTime>($"SELECT Created FROM {tableName} WHERE Id = 1");
            Assert.Equal(maxDateTime.Year, result.Year);
            Assert.Equal(maxDateTime.Month, result.Month);
            Assert.Equal(maxDateTime.Day, result.Day);
            Assert.Equal(maxDateTime.Hour, result.Hour);
            Assert.Equal(maxDateTime.Minute, result.Minute);
            Assert.Equal(maxDateTime.Second, result.Second);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_DateTime64_SubMillisecond_RoundTrips()
    {
        var tableName = $"test_boundary_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Timestamp DateTime64(6)
            ) ENGINE = Memory");

        try
        {
            var timestamp = new DateTime(2024, 6, 15, 12, 30, 45, DateTimeKind.Utc).AddTicks(1234567);

            await using var inserter = connection.CreateBulkInserter<DateTime64Row>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new DateTime64Row { Id = 1, Timestamp = timestamp });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(1, count);

            var result = await connection.ExecuteScalarAsync<DateTime>($"SELECT Timestamp FROM {tableName} WHERE Id = 1");
            Assert.Equal(timestamp.Year, result.Year);
            Assert.Equal(timestamp.Month, result.Month);
            Assert.Equal(timestamp.Day, result.Day);
            Assert.Equal(timestamp.Hour, result.Hour);
            Assert.Equal(timestamp.Minute, result.Minute);
            Assert.Equal(timestamp.Second, result.Second);
            Assert.Equal(timestamp.Millisecond, result.Millisecond);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Date Boundary Tests

    [Fact]
    public async Task BulkInsert_Date_MinMax_RoundTrips()
    {
        var tableName = $"test_boundary_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                DateValue Date
            ) ENGINE = Memory");

        try
        {
            var minDate = new DateOnly(1970, 1, 1);
            var maxDate = new DateOnly(2149, 6, 6);

            await using var inserter = connection.CreateBulkInserter<DateRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new DateRow { Id = 1, DateValue = minDate });
            await inserter.AddAsync(new DateRow { Id = 2, DateValue = maxDate });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(2, count);

            var resultMin = await connection.ExecuteScalarAsync<DateOnly>(
                $"SELECT DateValue FROM {tableName} WHERE Id = 1");
            Assert.Equal(minDate, resultMin);

            var resultMax = await connection.ExecuteScalarAsync<DateOnly>(
                $"SELECT DateValue FROM {tableName} WHERE Id = 2");
            Assert.Equal(maxDate, resultMax);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_Date32_ExtendedRange_RoundTrips()
    {
        var tableName = $"test_boundary_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                DateValue Date32
            ) ENGINE = Memory");

        try
        {
            var earlyDate = new DateOnly(1900, 1, 1);
            var lateDate = new DateOnly(2299, 12, 31);

            await using var inserter = connection.CreateBulkInserter<DateRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new DateRow { Id = 1, DateValue = earlyDate });
            await inserter.AddAsync(new DateRow { Id = 2, DateValue = lateDate });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(2, count);

            var resultEarly = await connection.ExecuteScalarAsync<DateOnly>(
                $"SELECT DateValue FROM {tableName} WHERE Id = 1");
            Assert.Equal(earlyDate, resultEarly);

            var resultLate = await connection.ExecuteScalarAsync<DateOnly>(
                $"SELECT DateValue FROM {tableName} WHERE Id = 2");
            Assert.Equal(lateDate, resultLate);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region BatchSize Boundary Tests

    [Fact]
    public async Task BulkInsert_BatchSize_ExactMultiple()
    {
        var tableName = $"test_boundary_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Name String
            ) ENGINE = Memory");

        try
        {
            var options = new BulkInsertOptions { BatchSize = 100 };
            await using var inserter = connection.CreateBulkInserter<SimpleRow>(tableName, options);
            await inserter.InitAsync();

            for (int i = 0; i < 1000; i++)
            {
                await inserter.AddAsync(new SimpleRow { Id = i, Name = $"Row_{i}" });
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
    public async Task BulkInsert_BatchSize_OffByOne()
    {
        var tableName = $"test_boundary_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Name String
            ) ENGINE = Memory");

        try
        {
            var options = new BulkInsertOptions { BatchSize = 100 };
            await using var inserter = connection.CreateBulkInserter<SimpleRow>(tableName, options);
            await inserter.InitAsync();

            for (int i = 0; i < 1001; i++)
            {
                await inserter.AddAsync(new SimpleRow { Id = i, Name = $"Row_{i}" });
            }

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(1001, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_BatchSize_One()
    {
        var tableName = $"test_boundary_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Name String
            ) ENGINE = Memory");

        try
        {
            var options = new BulkInsertOptions { BatchSize = 1 };
            await using var inserter = connection.CreateBulkInserter<SimpleRow>(tableName, options);
            await inserter.InitAsync();

            for (int i = 0; i < 5; i++)
            {
                await inserter.AddAsync(new SimpleRow { Id = i, Name = $"Row_{i}" });
            }

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(5, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Edge Case Row Count Tests

    [Fact]
    public async Task BulkInsert_ZeroRows_Succeeds()
    {
        var tableName = $"test_boundary_{Guid.NewGuid():N}";
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

            // No AddAsync calls — zero rows
            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(0, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_SingleRow_RoundTrips()
    {
        var tableName = $"test_boundary_{Guid.NewGuid():N}";
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

            await inserter.AddAsync(new SimpleRow { Id = 42, Name = "OnlyRow" });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(1, count);

            string readBack = null!;
            await foreach (var row in connection.QueryAsync($"SELECT Name FROM {tableName} WHERE Id = 42"))
            {
                readBack = row.GetFieldValue<string>("Name");
            }

            Assert.Equal("OnlyRow", readBack);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region UUID Boundary Tests

    [Fact]
    public async Task BulkInsert_Uuid_AllZeros_RoundTrips()
    {
        var tableName = $"test_boundary_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                UniqueId UUID
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<UuidRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new UuidRow { Id = 1, UniqueId = Guid.Empty });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(1, count);

            var result = await connection.ExecuteScalarAsync<Guid>($"SELECT UniqueId FROM {tableName} WHERE Id = 1");
            Assert.Equal(Guid.Empty, result);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Test POCOs

    private class FloatRow
    {
        public int Id { get; set; }
        public double Value { get; set; }
    }

    private class Float32Row
    {
        public int Id { get; set; }
        public float Value { get; set; }
    }

    private class StringRow
    {
        public int Id { get; set; }
        public string Value { get; set; } = string.Empty;
    }

    private class DateTimeRow
    {
        public int Id { get; set; }
        public DateTime Created { get; set; }
    }

    private class DateTime64Row
    {
        public int Id { get; set; }
        public DateTime Timestamp { get; set; }
    }

    private class DateRow
    {
        public int Id { get; set; }
        public DateOnly DateValue { get; set; }
    }

    private class SimpleRow
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private class UuidRow
    {
        public int Id { get; set; }
        public Guid UniqueId { get; set; }
    }

    #endregion
}
