using System.Data;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// End-to-end coverage for the transparent-wrapper nullability fix class: bulk insert into
/// <c>SimpleAggregateFunction(fn, Nullable(T))</c> writes the null-map correctly (null + non-null
/// values), a SAF-wrapped Decimal round-trips (exercising decimal scale/precision extraction after the
/// SAF strip), <c>LowCardinality(Nullable(T))</c> is a control, and <c>GetSchemaTable().AllowDBNull</c>
/// reflects nullability through the wrappers.
/// </summary>
[Collection("ClickHouse")]
public class BulkInsertTransparentWrapperTests
{
    private readonly ClickHouseFixture _fixture;

    public BulkInsertTransparentWrapperTests(ClickHouseFixture fixture) => _fixture = fixture;

    private sealed class SafRow
    {
        [ClickHouseColumn(Name = "value")]
        public double? Value { get; set; }
    }

    [Theory]
    [InlineData(1.5)]
    [InlineData(null)]
    public async Task SimpleAggregateFunction_NullableInner_RoundTrips_Poco(double? value)
    {
        var table = $"safnull_{Guid.NewGuid():N}";
        await using var c = new ClickHouseConnection(_fixture.ConnectionString);
        await c.OpenAsync();
        await c.ExecuteNonQueryAsync($"CREATE TABLE {table} (value SimpleAggregateFunction(anyLast, Nullable(Float64))) ENGINE = Memory");
        try
        {
            await using var inserter = c.CreateBulkInserter<SafRow>(table);
            await inserter.InitAsync();
            await inserter.AddAsync(new SafRow { Value = value });
            await inserter.CompleteAsync();

            await using var reader = await c.ExecuteReaderAsync($"SELECT value FROM {table}");
            Assert.True(await reader.ReadAsync());
            if (value is null)
                Assert.True(reader.IsDBNull(0));
            else
                Assert.Equal(value.Value, reader.GetFieldValue<double>(0));
        }
        finally { await c.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}"); }
    }

    [Fact]
    public async Task SimpleAggregateFunction_NullableInner_RoundTrips_Dynamic()
    {
        var table = $"safdyn_{Guid.NewGuid():N}";
        await using var c = new ClickHouseConnection(_fixture.ConnectionString);
        await c.OpenAsync();
        await c.ExecuteNonQueryAsync($"CREATE TABLE {table} (id Int32, value SimpleAggregateFunction(anyLast, Nullable(Float64))) ENGINE = Memory");
        try
        {
            await c.BulkInsertAsync(table, new[] { "id", "value" },
                new[]
                {
                    new object?[] { 1, 2.5 },
                    new object?[] { 2, null },
                });

            var nonNull = await c.ExecuteScalarAsync<double>($"SELECT value FROM {table} WHERE id = 1");
            Assert.Equal(2.5, nonNull);
            var nullCount = await c.ExecuteScalarAsync<long>($"SELECT count() FROM {table} WHERE value IS NULL");
            Assert.Equal(1, nullCount);
        }
        finally { await c.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}"); }
    }

    private sealed class DecRow
    {
        [ClickHouseColumn(Name = "value")]
        public decimal Value { get; set; }
    }

    [Fact]
    public async Task SimpleAggregateFunction_DecimalInner_RoundTrips()
    {
        var table = $"safdec_{Guid.NewGuid():N}";
        await using var c = new ClickHouseConnection(_fixture.ConnectionString);
        await c.OpenAsync();
        await c.ExecuteNonQueryAsync($"CREATE TABLE {table} (value SimpleAggregateFunction(anyLast, Decimal64(4))) ENGINE = Memory");
        try
        {
            await using var inserter = c.CreateBulkInserter<DecRow>(table);
            await inserter.InitAsync();
            await inserter.AddAsync(new DecRow { Value = 12.3456m });
            await inserter.CompleteAsync();

            Assert.Equal(12.3456m, await c.ExecuteScalarAsync<decimal>($"SELECT value FROM {table}"));
        }
        finally { await c.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}"); }
    }

    private sealed class LcRow
    {
        [ClickHouseColumn(Name = "value")]
        public string? Value { get; set; }
    }

    [Theory]
    [InlineData("hello")]
    [InlineData(null)]
    public async Task LowCardinalityNullable_RoundTrips(string? value)
    {
        var table = $"lcnull_{Guid.NewGuid():N}";
        await using var c = new ClickHouseConnection(_fixture.ConnectionString);
        await c.OpenAsync();
        await c.ExecuteNonQueryAsync($"CREATE TABLE {table} (value LowCardinality(Nullable(String))) ENGINE = Memory");
        try
        {
            await using var inserter = c.CreateBulkInserter<LcRow>(table);
            await inserter.InitAsync();
            await inserter.AddAsync(new LcRow { Value = value });
            await inserter.CompleteAsync();

            await using var reader = await c.ExecuteReaderAsync($"SELECT value FROM {table}");
            Assert.True(await reader.ReadAsync());
            if (value is null)
                Assert.True(reader.IsDBNull(0));
            else
                Assert.Equal(value, reader.GetFieldValue<string>(0));
        }
        finally { await c.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}"); }
    }

    [Fact]
    public async Task GetSchemaTable_AllowDBNull_AccountsForWrappers()
    {
        var table = $"schema_{Guid.NewGuid():N}";
        await using var c = new ClickHouseConnection(_fixture.ConnectionString);
        await c.OpenAsync();
        await c.ExecuteNonQueryAsync($@"CREATE TABLE {table} (
            plain Int32,
            nullable Nullable(Int32),
            lc_str LowCardinality(String),
            lc_null LowCardinality(Nullable(String)),
            saf_null SimpleAggregateFunction(anyLast, Nullable(Float64))
        ) ENGINE = Memory");
        try
        {
            await using var reader = await c.ExecuteReaderAsync($"SELECT * FROM {table}");
            var schema = reader.GetSchemaTable();
            Assert.NotNull(schema);

            var allowNull = new Dictionary<string, bool>();
            foreach (DataRow row in schema!.Rows)
                allowNull[(string)row["ColumnName"]] = (bool)row["AllowDBNull"];

            Assert.False(allowNull["plain"]);
            Assert.True(allowNull["nullable"]);
            Assert.False(allowNull["lc_str"]);
            Assert.True(allowNull["lc_null"]);   // Nullable inside LowCardinality
            Assert.True(allowNull["saf_null"]);  // Nullable inside SimpleAggregateFunction
        }
        finally { await c.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}"); }
    }
}
