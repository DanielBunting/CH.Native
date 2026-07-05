using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Ported from the driver's InsertBinarySchemaTests / bulk null-argument tests: explicit
/// <see cref="BulkInsertOptions.ColumnTypes"/> round-trip, and argument guards. Notes divergences —
/// CH.Native requires an explicit column list (null throws) and throws <see cref="ArgumentNullException"/>
/// for a null table name (driver throws <c>InvalidOperationException</c>).
/// </summary>
[Collection("ClickHouse")]
public class BulkInsertDynamicExtrasTests
{
    private readonly ClickHouseFixture _fixture;

    public BulkInsertDynamicExtrasTests(ClickHouseFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task ColumnTypes_Provided_RoundTripsData()
    {
        var table = $"coltypes_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync($"CREATE TABLE {table} (id UInt64, value String) ENGINE = Memory");
        try
        {
            var options = new BulkInsertOptions
            {
                UseSchemaCache = false,
                ColumnTypes = new Dictionary<string, string> { ["id"] = "UInt64", ["value"] = "String" },
            };
            await connection.BulkInsertAsync(table, new[] { "id", "value" },
                new[]
                {
                    new object?[] { 1UL, "alpha" },
                    new object?[] { 2UL, "beta" },
                },
                options);

            await using var reader = await connection.ExecuteReaderAsync($"SELECT id, value FROM {table} ORDER BY id");
            var seen = new List<(ulong, string)>();
            while (await reader.ReadAsync())
                seen.Add((reader.GetFieldValue<ulong>(0), reader.GetFieldValue<string>(1)));

            Assert.Equal(new[] { (1UL, "alpha"), (2UL, "beta") }, seen);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    private sealed class SimpleRow
    {
        public int Id { get; set; }
    }

    [Fact]
    public async Task NullColumnNames_Throws()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        Assert.Throws<ArgumentNullException>(() => connection.CreateBulkInserter("some_table", null!));
    }

    [Fact]
    public async Task NullTableName_ThrowsArgumentNull()
    {
        // Divergence: the driver throws InvalidOperationException; CH.Native guards the table name
        // with ArgumentNullException at inserter construction.
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        Assert.Throws<ArgumentNullException>(() => connection.CreateBulkInserter<SimpleRow>(null!));
    }
}
