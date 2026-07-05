using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Covers build-time mapping validation added to <c>BulkInserter&lt;T&gt;</c>: duplicate and
/// empty/whitespace column names are rejected, and indexer properties are excluded rather than
/// mis-mapped. (Ported from the driver's BinaryInsertTypeRegistry / PocoRegistration tests.)
/// </summary>
[Collection("ClickHouse")]
public class BulkInserterMappingValidationTests
{
    private readonly ClickHouseFixture _fixture;

    public BulkInserterMappingValidationTests(ClickHouseFixture fixture) => _fixture = fixture;

    private sealed class DuplicateColumnPoco
    {
        [ClickHouseColumn(Name = "id")]
        public int A { get; set; }

        [ClickHouseColumn(Name = "id")]
        public int B { get; set; }
    }

    private sealed class EmptyColumnNamePoco
    {
        [ClickHouseColumn(Name = "  ")]
        public int Value { get; set; }
    }

    private sealed class IndexerPoco
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";

        // Indexers must be skipped, not mapped.
        public string this[int i] => i.ToString();
    }

    [Fact]
    public async Task DuplicateColumnNames_Throws()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        var table = $"dup_{Guid.NewGuid():N}";
        await connection.ExecuteNonQueryAsync($"CREATE TABLE {table} (id Int32) ENGINE = Memory");
        try
        {
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await using var inserter = connection.CreateBulkInserter<DuplicateColumnPoco>(table);
                await inserter.InitAsync();
            });
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task EmptyColumnName_Throws()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        var table = $"empty_{Guid.NewGuid():N}";
        await connection.ExecuteNonQueryAsync($"CREATE TABLE {table} (value Int32) ENGINE = Memory");
        try
        {
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await using var inserter = connection.CreateBulkInserter<EmptyColumnNamePoco>(table);
                await inserter.InitAsync();
            });
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task IndexerProperty_IsExcluded_InsertSucceeds()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        var table = $"idx_{Guid.NewGuid():N}";
        await connection.ExecuteNonQueryAsync($"CREATE TABLE {table} (Id Int32, Name String) ENGINE = Memory");
        try
        {
            await using var inserter = connection.CreateBulkInserter<IndexerPoco>(table);
            await inserter.InitAsync();
            await inserter.AddAsync(new IndexerPoco { Id = 1, Name = "ok" });
            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {table}");
            Assert.Equal(1, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }
}
