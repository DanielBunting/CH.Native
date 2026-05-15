using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.Mapping;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration.Linq;

// Validates the LINQ materialization pipeline for rectangular property types.
// The expression visitor does not translate int[,] indexers — this only covers the
// result-projection path, which shares ReflectionTypedRowMapper with QueryAsync<T>.
[Collection("ClickHouse")]
public class LinqMultiDimTests : IAsyncLifetime
{
    private readonly ClickHouseFixture _fixture;
    private readonly string _tableName;

    public LinqMultiDimTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
        _tableName = $"linq_multidim_{Guid.NewGuid():N}";
    }

    public async Task InitializeAsync()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {_tableName} (
                id Int32,
                grid Array(Array(Int32))
            ) ENGINE = MergeTree() ORDER BY id");

        await connection.ExecuteNonQueryAsync(
            $"INSERT INTO {_tableName} (id, grid) VALUES " +
            "(1, [[1, 2, 3], [4, 5, 6]]), " +
            "(2, [[7, 8], [9, 10]])");
    }

    public async Task DisposeAsync()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {_tableName}");
    }

    [Fact]
    public async Task Table_MaterializesRectangularInt2D()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var rows = await connection.Table<RectInt2DLinqRow>(_tableName)
            .OrderBy(r => r.Id)
            .ToListAsync();

        Assert.Equal(2, rows.Count);

        Assert.Equal(2, rows[0].Grid.GetLength(0));
        Assert.Equal(3, rows[0].Grid.GetLength(1));
        Assert.Equal(1, rows[0].Grid[0, 0]);
        Assert.Equal(6, rows[0].Grid[1, 2]);

        Assert.Equal(2, rows[1].Grid.GetLength(0));
        Assert.Equal(2, rows[1].Grid.GetLength(1));
        Assert.Equal(7, rows[1].Grid[0, 0]);
        Assert.Equal(10, rows[1].Grid[1, 1]);
    }

    private class RectInt2DLinqRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "grid", Order = 1)]
        public int[,] Grid { get; set; } = new int[0, 0];
    }
}
