using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration.MultiDim;

// Covers the ADO.NET reader's rectangular-array conversion path. The LINQ /
// typed-mapper pipeline bypasses GetFieldValue<T>(), so users calling the
// reader directly need their own coverage.
[Collection("ClickHouse")]
public class DataReaderRectangularTests : IAsyncLifetime
{
    private readonly ClickHouseFixture _fixture;
    private readonly string _tableName;

    public DataReaderRectangularTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
        _tableName = $"reader_multidim_{Guid.NewGuid():N}";
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
    public async Task GetFieldValue_RectangularInt2D_ConvertsJaggedColumn()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await using var reader = await connection.ExecuteReaderAsync(
            $"SELECT id, grid FROM {_tableName} ORDER BY id");

        Assert.True(await reader.ReadAsync());
        var firstId = reader.GetFieldValue<int>(0);
        var firstGrid = reader.GetFieldValue<int[,]>(1);

        Assert.Equal(1, firstId);
        Assert.Equal(2, firstGrid.GetLength(0));
        Assert.Equal(3, firstGrid.GetLength(1));
        Assert.Equal(1, firstGrid[0, 0]);
        Assert.Equal(6, firstGrid[1, 2]);

        Assert.True(await reader.ReadAsync());
        var secondGrid = reader.GetFieldValue<int[,]>("grid");

        Assert.Equal(2, secondGrid.GetLength(0));
        Assert.Equal(2, secondGrid.GetLength(1));
        Assert.Equal(7, secondGrid[0, 0]);
        Assert.Equal(10, secondGrid[1, 1]);

        Assert.False(await reader.ReadAsync());
    }
}
