using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration.MultiDim;

// Rank-3 end-to-end read of a real Array(Array(Array(Int32))) column into int[,,].
// Rank-3 rectangular conversion was previously validated only as an isolated
// reflection unit test (JaggedToRectangularConverter); this drives the full reader
// path — wire decode → jagged materialisation → rectangular conversion — so a shape
// or stride bug above rank 2 is caught against a live server.
[Collection("ClickHouse")]
public class DataReaderRectangular3DTests : IAsyncLifetime
{
    private readonly ClickHouseFixture _fixture;
    private readonly string _tableName;

    public DataReaderRectangular3DTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
        _tableName = $"reader_multidim3d_{Guid.NewGuid():N}";
    }

    public async Task InitializeAsync()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {_tableName} (
                id Int32,
                cube Array(Array(Array(Int32)))
            ) ENGINE = MergeTree() ORDER BY id");

        // 2 x 2 x 2 cube with distinct, position-revealing values.
        await connection.ExecuteNonQueryAsync(
            $"INSERT INTO {_tableName} (id, cube) VALUES " +
            "(1, [[[1, 2], [3, 4]], [[5, 6], [7, 8]]])");
    }

    public async Task DisposeAsync()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {_tableName}");
    }

    [Fact]
    public async Task GetFieldValue_RectangularInt3D_ConvertsJaggedColumn()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await using var reader = await connection.ExecuteReaderAsync(
            $"SELECT id, cube FROM {_tableName} ORDER BY id");

        Assert.True(await reader.ReadAsync());
        var cube = reader.GetFieldValue<int[,,]>(1);

        Assert.Equal(2, cube.GetLength(0));
        Assert.Equal(2, cube.GetLength(1));
        Assert.Equal(2, cube.GetLength(2));

        // Each index must land where ClickHouse placed it.
        Assert.Equal(1, cube[0, 0, 0]);
        Assert.Equal(2, cube[0, 0, 1]);
        Assert.Equal(3, cube[0, 1, 0]);
        Assert.Equal(4, cube[0, 1, 1]);
        Assert.Equal(5, cube[1, 0, 0]);
        Assert.Equal(6, cube[1, 0, 1]);
        Assert.Equal(7, cube[1, 1, 0]);
        Assert.Equal(8, cube[1, 1, 1]);
    }
}
