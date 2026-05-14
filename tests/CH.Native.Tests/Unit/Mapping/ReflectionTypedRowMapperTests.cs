using CH.Native.Data;
using CH.Native.Exceptions;
using CH.Native.Mapping;
using Xunit;

namespace CH.Native.Tests.Unit.Mapping;

public class ReflectionTypedRowMapperTests
{
    private class Rect2DRow
    {
        public int Id { get; set; }
        public int[,] Grid { get; set; } = new int[0, 0];
    }

    private class Rect3DRow
    {
        public int Id { get; set; }
        public int[,,] Cube { get; set; } = new int[0, 0, 0];
    }

    [Fact]
    public void MapRow_RectangularInt2D_ConvertsJaggedToRect()
    {
        // Column reader materialized Array(Array(Int32)) as int[][] — the typed
        // fast-path Setter must route through JaggedToRectangularConverter
        // before the property assignment.
        var idColumn = new TypedColumn<int>(new[] { 7 });
        var gridColumn = new TypedColumn<int[][]>(new[]
        {
            new[] { new[] { 1, 2, 3 }, new[] { 4, 5, 6 } }
        });

        var mapper = TypedRowMapperFactory.GetMapper<Rect2DRow>(new[] { "Id", "Grid" });
        var row = mapper.MapRow(new ITypedColumn[] { idColumn, gridColumn }, 0);

        Assert.Equal(7, row.Id);
        Assert.Equal(2, row.Grid.GetLength(0));
        Assert.Equal(3, row.Grid.GetLength(1));
        Assert.Equal(1, row.Grid[0, 0]);
        Assert.Equal(6, row.Grid[1, 2]);
    }

    [Fact]
    public void MapRow_RectangularInt2D_RaggedSource_Throws()
    {
        var idColumn = new TypedColumn<int>(new[] { 1 });
        var gridColumn = new TypedColumn<int[][]>(new[]
        {
            new[] { new[] { 1, 2 }, new[] { 3 } } // ragged
        });

        var mapper = TypedRowMapperFactory.GetMapper<Rect2DRow>(new[] { "Id", "Grid" });

        Assert.Throws<ClickHouseTypeConversionException>(
            () => mapper.MapRow(new ITypedColumn[] { idColumn, gridColumn }, 0));
    }

    [Fact]
    public void MapRow_RectangularInt3D_ConvertsCorrectly()
    {
        var idColumn = new TypedColumn<int>(new[] { 2 });
        var cubeColumn = new TypedColumn<int[][][]>(new[]
        {
            new[]
            {
                new[] { new[] { 1, 2 }, new[] { 3, 4 } },
                new[] { new[] { 5, 6 }, new[] { 7, 8 } },
            }
        });

        var mapper = TypedRowMapperFactory.GetMapper<Rect3DRow>(new[] { "Id", "Cube" });
        var row = mapper.MapRow(new ITypedColumn[] { idColumn, cubeColumn }, 0);

        Assert.Equal(2, row.Id);
        Assert.Equal(2, row.Cube.GetLength(0));
        Assert.Equal(2, row.Cube.GetLength(1));
        Assert.Equal(2, row.Cube.GetLength(2));
        Assert.Equal(1, row.Cube[0, 0, 0]);
        Assert.Equal(8, row.Cube[1, 1, 1]);
    }
}
