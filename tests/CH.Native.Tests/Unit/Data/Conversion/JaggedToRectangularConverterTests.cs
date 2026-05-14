using CH.Native.Data.Conversion;
using CH.Native.Exceptions;
using Xunit;

namespace CH.Native.Tests.Unit.Data.Conversion;

public class JaggedToRectangularConverterTests
{
    [Fact]
    public void ToRectangular2D_Uniform_ReturnsRectangular()
    {
        var jagged = new[]
        {
            new[] { 1, 2, 3 },
            new[] { 4, 5, 6 },
        };

        var rect = JaggedToRectangularConverter.ToRectangular2D(jagged);

        Assert.Equal(2, rect.GetLength(0));
        Assert.Equal(3, rect.GetLength(1));
        Assert.Equal(1, rect[0, 0]);
        Assert.Equal(3, rect[0, 2]);
        Assert.Equal(6, rect[1, 2]);
    }

    [Fact]
    public void ToRectangular2D_Empty_ReturnsZeroByZero()
    {
        var jagged = Array.Empty<int[]>();
        var rect = JaggedToRectangularConverter.ToRectangular2D(jagged);
        Assert.Equal(0, rect.GetLength(0));
        Assert.Equal(0, rect.GetLength(1));
    }

    [Fact]
    public void ToRectangular2D_Ragged_ThrowsWithRowIndex()
    {
        var jagged = new[]
        {
            new[] { 1, 2 },
            new[] { 3 }, // ragged
        };

        var ex = Assert.Throws<ClickHouseTypeConversionException>(
            () => JaggedToRectangularConverter.ToRectangular2D(jagged));

        Assert.Equal(1, ex.RowIndex);
        Assert.Equal(2, ex.ExpectedLength);
        Assert.Equal(1, ex.ActualLength);
        Assert.Contains("row 1", ex.Message);
        Assert.Contains("expected 2", ex.Message);
    }

    [Fact]
    public void ToRectangular3D_Uniform_ReturnsRectangular()
    {
        var jagged = new[]
        {
            new[] { new[] { 1, 2 }, new[] { 3, 4 } },
            new[] { new[] { 5, 6 }, new[] { 7, 8 } },
        };

        var rect = JaggedToRectangularConverter.ToRectangular3D(jagged);

        Assert.Equal(2, rect.GetLength(0));
        Assert.Equal(2, rect.GetLength(1));
        Assert.Equal(2, rect.GetLength(2));
        Assert.Equal(1, rect[0, 0, 0]);
        Assert.Equal(8, rect[1, 1, 1]);
    }

    [Fact]
    public void ToRectangular3D_RaggedInner_Throws()
    {
        var jagged = new[]
        {
            new[] { new[] { 1, 2 }, new[] { 3, 4 } },
            new[] { new[] { 5, 6 }, new[] { 7 } }, // ragged inner
        };

        var ex = Assert.Throws<ClickHouseTypeConversionException>(
            () => JaggedToRectangularConverter.ToRectangular3D(jagged));

        Assert.Equal(2, ex.ExpectedLength);
        Assert.Equal(1, ex.ActualLength);
    }

    [Fact]
    public void ToRectangular_ReflectionRank4_ProducesCorrectShape()
    {
        // Build a uniform jagged 2x2x2x2.
        var jagged = new int[2][][][];
        for (int i = 0; i < 2; i++)
        {
            jagged[i] = new int[2][][];
            for (int j = 0; j < 2; j++)
            {
                jagged[i][j] = new int[2][];
                for (int k = 0; k < 2; k++)
                {
                    jagged[i][j][k] = new[] { i * 8 + j * 4 + k * 2, i * 8 + j * 4 + k * 2 + 1 };
                }
            }
        }

        var rect = (int[,,,])JaggedToRectangularConverter.ToRectangular(jagged, typeof(int[,,,]));

        Assert.Equal(2, rect.GetLength(0));
        Assert.Equal(2, rect.GetLength(1));
        Assert.Equal(2, rect.GetLength(2));
        Assert.Equal(2, rect.GetLength(3));
        Assert.Equal(0, rect[0, 0, 0, 0]);
        Assert.Equal(15, rect[1, 1, 1, 1]);
    }

    [Fact]
    public void ToRectangular_ReflectionRaggedAtDeepDim_Throws()
    {
        var jagged = new[]
        {
            new[] { new[] { 1, 2 }, new[] { 3, 4 } },
            new[] { new[] { 5 } }, // dim 1 length differs from expected 2
        };

        Assert.Throws<ClickHouseTypeConversionException>(
            () => JaggedToRectangularConverter.ToRectangular(jagged, typeof(int[,,])));
    }
}
