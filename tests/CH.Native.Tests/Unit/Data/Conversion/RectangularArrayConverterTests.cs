using CH.Native.Data.Conversion;
using Xunit;

namespace CH.Native.Tests.Unit.Data.Conversion;

public class RectangularArrayConverterTests
{
    [Fact]
    public void To2DJagged_Int_PreservesRowMajorOrder()
    {
        var rect = new int[2, 3]
        {
            { 1, 2, 3 },
            { 4, 5, 6 },
        };

        var jagged = RectangularArrayConverter.To2DJagged(rect);

        Assert.Equal(2, jagged.Length);
        Assert.Equal(new[] { 1, 2, 3 }, jagged[0]);
        Assert.Equal(new[] { 4, 5, 6 }, jagged[1]);
    }

    [Fact]
    public void To2DJagged_EmptyOuter_ReturnsEmpty()
    {
        var rect = new int[0, 5];
        var jagged = RectangularArrayConverter.To2DJagged(rect);
        Assert.Empty(jagged);
    }

    [Fact]
    public void To2DJagged_EmptyInner_ReturnsRowsOfZeroLength()
    {
        var rect = new int[3, 0];
        var jagged = RectangularArrayConverter.To2DJagged(rect);
        Assert.Equal(3, jagged.Length);
        Assert.All(jagged, row => Assert.Empty(row));
    }

    [Fact]
    public void To2DJagged_NullableElement_PreservesNulls()
    {
        var rect = new int?[2, 2]
        {
            { 1, null },
            { null, 4 },
        };

        var jagged = RectangularArrayConverter.To2DJagged(rect);

        Assert.Equal(2, jagged.Length);
        Assert.Equal(new int?[] { 1, null }, jagged[0]);
        Assert.Equal(new int?[] { null, 4 }, jagged[1]);
    }

    [Fact]
    public void To3DJagged_Int_PreservesRowMajorOrder()
    {
        var rect = new int[2, 2, 2]
        {
            { { 1, 2 }, { 3, 4 } },
            { { 5, 6 }, { 7, 8 } },
        };

        var jagged = RectangularArrayConverter.To3DJagged(rect);

        Assert.Equal(2, jagged.Length);
        Assert.Equal(new[] { 1, 2 }, jagged[0][0]);
        Assert.Equal(new[] { 3, 4 }, jagged[0][1]);
        Assert.Equal(new[] { 5, 6 }, jagged[1][0]);
        Assert.Equal(new[] { 7, 8 }, jagged[1][1]);
    }

    [Fact]
    public void ToJagged_Rank4_ReflectsCorrectShape()
    {
        // Reflection path for arbitrary rank.
        var rect = new int[2, 2, 2, 2];
        int counter = 0;
        for (int i = 0; i < 2; i++)
        for (int j = 0; j < 2; j++)
        for (int k = 0; k < 2; k++)
        for (int l = 0; l < 2; l++)
            rect[i, j, k, l] = counter++;

        var jagged = (int[][][][])RectangularArrayConverter.ToJagged(rect);

        Assert.Equal(2, jagged.Length);
        Assert.Equal(0, jagged[0][0][0][0]);
        Assert.Equal(15, jagged[1][1][1][1]);
        Assert.Equal(new[] { 0, 1 }, jagged[0][0][0]);
        Assert.Equal(new[] { 14, 15 }, jagged[1][1][1]);
    }

    [Fact]
    public void ToJagged_Rank1_ReturnsInputUnchanged()
    {
        var arr = new int[] { 1, 2, 3 };
        var result = RectangularArrayConverter.ToJagged(arr);
        Assert.Same(arr, result);
    }

    [Fact]
    public void To2DJagged_Null_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => RectangularArrayConverter.To2DJagged<int>(null!));
    }
}
