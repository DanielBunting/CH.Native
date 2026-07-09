using CH.Native.Data.Conversion;
using CH.Native.Exceptions;
using CH.Native.Parameters;
using Xunit;

namespace CH.Native.Tests.Unit.Data.Conversion;

/// <summary>
/// Pins that rectangular (T[,], T[,,]) and jagged inputs are treated equivalently at the
/// parameter-serialization boundary, and that the jagged→rectangular conversion enforces its
/// rectangularity contract with <see cref="ClickHouseTypeConversionException"/>.
/// </summary>
public class MultidimArrayEquivalenceTests
{
    [Fact]
    public void Rectangular2D_SerializesIdenticallyToJagged()
    {
        var rect = new[,] { { 1, 2, 3 }, { 4, 5, 6 } };
        var jagged = new[] { new[] { 1, 2, 3 }, new[] { 4, 5, 6 } };

        var fromRect = ParameterSerializer.Serialize(rect, "Array(Array(Int32))");
        var fromJagged = ParameterSerializer.Serialize(jagged, "Array(Array(Int32))");

        Assert.Equal(fromJagged, fromRect);
    }

    [Fact]
    public void Rectangular3D_SerializesIdenticallyToJagged()
    {
        var rect = new[, ,] { { { 1, 2 }, { 3, 4 } }, { { 5, 6 }, { 7, 8 } } };
        var jagged = new[]
        {
            new[] { new[] { 1, 2 }, new[] { 3, 4 } },
            new[] { new[] { 5, 6 }, new[] { 7, 8 } },
        };

        Assert.Equal(
            ParameterSerializer.Serialize(jagged, "Array(Array(Array(Int32)))"),
            ParameterSerializer.Serialize(rect, "Array(Array(Array(Int32)))"));
    }

    [Fact]
    public void ToRectangular2D_RoundTripsThroughJagged()
    {
        var jagged = new[] { new[] { 1, 2 }, new[] { 3, 4 } };
        var rect = JaggedToRectangularConverter.ToRectangular2D(jagged);

        Assert.Equal(2, rect.GetLength(0));
        Assert.Equal(2, rect.GetLength(1));
        Assert.Equal(4, rect[1, 1]);

        var back = RectangularArrayConverter.To2DJagged(rect);
        Assert.Equal(jagged[0], back[0]);
        Assert.Equal(jagged[1], back[1]);
    }

    [Fact]
    public void ToRectangular2D_RaggedRows_ThrowsConversionException()
    {
        var ragged = new[] { new[] { 1, 2 }, new[] { 3 } };
        Assert.Throws<ClickHouseTypeConversionException>(() => JaggedToRectangularConverter.ToRectangular2D(ragged));
    }

    [Fact]
    public void ToRectangular2D_Null_ThrowsArgumentNull() =>
        Assert.Throws<ArgumentNullException>(() => JaggedToRectangularConverter.ToRectangular2D<int>(null!));
}
