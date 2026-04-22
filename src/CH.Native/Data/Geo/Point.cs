namespace CH.Native.Data.Geo;

/// <summary>
/// Represents a ClickHouse Point (Tuple(Float64, Float64)).
/// </summary>
public readonly record struct Point(double X, double Y)
{
    /// <summary>The origin point (0, 0).</summary>
    public static readonly Point Zero = new(0.0, 0.0);
}
