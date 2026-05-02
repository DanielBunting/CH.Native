namespace CH.Native.SystemTests;

/// <summary>
/// Trait category constants. Use as <c>[Trait(Categories.Name, Categories.Cluster)]</c>.
/// </summary>
internal static class Categories
{
    public const string Name = "Category";

    public const string Allocation = "Allocation";
    public const string Cluster = "Cluster";
    public const string Chaos = "Chaos";
    public const string VersionMatrix = "VersionMatrix";
    public const string Soak = "Soak";
    public const string Stress = "Stress";
    public const string Observability = "Observability";
    public const string Resilience = "Resilience";
    public const string ServerFailures = "ServerFailures";
    public const string Cancellation = "Cancellation";
    public const string Streams = "Streams";
    public const string Linq = "Linq";
    public const string Security = "Security";
    public const string DependencyInjection = "DependencyInjection";
    public const string Suite = "Suite";

    /// <summary>
    /// Long-running boundary tests (e.g. LowCardinality 65k dictionary transition).
    /// Excluded from default runs via <c>--filter "Category!=LongBoundary"</c>;
    /// nightly CI opts in by selecting <c>Category=LongBoundary</c>.
    /// </summary>
    public const string LongBoundary = "LongBoundary";
}
