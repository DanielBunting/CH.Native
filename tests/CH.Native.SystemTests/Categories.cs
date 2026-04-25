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
}
