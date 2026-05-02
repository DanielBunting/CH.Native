namespace CH.Native.SystemTests.VersionMatrix;

/// <summary>
/// The pinned ClickHouse image tags the version matrix runs against. Edit this list when
/// adopting a new LTS or dropping a retired one. Keep entries sorted oldest → newest so
/// failures are reported in upgrade order.
/// </summary>
internal static class SupportedImages
{
    public const string V23_8_LTS = "clickhouse/clickhouse-server:23.8";
    public const string V24_3_LTS = "clickhouse/clickhouse-server:24.3";
    public const string V24_8_LTS = "clickhouse/clickhouse-server:24.8";
    public const string Latest = "clickhouse/clickhouse-server:25.3";

    public static IEnumerable<object[]> All =>
    [
        [V23_8_LTS],
        [V24_3_LTS],
        [V24_8_LTS],
        [Latest],
    ];

    /// <summary>Images that support the JSON object type (24.8+).</summary>
    public static IEnumerable<object[]> WithJsonType =>
    [
        [V24_8_LTS],
        [Latest],
    ];

    /// <summary>
    /// Images that support the experimental Dynamic and Variant types.
    /// Both are gated by <c>SET allow_experimental_*_type = 1</c> session settings;
    /// this list pins the images on which the wire format is exercised here.
    /// </summary>
    public static IEnumerable<object[]> WithDynamicVariant =>
    [
        [V24_8_LTS],
        [Latest],
    ];
}
