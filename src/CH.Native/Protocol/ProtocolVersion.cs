namespace CH.Native.Protocol;

/// <summary>
/// ClickHouse native protocol revision constants.
/// Protocol features are enabled based on the negotiated version.
/// </summary>
public static class ProtocolVersion
{
    /// <summary>
    /// Minimum supported protocol revision.
    /// </summary>
    public const int MinSupported = 54406;

    /// <summary>
    /// Current protocol revision we advertise to the server.
    /// </summary>
    public const int Current = 54467;

    /// <summary>
    /// Protocol revision that introduced server timezone and display name in hello.
    /// </summary>
    public const int WithTimezone = 54423;

    /// <summary>
    /// Protocol revision that introduced display name (same as timezone).
    /// </summary>
    public const int WithDisplayName = 54423;

    /// <summary>
    /// Protocol revision that introduced server logs.
    /// </summary>
    public const int WithServerLogs = 54406;

    /// <summary>
    /// Protocol revision that introduced quota key in client info.
    /// </summary>
    public const int WithQuotaKey = 54420;

    /// <summary>
    /// Protocol revision that introduced inter-server secret.
    /// </summary>
    public const int WithInterServerSecret = 54441;

    /// <summary>
    /// Protocol revision that introduced OpenTelemetry tracing support.
    /// </summary>
    public const int WithOpenTelemetry = 54442;

    /// <summary>
    /// Protocol revision that introduced distributed query depth.
    /// </summary>
    public const int WithDistributedDepth = 54448;

    /// <summary>
    /// Protocol revision that introduced client write info in progress.
    /// </summary>
    public const int WithClientWriteInfo = 54450;

    /// <summary>
    /// Protocol revision that introduced parallel replica settings.
    /// </summary>
    public const int WithParallelReplicas = 54453;

    /// <summary>
    /// Protocol revision that introduced custom type serialization.
    /// </summary>
    public const int WithCustomSerialization = 54454;

    /// <summary>
    /// Protocol revision that introduced hello addendum (quota key after hello).
    /// </summary>
    public const int WithAddendum = 54458;

    /// <summary>
    /// Protocol revision that introduced version patch in client info.
    /// </summary>
    public const int WithVersionPatch = 54401;

    /// <summary>
    /// Protocol revision that introduced initial query start time.
    /// </summary>
    public const int WithInitialQueryStartTime = 54449;

    /// <summary>
    /// Protocol revision that introduced X-Forwarded-For in client info.
    /// </summary>
    public const int WithForwardedFor = 54443;

    /// <summary>
    /// Protocol revision that introduced HTTP referer in client info.
    /// </summary>
    public const int WithReferer = 54447;

    /// <summary>
    /// Protocol revision that introduced query parameters.
    /// </summary>
    public const int WithParameters = 54459;

    /// <summary>
    /// Protocol revision that introduced password complexity rules.
    /// </summary>
    public const int WithPasswordComplexityRules = 54461;

    /// <summary>
    /// Protocol revision that introduced interserver secret v2 (nonce).
    /// </summary>
    public const int WithInterServerSecretV2 = 54462;

    /// <summary>
    /// Protocol revision that introduced query elapsed time in progress messages.
    /// </summary>
    public const int WithServerQueryTimeInProgress = 54460;

    /// <summary>
    /// Protocol revision that introduced total bytes in progress messages.
    /// </summary>
    public const int WithTotalBytesInProgress = 54463;

    /// <summary>
    /// Returns true if the given feature is supported at the specified revision.
    /// </summary>
    /// <param name="revision">The negotiated protocol revision.</param>
    /// <param name="feature">The feature revision constant to check.</param>
    /// <returns>True if the feature is supported.</returns>
    public static bool Supports(int revision, int feature) => revision >= feature;
}
