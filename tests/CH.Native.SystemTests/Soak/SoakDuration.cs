namespace CH.Native.SystemTests.Soak;

internal static class SoakDuration
{
    private const string EnvVar = "CHNATIVE_SOAK_DURATION";
    private static readonly TimeSpan Default = TimeSpan.FromMinutes(10);

    public static TimeSpan Resolve()
    {
        var raw = Environment.GetEnvironmentVariable(EnvVar);
        if (string.IsNullOrWhiteSpace(raw)) return Default;
        return TimeSpan.TryParse(raw, out var parsed) ? parsed : Default;
    }
}
