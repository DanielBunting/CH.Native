namespace CH.Native.Samples.Hosting.Providers;

/// <summary>
/// Resolves files under <c>./docker/generated/</c> (relative to the sample
/// project root) regardless of whether the app is launched via
/// <c>dotnet run --project</c> (cwd = repo root) or executed directly from
/// <c>bin/Debug/net8.0/</c>.
/// </summary>
internal static class DockerArtifacts
{
    public static string Resolve(string relative)
    {
        // bin/Debug/net8.0/ → ../../../docker/generated/
        var fromBin = Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory, "..", "..", "..", "docker", "generated", relative));
        if (File.Exists(fromBin)) return fromBin;

        return Path.Combine(Directory.GetCurrentDirectory(), "docker", "generated", relative);
    }
}
