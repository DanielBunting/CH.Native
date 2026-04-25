using System.Text.Json;
using System.Text.Json.Serialization;

namespace CH.Native.SystemTests.Allocation;

/// <summary>
/// Loads and (in record mode) rewrites the per-scenario allocation budget file.
/// </summary>
internal sealed class AllocationBudget
{
    private const string RecordEnvVar = "CHNATIVE_ALLOC_RECORD";
    private const double TolerancePct = 0.25; // 25% headroom — wide enough to absorb GC/JIT jitter.

    private readonly string _path;
    private readonly Dictionary<string, long> _budgets;

    public bool Recording { get; }

    private AllocationBudget(string path, Dictionary<string, long> budgets, bool recording)
    {
        _path = path;
        _budgets = budgets;
        Recording = recording;
    }

    public static AllocationBudget Load()
    {
        var recording = Environment.GetEnvironmentVariable(RecordEnvVar) == "1";
        var path = ResolvePath();

        Dictionary<string, long> budgets;
        if (File.Exists(path))
        {
            var json = File.ReadAllText(path);
            budgets = JsonSerializer.Deserialize<Dictionary<string, long>>(json, JsonOpts) ?? new();
        }
        else
        {
            budgets = new();
        }

        return new AllocationBudget(path, budgets, recording);
    }

    public void Assert(string scenario, long observedBytes)
    {
        if (Recording)
        {
            _budgets[scenario] = observedBytes;
            Save();
            return;
        }

        if (!_budgets.TryGetValue(scenario, out var baseline))
        {
            throw new InvalidOperationException(
                $"No allocation baseline for '{scenario}'. " +
                $"Run with {RecordEnvVar}=1 to record one, then commit {Path.GetFileName(_path)}.");
        }

        var ceiling = (long)(baseline * (1.0 + TolerancePct));
        if (observedBytes > ceiling)
        {
            throw new Xunit.Sdk.XunitException(
                $"Allocation regression in '{scenario}': observed {observedBytes:N0} bytes, " +
                $"baseline {baseline:N0} bytes (+{TolerancePct:P0} = {ceiling:N0}). " +
                $"If intentional, re-record with {RecordEnvVar}=1.");
        }
    }

    private void Save()
    {
        var ordered = _budgets.OrderBy(kv => kv.Key, StringComparer.Ordinal)
                              .ToDictionary(kv => kv.Key, kv => kv.Value);
        Directory.CreateDirectory(Path.GetDirectoryName(_path)!);
        File.WriteAllText(_path, JsonSerializer.Serialize(ordered, JsonOpts));
    }

    private static string ResolvePath()
    {
        // Walk up from the test binary until we hit the repo root (sibling of tests/).
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            var candidate = Path.Combine(dir.FullName, "tests", "CH.Native.SystemTests", "baselines", "allocation-budgets.json");
            if (File.Exists(candidate) || Directory.Exists(Path.Combine(dir.FullName, "tests", "CH.Native.SystemTests")))
                return candidate;
            dir = dir.Parent;
        }
        // Fallback: emit beside the test binary if we couldn't locate the source tree.
        return Path.Combine(AppContext.BaseDirectory, "baselines", "allocation-budgets.json");
    }

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter() }
    };
}
