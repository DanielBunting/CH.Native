using System.Diagnostics.Metrics;
using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Observability;

/// <summary>
/// OpenTelemetry conventions discourage encoding the unit in the metric name
/// when the <c>unit</c> property is set — duplicating it produces confusing
/// names like <c>query_duration_seconds {unit=s}</c> and breaks downstream
/// tools that auto-rename based on unit. This test enumerates every metric
/// emitted under the <c>CH.Native</c> meter during a representative workload
/// and asserts none ends in a recognised unit suffix.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Observability)]
public class MetricNamingConventionTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    private static readonly string[] DisallowedUnitSuffixes =
    {
        "_seconds", "_milliseconds", "_microseconds", "_nanoseconds",
        "_bytes", "_kilobytes", "_megabytes",
    };

    public MetricNamingConventionTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task EveryEmittedMetric_HasNoUnitSuffixWhenUnitIsSet()
    {
        var observed = new HashSet<(string name, string? unit)>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == "CH.Native")
            {
                observed.Add((instrument.Name, instrument.Unit));
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.Start();

        // Generate a representative workload so all eligible instruments publish.
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();
        _ = await conn.ExecuteScalarAsync<int>("SELECT 1");
        _ = await conn.ExecuteScalarAsync<int>("SELECT 2");

        var offenders = observed
            .Where(o => !string.IsNullOrEmpty(o.unit))
            .Where(o => DisallowedUnitSuffixes.Any(s => o.name.EndsWith(s, StringComparison.OrdinalIgnoreCase)))
            .ToList();

        foreach (var (name, unit) in observed)
            _output.WriteLine($"{name} (unit={unit ?? "<none>"})");

        Assert.True(offenders.Count == 0,
            $"Metrics encode their unit in both the name and the unit property: " +
            string.Join(", ", offenders.Select(o => $"{o.name} {{unit={o.unit}}}")));
    }
}
