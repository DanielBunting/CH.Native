using System.Diagnostics.Metrics;
using CH.Native.Telemetry;
using Xunit;

namespace CH.Native.Tests.Unit.Telemetry;

/// <summary>
/// Audit finding #20: <see cref="ClickHouseMeter.RecordRetry"/> currently tags
/// every retry with <c>error.type = ex.GetType().Name</c>. The full type name
/// is unbounded — anything any user-supplied <c>ShouldRetry</c> classifies as
/// transient ends up as a distinct Prometheus series. These tests document
/// the present behaviour so a future bucketed implementation has a clear
/// before/after baseline.
/// </summary>
[Collection("MeterTests")]
public class ClickHouseMeterTests : IDisposable
{
    private readonly MeterListener _listener;
    private readonly List<(string Instrument, long Value, KeyValuePair<string, object?>[] Tags)> _measurements = new();
    private readonly object _lock = new();

    public ClickHouseMeterTests()
    {
        _listener = new MeterListener
        {
            InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == ClickHouseMeter.Name)
                    listener.EnableMeasurementEvents(instrument);
            }
        };
        _listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            lock (_lock)
            {
                _measurements.Add((instrument.Name, value, tags.ToArray()));
            }
        });
        _listener.SetMeasurementEventCallback<double>((instrument, _, _, _) =>
        {
            // We only assert long counters here; histograms are still drained so
            // they don't throw, but their values are not recorded.
            _ = instrument;
        });
        _listener.Start();
    }

    public void Dispose() => _listener.Dispose();

    [Fact]
    public void RecordRetry_BucketsKnownExceptionTypes()
    {
        ClickHouseMeter.RecordRetry(1, TimeSpan.FromMilliseconds(10), "SocketException");

        var retry = SingleMeasurement("ch_native_retry_attempts_total");
        var errorType = TagValue(retry.Tags, "error.type");
        Assert.Equal("network", errorType);
    }

    [Theory]
    [InlineData("SocketException", "network")]
    [InlineData("ClickHouseConnectionException", "network")]
    [InlineData("IOException", "network")]
    [InlineData("System.IO.IOException", "network")]
    [InlineData("TimeoutException", "timeout")]
    [InlineData("HttpRequestTimeoutException", "timeout")]
    [InlineData("OperationCanceledException", "cancelled")]
    [InlineData("TaskCanceledException", "cancelled")]
    [InlineData("ClickHouseServerException", "server")]
    [InlineData("ClickHouseProtocolException", "client")]
    [InlineData("OverflowException", "client")]
    [InlineData("ArgumentException", "client")]
    [InlineData("MyApp.Custom.WeirdException", "other")]
    [InlineData("", "other")]
    public void BucketExceptionType_MapsToFixedLabels(string input, string expected)
    {
        Assert.Equal(expected, ClickHouseMeter.BucketExceptionType(input));
    }

    // Audit finding #20: error.type cardinality must be bounded. After the fix
    // adds a bucketing layer (network/server/client/timeout/cancelled/other or
    // similar), 100 distinct exception class names should collapse to a small
    // fixed set. This test FAILS today because RecordRetry forwards the raw
    // string verbatim, producing 100 distinct series.
    [Fact]
    public void RecordRetry_DistinctExceptionNames_AreBucketedToSmallFixedSet()
    {
        const int MaxAllowedDistinctBuckets = 10;

        for (int i = 0; i < 100; i++)
            ClickHouseMeter.RecordRetry(1, TimeSpan.FromMilliseconds(1), $"CustomTransientException_{i}");

        var distinct = _measurements
            .Where(m => m.Instrument == "ch_native_retry_attempts_total")
            .Select(m => TagValue(m.Tags, "error.type"))
            .Distinct()
            .ToList();

        Assert.True(distinct.Count <= MaxAllowedDistinctBuckets,
            $"Expected ≤{MaxAllowedDistinctBuckets} distinct error.type buckets after bucketing fix; got {distinct.Count}: " +
            string.Join(", ", distinct.Take(10)));
    }

    // Audit finding #20: a long namespaced generic type name should NOT make
    // it into a metric tag. After bucketing, this should reduce to one of the
    // known buckets ("client", "other", etc.) — definitely not the raw string.
    [Fact]
    public void RecordRetry_PathologicalTypeName_IsNotForwardedVerbatim()
    {
        var pathological = "MyApp.Some.Long.Namespace.WithGeneric`3+Nested[System.Int32,System.String,My.Other.Type]";
        ClickHouseMeter.RecordRetry(1, TimeSpan.FromMilliseconds(1), pathological);

        var retry = _measurements.Last(m => m.Instrument == "ch_native_retry_attempts_total");
        var errorType = TagValue(retry.Tags, "error.type");

        Assert.NotNull(errorType);
        Assert.NotEqual(pathological, errorType);
        // Sanity: the bucket label should be short.
        Assert.True(errorType!.Length <= 32,
            $"Expected short bucket label, got {errorType.Length} chars: {errorType}");
    }

    private (string Instrument, long Value, KeyValuePair<string, object?>[] Tags) SingleMeasurement(string name)
    {
        var match = _measurements.Where(m => m.Instrument == name).ToList();
        Assert.Single(match);
        return match[0];
    }

    private static string? TagValue(KeyValuePair<string, object?>[] tags, string key)
        => tags.FirstOrDefault(t => t.Key == key).Value?.ToString();
}
