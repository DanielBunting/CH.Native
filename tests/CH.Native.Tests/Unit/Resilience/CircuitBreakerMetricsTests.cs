using System.Diagnostics.Metrics;
using CH.Native.Resilience;
using CH.Native.Telemetry;
using Xunit;

namespace CH.Native.Tests.Unit.Resilience;

/// <summary>
/// Pins that every state transition increments the
/// <c>ch_native_circuit_breaker_state_changes_total</c> counter exactly once
/// and emits the expected from_state/to_state/server.address tag triple.
/// Uses a MeterListener so the unit test project doesn't need the OpenTelemetry
/// SDK packages — we observe the counter at the source.
/// </summary>
public class CircuitBreakerMetricsTests
{
    private const string CounterName = "ch_native_circuit_breaker_state_changes_total";

    private sealed record Sample(long Value, string? From, string? To, string? Server);

    private sealed class Recorder : IDisposable
    {
        private readonly MeterListener _listener;
        private readonly List<Sample> _samples = new();
        private readonly object _gate = new();

        public Recorder()
        {
            _listener = new MeterListener
            {
                InstrumentPublished = (instrument, l) =>
                {
                    if (instrument.Meter.Name == ClickHouseMeter.Name && instrument.Name == CounterName)
                        l.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<long>(OnMeasurement);
            _listener.Start();
        }

        public IReadOnlyList<Sample> Samples
        {
            get { lock (_gate) return _samples.ToArray(); }
        }

        private void OnMeasurement(Instrument instrument, long value, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
        {
            string? from = null, to = null, server = null;
            foreach (var t in tags)
            {
                if (t.Key == "from_state") from = t.Value as string;
                else if (t.Key == "to_state") to = t.Value as string;
                else if (t.Key == "server.address") server = t.Value as string;
            }
            lock (_gate) _samples.Add(new Sample(value, from, to, server));
        }

        public void Dispose() => _listener.Dispose();
    }

    [Fact]
    public void OnFailure_ClosedToOpen_RecordsSingleTransitionWithTags()
    {
        using var rec = new Recorder();
        var breaker = new CircuitBreaker(new CircuitBreakerOptions { FailureThreshold = 2 })
        {
            ServerAddress = "host-a:9000",
        };

        breaker.RecordFailure();
        breaker.RecordFailure();

        Assert.Equal(CircuitBreakerState.Open, breaker.State);
        Assert.Single(rec.Samples);
        var s = rec.Samples[0];
        Assert.Equal(1L, s.Value);
        Assert.Equal("closed", s.From);
        Assert.Equal("open", s.To);
        Assert.Equal("host-a:9000", s.Server);
    }

    [Fact]
    public void OnFailure_BelowThreshold_DoesNotRecord()
    {
        using var rec = new Recorder();
        var breaker = new CircuitBreaker(new CircuitBreakerOptions { FailureThreshold = 5 });

        for (var i = 0; i < 4; i++) breaker.RecordFailure();

        Assert.Equal(CircuitBreakerState.Closed, breaker.State);
        Assert.Empty(rec.Samples);
    }

    [Fact]
    public void StateGetter_OpenToHalfOpen_RecordsTransition()
    {
        using var rec = new Recorder();
        var breaker = new CircuitBreaker(new CircuitBreakerOptions
        {
            FailureThreshold = 1,
            OpenDuration = TimeSpan.FromMilliseconds(50),
        });

        breaker.RecordFailure();
        Assert.Equal(CircuitBreakerState.Open, breaker.State);

        Thread.Sleep(120);

        Assert.Equal(CircuitBreakerState.HalfOpen, breaker.State);

        Assert.Equal(2, rec.Samples.Count);
        Assert.Equal(("closed", "open"), (rec.Samples[0].From, rec.Samples[0].To));
        Assert.Equal(("open", "halfopen"), (rec.Samples[1].From, rec.Samples[1].To));
    }

    [Fact]
    public void OnSuccess_HalfOpenToClosed_RecordsTransition()
    {
        using var rec = new Recorder();
        var breaker = new CircuitBreaker(new CircuitBreakerOptions
        {
            FailureThreshold = 1,
            OpenDuration = TimeSpan.FromMilliseconds(50),
        });

        breaker.RecordFailure();
        Thread.Sleep(120);
        _ = breaker.State; // trip Open→HalfOpen

        breaker.RecordSuccess();

        Assert.Equal(CircuitBreakerState.Closed, breaker.State);
        Assert.Equal(3, rec.Samples.Count);
        Assert.Equal(("halfopen", "closed"), (rec.Samples[2].From, rec.Samples[2].To));
    }

    [Fact]
    public void OnFailure_HalfOpenToOpen_RecordsTransition()
    {
        using var rec = new Recorder();
        var breaker = new CircuitBreaker(new CircuitBreakerOptions
        {
            FailureThreshold = 1,
            OpenDuration = TimeSpan.FromMilliseconds(50),
        });

        breaker.RecordFailure();
        Thread.Sleep(120);
        _ = breaker.State; // trip Open→HalfOpen

        breaker.RecordFailure();

        Assert.Equal(CircuitBreakerState.Open, breaker.State);
        Assert.Equal(3, rec.Samples.Count);
        Assert.Equal(("halfopen", "open"), (rec.Samples[2].From, rec.Samples[2].To));
    }

    [Fact]
    public void Reset_FromOpen_RecordsTransition()
    {
        using var rec = new Recorder();
        var breaker = new CircuitBreaker(new CircuitBreakerOptions { FailureThreshold = 1 });

        breaker.RecordFailure();
        Assert.Equal(CircuitBreakerState.Open, breaker.State);

        breaker.Reset();

        Assert.Equal(2, rec.Samples.Count);
        Assert.Equal(("open", "closed"), (rec.Samples[1].From, rec.Samples[1].To));
    }

    [Fact]
    public void Reset_FromClosed_DoesNotRecord()
    {
        using var rec = new Recorder();
        var breaker = new CircuitBreaker();

        breaker.Reset();

        Assert.Empty(rec.Samples);
    }

    [Fact]
    public void DefaultServerAddress_IsEmittedWhenNotSet()
    {
        using var rec = new Recorder();
        var breaker = new CircuitBreaker(new CircuitBreakerOptions { FailureThreshold = 1 });

        breaker.RecordFailure();

        Assert.Single(rec.Samples);
        Assert.Equal("default", rec.Samples[0].Server);
    }

    [Fact]
    public async Task HalfOpenToOpen_RaceLoser_DoesNotDoubleCount()
    {
        // Two threads observe the breaker as HalfOpen, then both call RecordFailure.
        // The winner flips HalfOpen→Open and emits one transition; the loser sees
        // _state == Open and must not emit a second event.
        using var rec = new Recorder();
        var breaker = new CircuitBreaker(new CircuitBreakerOptions
        {
            FailureThreshold = 1,
            OpenDuration = TimeSpan.FromMilliseconds(50),
        });

        breaker.RecordFailure();
        await Task.Delay(120);
        _ = breaker.State; // Open→HalfOpen

        var samplesBefore = rec.Samples.Count;

        var barrier = new Barrier(2);
        var t1 = Task.Run(() => { barrier.SignalAndWait(); breaker.RecordFailure(); });
        var t2 = Task.Run(() => { barrier.SignalAndWait(); breaker.RecordFailure(); });
        await Task.WhenAll(t1, t2);

        var halfToOpen = rec.Samples
            .Skip(samplesBefore)
            .Count(s => s.From == "halfopen" && s.To == "open");
        Assert.Equal(1, halfToOpen);
    }
}
