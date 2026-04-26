using CH.Native.Resilience;
using CH.Native.Telemetry;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace CH.Native.Tests.Unit.Logging;

[Collection("MeterTests")]
public class CircuitBreakerLoggingTests
{
    [Fact]
    public void OpeningCircuit_LogsEventId21Warning()
    {
        var capture = new CaptureLoggerProvider();
        var logger = new ClickHouseLogger(capture);
        var options = new CircuitBreakerOptions
        {
            FailureThreshold = 2,
            OpenDuration = TimeSpan.FromMilliseconds(50),
            FailureWindow = TimeSpan.FromMinutes(1)
        };
        var breaker = new CircuitBreaker(options, logger)
        {
            ServerAddress = "host:9000"
        };

        breaker.RecordFailure();
        breaker.RecordFailure();

        var transition = Assert.Single(capture.Entries, e => e.EventId.Id == 21);
        Assert.Equal(Microsoft.Extensions.Logging.LogLevel.Warning, transition.Level);
    }

    [Fact]
    public void RecoveryToClosed_LogsEventId22Information()
    {
        var capture = new CaptureLoggerProvider();
        var logger = new ClickHouseLogger(capture);
        var time = new FakeTimeProvider();
        var options = new CircuitBreakerOptions
        {
            FailureThreshold = 1,
            OpenDuration = TimeSpan.FromSeconds(30),
            FailureWindow = TimeSpan.FromMinutes(1)
        };
        var breaker = new CircuitBreaker(options, logger, time)
        {
            ServerAddress = "host:9000"
        };

        breaker.RecordFailure();
        Assert.Equal(CircuitBreakerState.Open, breaker.State);

        time.Advance(TimeSpan.FromSeconds(31));
        _ = breaker.State; // triggers Open -> HalfOpen transition
        breaker.RecordSuccess(); // HalfOpen -> Closed

        Assert.Contains(capture.Entries, e => e.EventId.Id == 22);
    }
}
