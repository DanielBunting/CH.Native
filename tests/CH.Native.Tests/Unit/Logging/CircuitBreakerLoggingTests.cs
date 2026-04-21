using CH.Native.Resilience;
using CH.Native.Telemetry;
using Xunit;

namespace CH.Native.Tests.Unit.Logging;

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
    public async Task RecoveryToClosed_LogsEventId22Information()
    {
        var capture = new CaptureLoggerProvider();
        var logger = new ClickHouseLogger(capture);
        var options = new CircuitBreakerOptions
        {
            FailureThreshold = 1,
            OpenDuration = TimeSpan.FromMilliseconds(30),
            FailureWindow = TimeSpan.FromMinutes(1)
        };
        var breaker = new CircuitBreaker(options, logger)
        {
            ServerAddress = "host:9000"
        };

        breaker.RecordFailure();
        Assert.Equal(CircuitBreakerState.Open, breaker.State);

        await Task.Delay(50);
        _ = breaker.State; // triggers Open -> HalfOpen transition
        breaker.RecordSuccess(); // HalfOpen -> Closed

        Assert.Contains(capture.Entries, e => e.EventId.Id == 22);
    }
}
