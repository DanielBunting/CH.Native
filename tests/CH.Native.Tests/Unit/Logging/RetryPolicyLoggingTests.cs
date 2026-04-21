using CH.Native.Resilience;
using CH.Native.Telemetry;
using Microsoft.Extensions.Logging;
using Xunit;

namespace CH.Native.Tests.Unit.Logging;

public class RetryPolicyLoggingTests
{
    [Fact]
    public async Task RetryAttempt_LogsEventId20WithAttemptAndDelay()
    {
        var capture = new CaptureLoggerProvider();
        var logger = new ClickHouseLogger(capture);
        var options = new RetryOptions
        {
            MaxRetries = 2,
            BaseDelay = TimeSpan.FromMilliseconds(1),
            MaxDelay = TimeSpan.FromMilliseconds(5)
        };
        var policy = new RetryPolicy(options, logger);

        int attempts = 0;
        await Assert.ThrowsAnyAsync<Exception>(async () =>
            await policy.ExecuteAsync<bool>(_ =>
            {
                attempts++;
                throw new TimeoutException("transient");
            }));

        var retries = capture.Entries.Where(e => e.EventId.Id == 20).ToList();
        Assert.Equal(2, retries.Count);
        Assert.All(retries, e => Assert.Equal(LogLevel.Warning, e.Level));
        Assert.Equal(3, attempts);
    }
}
