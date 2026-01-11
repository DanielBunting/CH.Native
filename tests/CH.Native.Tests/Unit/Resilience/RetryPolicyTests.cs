using System.Diagnostics;
using System.Net.Sockets;
using CH.Native.Exceptions;
using CH.Native.Resilience;
using Xunit;

namespace CH.Native.Tests.Unit.Resilience;

public class RetryPolicyTests
{
    [Fact]
    public async Task ExecuteAsync_SuccessOnFirstAttempt_ReturnsResult()
    {
        var policy = new RetryPolicy();
        var callCount = 0;

        var result = await policy.ExecuteAsync(async _ =>
        {
            callCount++;
            await Task.Yield();
            return 42;
        });

        Assert.Equal(42, result);
        Assert.Equal(1, callCount);
    }

    [Fact]
    public async Task ExecuteAsync_TransientFailureThenSuccess_Retries()
    {
        var policy = new RetryPolicy();
        var callCount = 0;

        var result = await policy.ExecuteAsync(async _ =>
        {
            callCount++;
            await Task.Yield();
            if (callCount < 3)
                throw new SocketException();
            return 42;
        });

        Assert.Equal(42, result);
        Assert.Equal(3, callCount);
    }

    [Fact]
    public async Task ExecuteAsync_ExceedsMaxRetries_ThrowsAggregateException()
    {
        var options = new RetryOptions { MaxRetries = 2, BaseDelay = TimeSpan.FromMilliseconds(1) };
        var policy = new RetryPolicy(options);
        var callCount = 0;

        var ex = await Assert.ThrowsAsync<AggregateException>(async () =>
        {
            await policy.ExecuteAsync(async _ =>
            {
                callCount++;
                await Task.Yield();
                throw new SocketException();
#pragma warning disable CS0162
                return 42;
#pragma warning restore CS0162
            });
        });

        Assert.Equal(3, callCount); // 1 initial + 2 retries
        Assert.Equal(3, ex.InnerExceptions.Count);
        Assert.All(ex.InnerExceptions, e => Assert.IsType<SocketException>(e));
    }

    [Fact]
    public async Task ExecuteAsync_NonTransientException_ThrowsImmediately()
    {
        var policy = new RetryPolicy();
        var callCount = 0;

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await policy.ExecuteAsync(async _ =>
            {
                callCount++;
                await Task.Yield();
                throw new InvalidOperationException("Not transient");
#pragma warning disable CS0162
                return 42;
#pragma warning restore CS0162
            });
        });

        Assert.Equal(1, callCount); // No retries for non-transient
    }

    [Theory]
    [InlineData(typeof(SocketException))]
    [InlineData(typeof(TimeoutException))]
    [InlineData(typeof(IOException))]
    public void IsTransientException_RecognizesTransientTypes(Type exceptionType)
    {
        var ex = (Exception)Activator.CreateInstance(exceptionType)!;
        Assert.True(RetryPolicy.IsTransientException(ex));
    }

    [Fact]
    public void IsTransientException_ClickHouseConnectionException_IsTransient()
    {
        var ex = new ClickHouseConnectionException("Connection failed");
        Assert.True(RetryPolicy.IsTransientException(ex));
    }

    [Theory]
    [InlineData(159, true)]  // TIMEOUT_EXCEEDED
    [InlineData(164, true)]  // READONLY
    [InlineData(209, true)]  // SOCKET_TIMEOUT
    [InlineData(210, true)]  // NETWORK_ERROR
    [InlineData(242, true)]  // TOO_MANY_SIMULTANEOUS_QUERIES
    [InlineData(252, true)]  // TOO_SLOW
    [InlineData(62, false)]  // SYNTAX_ERROR (not transient)
    [InlineData(60, false)]  // TABLE_ALREADY_EXISTS (not transient)
    public void IsTransientException_ClickHouseServerException_ChecksErrorCode(int errorCode, bool isTransient)
    {
        var ex = new ClickHouseServerException(errorCode, "DB::Exception", "Test error", "");
        Assert.Equal(isTransient, RetryPolicy.IsTransientException(ex));
    }

    [Fact]
    public void IsTransientException_InvalidOperationException_NotTransient()
    {
        var ex = new InvalidOperationException("Not transient");
        Assert.False(RetryPolicy.IsTransientException(ex));
    }

    [Fact]
    public void IsTransientException_AggregateException_ChecksInnerExceptions()
    {
        var inner = new SocketException();
        var aggregate = new AggregateException(inner);
        Assert.True(RetryPolicy.IsTransientException(aggregate));

        var notTransient = new AggregateException(new InvalidOperationException());
        Assert.False(RetryPolicy.IsTransientException(notTransient));
    }

    [Fact]
    public async Task ExecuteAsync_AppliesExponentialBackoff()
    {
        var options = new RetryOptions
        {
            MaxRetries = 3,
            BaseDelay = TimeSpan.FromMilliseconds(50),
            BackoffMultiplier = 2.0,
            MaxDelay = TimeSpan.FromSeconds(10)
        };
        var policy = new RetryPolicy(options);
        var delays = new List<TimeSpan>();
        var callCount = 0;
        var lastCall = Stopwatch.GetTimestamp();

        var ex = await Assert.ThrowsAsync<AggregateException>(async () =>
        {
            await policy.ExecuteAsync(async _ =>
            {
                var now = Stopwatch.GetTimestamp();
                callCount++;
                // Skip recording first call (no retry delay yet), record subsequent calls
                if (callCount > 1)
                {
                    delays.Add(Stopwatch.GetElapsedTime(lastCall));
                }
                lastCall = now;
                await Task.Yield();
                throw new SocketException();
#pragma warning disable CS0162
                return 42;
#pragma warning restore CS0162
            });
        });

        Assert.Equal(3, delays.Count);
        // Verify delays are increasing (exponential backoff)
        // Use generous ranges due to CI environment timing variability
        // First delay should be at least ~40ms (50ms base minus jitter)
        Assert.True(delays[0].TotalMilliseconds >= 30, $"First delay {delays[0].TotalMilliseconds}ms should be >= 30ms");
        // Each subsequent delay should be longer than the previous (exponential growth)
        Assert.True(delays[1] >= delays[0], $"Second delay {delays[1].TotalMilliseconds}ms should be >= first {delays[0].TotalMilliseconds}ms");
        Assert.True(delays[2] >= delays[1], $"Third delay {delays[2].TotalMilliseconds}ms should be >= second {delays[1].TotalMilliseconds}ms");
    }

    [Fact]
    public async Task ExecuteAsync_RespectsMaxDelay()
    {
        var options = new RetryOptions
        {
            MaxRetries = 3,
            BaseDelay = TimeSpan.FromMilliseconds(100),
            BackoffMultiplier = 100.0, // Would exceed max without cap
            MaxDelay = TimeSpan.FromMilliseconds(150)
        };
        var policy = new RetryPolicy(options);
        var delays = new List<TimeSpan>();
        var callCount = 0;
        var lastCall = Stopwatch.GetTimestamp();

        var ex = await Assert.ThrowsAsync<AggregateException>(async () =>
        {
            await policy.ExecuteAsync(async _ =>
            {
                var now = Stopwatch.GetTimestamp();
                callCount++;
                // Skip recording first call (no retry delay yet), record subsequent calls
                if (callCount > 1)
                {
                    delays.Add(Stopwatch.GetElapsedTime(lastCall));
                }
                lastCall = now;
                await Task.Yield();
                throw new SocketException();
#pragma warning disable CS0162
                return 42;
#pragma warning restore CS0162
            });
        });

        // All delays after the first should be capped at ~150ms (plus jitter)
        foreach (var delay in delays.Skip(1))
        {
            Assert.InRange(delay.TotalMilliseconds, 100, 220); // Max + 25% jitter + tolerance
        }
    }

    [Fact]
    public async Task ExecuteAsync_CancellationToken_Honored()
    {
        var policy = new RetryPolicy();
        var cts = new CancellationTokenSource();
        var callCount = 0;

        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await policy.ExecuteAsync(async ct =>
            {
                callCount++;
                if (callCount == 1)
                    cts.Cancel();
                ct.ThrowIfCancellationRequested();
                await Task.Delay(100, ct);
                throw new SocketException();
#pragma warning disable CS0162
                return 42;
#pragma warning restore CS0162
            }, cts.Token);
        });

        Assert.Equal(1, callCount);
    }

    [Fact]
    public async Task ExecuteAsync_CustomShouldRetry_IsRespected()
    {
        var options = new RetryOptions
        {
            MaxRetries = 3,
            BaseDelay = TimeSpan.FromMilliseconds(1),
            ShouldRetry = ex => ex is InvalidOperationException // Normally not transient
        };
        var policy = new RetryPolicy(options);
        var callCount = 0;

        var result = await policy.ExecuteAsync(async _ =>
        {
            callCount++;
            await Task.Yield();
            if (callCount < 3)
                throw new InvalidOperationException();
            return 42;
        });

        Assert.Equal(42, result);
        Assert.Equal(3, callCount);
    }

    [Fact]
    public async Task ExecuteAsync_VoidOverload_Works()
    {
        var policy = new RetryPolicy();
        var callCount = 0;

        await policy.ExecuteAsync(async _ =>
        {
            callCount++;
            await Task.Yield();
            if (callCount < 2)
                throw new SocketException();
        });

        Assert.Equal(2, callCount);
    }

    [Fact]
    public void IsTransientErrorCode_ReturnsCorrectly()
    {
        Assert.True(RetryPolicy.IsTransientErrorCode(159));
        Assert.True(RetryPolicy.IsTransientErrorCode(210));
        Assert.False(RetryPolicy.IsTransientErrorCode(62));
        Assert.False(RetryPolicy.IsTransientErrorCode(0));
    }

    [Fact]
    public async Task OnRetry_Event_RaisedOnRetry()
    {
        var options = new RetryOptions { MaxRetries = 3, BaseDelay = TimeSpan.FromMilliseconds(1) };
        var policy = new RetryPolicy(options);
        var retryEvents = new List<RetryEventArgs>();

        policy.OnRetry += (_, args) => retryEvents.Add(args);

        var callCount = 0;
        var result = await policy.ExecuteAsync(async _ =>
        {
            callCount++;
            await Task.Yield();
            if (callCount < 3)
                throw new SocketException();
            return 42;
        });

        Assert.Equal(42, result);
        Assert.Equal(2, retryEvents.Count);

        // First retry
        Assert.Equal(1, retryEvents[0].AttemptNumber);
        Assert.Equal(3, retryEvents[0].MaxRetries);
        Assert.IsType<SocketException>(retryEvents[0].Exception);
        Assert.True(retryEvents[0].Delay > TimeSpan.Zero);

        // Second retry
        Assert.Equal(2, retryEvents[1].AttemptNumber);
        Assert.Equal(3, retryEvents[1].MaxRetries);
    }

    [Fact]
    public async Task OnRetry_Event_NotRaisedOnSuccess()
    {
        var policy = new RetryPolicy();
        var retryCount = 0;

        policy.OnRetry += (_, _) => retryCount++;

        await policy.ExecuteAsync(async _ =>
        {
            await Task.Yield();
            return 42;
        });

        Assert.Equal(0, retryCount);
    }

    [Fact]
    public async Task OnRetry_Event_NotRaisedForNonTransient()
    {
        var policy = new RetryPolicy();
        var retryCount = 0;

        policy.OnRetry += (_, _) => retryCount++;

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await policy.ExecuteAsync(async _ =>
            {
                await Task.Yield();
                throw new InvalidOperationException();
#pragma warning disable CS0162
                return 42;
#pragma warning restore CS0162
            });
        });

        Assert.Equal(0, retryCount);
    }
}
