namespace CH.Native.Resilience;

/// <summary>
/// Configuration options for retry behavior.
/// </summary>
public sealed record RetryOptions
{
    /// <summary>
    /// Default retry options with 3 retries, 100ms base delay, and 2x exponential backoff.
    /// </summary>
    public static readonly RetryOptions Default = new();

    /// <summary>
    /// Gets the maximum number of retry attempts. Default is 3.
    /// </summary>
    public int MaxRetries { get; init; } = 3;

    /// <summary>
    /// Gets the initial delay between retries. Default is 100ms.
    /// </summary>
    public TimeSpan BaseDelay { get; init; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Gets the multiplier for exponential backoff. Default is 2.0.
    /// </summary>
    public double BackoffMultiplier { get; init; } = 2.0;

    /// <summary>
    /// Gets the maximum delay between retries. Default is 30 seconds.
    /// </summary>
    public TimeSpan MaxDelay { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets a custom function to determine if an exception should trigger a retry.
    /// If null, the default transient error detection is used.
    /// </summary>
    public Func<Exception, bool>? ShouldRetry { get; init; }
}
