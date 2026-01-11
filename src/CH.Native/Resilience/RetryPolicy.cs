using System.Net.Sockets;
using CH.Native.Exceptions;
using CH.Native.Telemetry;

namespace CH.Native.Resilience;

/// <summary>
/// Implements retry logic with exponential backoff for transient failures.
/// </summary>
public sealed class RetryPolicy
{
    private readonly RetryOptions _options;

    /// <summary>
    /// ClickHouse error codes considered transient and safe to retry.
    /// </summary>
    private static readonly HashSet<int> TransientErrorCodes = new()
    {
        159,  // TIMEOUT_EXCEEDED
        164,  // READONLY
        209,  // SOCKET_TIMEOUT
        210,  // NETWORK_ERROR
        242,  // TOO_MANY_SIMULTANEOUS_QUERIES
        252   // TOO_SLOW
    };

    /// <summary>
    /// Occurs when a retry attempt is about to be made.
    /// </summary>
    public event EventHandler<RetryEventArgs>? OnRetry;

    /// <summary>
    /// Creates a new retry policy with the specified options.
    /// </summary>
    /// <param name="options">The retry options, or null to use defaults.</param>
    public RetryPolicy(RetryOptions? options = null)
    {
        _options = options ?? RetryOptions.Default;
    }

    /// <summary>
    /// Executes an action with retry logic.
    /// </summary>
    /// <typeparam name="T">The return type of the action.</typeparam>
    /// <param name="action">The action to execute.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The result of the action.</returns>
    /// <exception cref="AggregateException">Thrown when all retry attempts fail.</exception>
    public async Task<T> ExecuteAsync<T>(
        Func<CancellationToken, Task<T>> action,
        CancellationToken cancellationToken = default)
    {
        var attempt = 0;
        var exceptions = new List<Exception>();

        while (true)
        {
            try
            {
                return await action(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex) when (attempt < _options.MaxRetries && ShouldRetryException(ex))
            {
                exceptions.Add(ex);
                attempt++;

                var delay = CalculateDelay(attempt);

                // Record retry telemetry
                ClickHouseMeter.RecordRetry(attempt, delay, ex.GetType().Name);

                // Raise retry event before delay
                OnRetry?.Invoke(this, new RetryEventArgs(attempt, _options.MaxRetries, ex, delay));

                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
                if (exceptions.Count == 1)
                {
                    throw;
                }
                throw new AggregateException(
                    $"All {attempt + 1} attempts failed. See inner exceptions for details.",
                    exceptions);
            }
        }
    }

    /// <summary>
    /// Executes an action with retry logic (void return).
    /// </summary>
    /// <param name="action">The action to execute.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async Task ExecuteAsync(
        Func<CancellationToken, Task> action,
        CancellationToken cancellationToken = default)
    {
        await ExecuteAsync(async ct =>
        {
            await action(ct).ConfigureAwait(false);
            return true;
        }, cancellationToken).ConfigureAwait(false);
    }

    private bool ShouldRetryException(Exception ex)
    {
        if (_options.ShouldRetry != null)
        {
            return _options.ShouldRetry(ex);
        }

        return IsTransientException(ex);
    }

    /// <summary>
    /// Determines if an exception represents a transient error that can be retried.
    /// </summary>
    /// <param name="ex">The exception to evaluate.</param>
    /// <returns>True if the exception is transient and the operation should be retried.</returns>
    public static bool IsTransientException(Exception ex)
    {
        return ex switch
        {
            SocketException => true,
            TimeoutException => true,
            IOException => true,
            ClickHouseConnectionException => true,
            ClickHouseServerException serverEx => TransientErrorCodes.Contains(serverEx.ErrorCode),
            AggregateException aggEx => aggEx.InnerExceptions.Any(IsTransientException),
            _ when ex.InnerException != null => IsTransientException(ex.InnerException),
            _ => false
        };
    }

    /// <summary>
    /// Determines if a ClickHouse error code represents a transient error.
    /// </summary>
    /// <param name="errorCode">The ClickHouse error code.</param>
    /// <returns>True if the error code is transient.</returns>
    public static bool IsTransientErrorCode(int errorCode)
    {
        return TransientErrorCodes.Contains(errorCode);
    }

    private TimeSpan CalculateDelay(int attempt)
    {
        // Exponential backoff: baseDelay * multiplier^(attempt-1)
        var exponentialDelayMs = _options.BaseDelay.TotalMilliseconds
            * Math.Pow(_options.BackoffMultiplier, attempt - 1);

        // Cap at max delay
        var cappedDelayMs = Math.Min(exponentialDelayMs, _options.MaxDelay.TotalMilliseconds);

        // Add jitter (0-25% of delay) to prevent thundering herd
        var jitterMs = cappedDelayMs * Random.Shared.NextDouble() * 0.25;

        return TimeSpan.FromMilliseconds(cappedDelayMs + jitterMs);
    }
}
