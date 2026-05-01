using System.Diagnostics;
using CH.Native.Connection;
using CH.Native.DependencyInjection;
using CH.Native.Exceptions;
using CH.Native.Resilience;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.DependencyInjection;

/// <summary>
/// Pre-fix a credential-provider exception escaped as its raw type
/// (e.g. <c>Azure.Identity.AuthenticationFailedException</c>) and
/// <see cref="RetryPolicy.IsTransientException"/> would walk the
/// <c>InnerException</c> chain — if it found an <see cref="System.IO.IOException"/>
/// or <see cref="System.Net.Sockets.SocketException"/> nested inside, the
/// failure was misclassified as transient and the entire retry budget burned
/// against a deterministically-broken provider. The Round-1 R1 fix
/// (auth = non-transient) was bypassed for any DI consumer using a credential
/// provider.
/// </summary>
[Trait(Categories.Name, Categories.DependencyInjection)]
public class ProviderFailureRetrySafetyTests
{
    private readonly ITestOutputHelper _output;

    public ProviderFailureRetrySafetyTests(ITestOutputHelper output) => _output = output;

    [Fact]
    public async Task ThrowingJwtProvider_ShortCircuitsRetry()
    {
        var services = new ServiceCollection();

        services.AddClickHouse(b =>
            b.WithHost("127.0.0.1")
             .WithPort(9000)
             .WithAuthMethod(ClickHouseAuthMethod.Jwt)
             .WithResilience(r => r.WithRetry(new RetryOptions
             {
                 MaxRetries = 5,
                 BaseDelay = TimeSpan.FromMilliseconds(500),
                 BackoffMultiplier = 2.0,
             })))
            .WithJwtProvider<AlwaysThrowingJwtProvider>();

        var sp = services.BuildServiceProvider();
        var ds = sp.GetRequiredService<ClickHouseDataSource>();

        var sw = Stopwatch.StartNew();
        var ex = await Assert.ThrowsAnyAsync<Exception>(() => ds.OpenConnectionAsync().AsTask());
        sw.Stop();
        _output.WriteLine($"Provider failure surfaced as {ex.GetType().Name} in {sw.Elapsed.TotalSeconds:F2}s.");

        // Single attempt — full budget would be ~15s.
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(3),
            $"Provider failure retried for {sw.Elapsed.TotalSeconds:F2}s; should short-circuit on auth.");

        // Walk to the inner ClickHouseAuthenticationException — outer may be
        // an aggregate from the retry policy or the bare exception.
        var auth = ex as ClickHouseAuthenticationException
            ?? ex.InnerException as ClickHouseAuthenticationException
            ?? (ex is AggregateException agg
                ? agg.InnerExceptions.OfType<ClickHouseAuthenticationException>().FirstOrDefault()
                : null);
        Assert.NotNull(auth);
        Assert.Contains("JWT", auth!.Message, StringComparison.OrdinalIgnoreCase);
    }

    private sealed class AlwaysThrowingJwtProvider : IClickHouseJwtProvider
    {
        public ValueTask<string> GetTokenAsync(CancellationToken cancellationToken)
            // Wrap an IOException so the pre-fix transient-walk would have classified it.
            => throw new InvalidOperationException(
                "simulated provider failure",
                new System.IO.IOException("network blip"));
    }
}
