using Xunit;

namespace CH.Native.Tests.Unit.Telemetry;

/// <summary>
/// Serialises every unit-test class that either observes
/// <see cref="CH.Native.Telemetry.ClickHouseMeter"/> via a
/// <c>MeterListener</c> or emits onto it via real
/// <c>CircuitBreaker</c>/<c>RetryPolicy</c> instances. The meter is a
/// process-wide static, so listeners attached in one test class receive
/// emissions from any other class running concurrently — leading to
/// non-deterministic <c>Assert.Single</c> / <c>Assert.Empty</c> /
/// <c>Assert.Equal(n, ...)</c> failures under parallel xUnit execution.
/// </summary>
[CollectionDefinition("MeterTests")]
public class MeterTestsCollection { }
