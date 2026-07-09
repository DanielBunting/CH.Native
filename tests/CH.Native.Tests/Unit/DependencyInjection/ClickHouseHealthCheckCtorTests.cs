using CH.Native.DependencyInjection.HealthChecks;
using Xunit;

namespace CH.Native.Tests.Unit.DependencyInjection;

public class ClickHouseHealthCheckCtorTests
{
    [Fact]
    public void Ctor_NullDataSource_Throws() =>
        Assert.Throws<ArgumentNullException>(() => new ClickHouseHealthCheck(null!, null));
}
