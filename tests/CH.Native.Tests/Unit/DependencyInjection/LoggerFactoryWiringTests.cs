using CH.Native.Connection;
using CH.Native.DependencyInjection;
using CH.Native.Tests.Unit.Logging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace CH.Native.Tests.Unit.DependencyInjection;

/// <summary>
/// Covers the DI integration supplying the container's <see cref="ILoggerFactory"/> to the data
/// source as a fallback, without overriding an explicitly configured factory. (Ported from the
/// driver's DataSourceInjectsLoggerFactoryFromDI / LoggerFactory-propagation tests.)
/// </summary>
public class LoggerFactoryWiringTests
{
    [Fact]
    public void ContainerLoggerFactory_IsWiredIntoDataSource()
    {
        ILoggerFactory factory = new CaptureLoggerProvider();
        var services = new ServiceCollection();
        services.AddSingleton(factory);
        services.AddClickHouse("Host=localhost;Port=9000;Database=default");

        using var provider = services.BuildServiceProvider();
        var dataSource = provider.GetRequiredService<ClickHouseDataSource>();

        Assert.Same(factory, dataSource.Settings.Telemetry?.LoggerFactory);
    }

    [Fact]
    public void ExplicitLoggerFactory_IsNotOverriddenByContainer()
    {
        ILoggerFactory containerFactory = new CaptureLoggerProvider();
        ILoggerFactory explicitFactory = new CaptureLoggerProvider();
        var services = new ServiceCollection();
        services.AddSingleton(containerFactory);
        services.AddClickHouse(builder => builder
            .WithHost("localhost")
            .WithPort(9000)
            .WithLoggerFactory(explicitFactory));

        using var provider = services.BuildServiceProvider();
        var dataSource = provider.GetRequiredService<ClickHouseDataSource>();

        Assert.Same(explicitFactory, dataSource.Settings.Telemetry?.LoggerFactory);
    }
}
