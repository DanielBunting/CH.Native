using System.Data.Common;
using CH.Native.Ado;
using Xunit;

namespace CH.Native.Tests.Unit.Ado;

public class ClickHouseProviderFactoryTests
{
    [Fact]
    public void Instance_IsSingleton()
    {
        Assert.Same(ClickHouseProviderFactory.Instance, ClickHouseProviderFactory.Instance);
    }

    [Fact]
    public void CreateConnection_ReturnsClickHouseDbConnection()
    {
        var conn = ClickHouseProviderFactory.Instance.CreateConnection();
        Assert.IsType<ClickHouseDbConnection>(conn);
    }

    [Fact]
    public void CreateCommand_ReturnsClickHouseDbCommand()
    {
        var cmd = ClickHouseProviderFactory.Instance.CreateCommand();
        Assert.IsType<ClickHouseDbCommand>(cmd);
    }

    [Fact]
    public void CreateParameter_ReturnsClickHouseDbParameter()
    {
        var param = ClickHouseProviderFactory.Instance.CreateParameter();
        Assert.IsType<ClickHouseDbParameter>(param);
    }

    [Fact]
    public void CanCreateDataAdapter_IsFalse()
    {
        // ClickHouse has no DataSet/DataAdapter pattern. Pin that.
        Assert.False(ClickHouseProviderFactory.Instance.CanCreateDataAdapter);
    }

    [Fact]
    public void CanCreateCommandBuilder_IsFalse()
    {
        Assert.False(ClickHouseProviderFactory.Instance.CanCreateCommandBuilder);
    }

    [Fact]
    public void Registration_RoundTripsViaDbProviderFactories()
    {
        const string invariantName = "CH.Native.Tests.ProviderFactoryRoundTrip";

        // Defensive: in case a previous run leaked the registration.
        try { DbProviderFactories.UnregisterFactory(invariantName); } catch { }

        DbProviderFactories.RegisterFactory(invariantName, ClickHouseProviderFactory.Instance);
        try
        {
            var resolved = DbProviderFactories.GetFactory(invariantName);
            Assert.Same(ClickHouseProviderFactory.Instance, resolved);
        }
        finally
        {
            DbProviderFactories.UnregisterFactory(invariantName);
        }
    }
}
