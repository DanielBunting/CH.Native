using System.Data.Common;
using CH.Native.Commands;
using CH.Native.Connection;

namespace CH.Native.Ado;

/// <summary>
/// A factory for creating ClickHouse ADO.NET provider objects.
/// </summary>
public sealed class ClickHouseProviderFactory : DbProviderFactory
{
    /// <summary>
    /// Gets the singleton instance of the provider factory.
    /// </summary>
    public static readonly ClickHouseProviderFactory Instance = new();

    private ClickHouseProviderFactory()
    {
    }

    /// <inheritdoc />
    public override DbConnection CreateConnection() => new ClickHouseConnection();

    /// <inheritdoc />
    public override DbCommand CreateCommand() => new ClickHouseCommand();

    /// <inheritdoc />
    public override DbParameter CreateParameter() => new ClickHouseDbParameter();

    /// <inheritdoc />
    public override bool CanCreateDataAdapter => false;

    /// <inheritdoc />
    public override bool CanCreateCommandBuilder => false;
}
