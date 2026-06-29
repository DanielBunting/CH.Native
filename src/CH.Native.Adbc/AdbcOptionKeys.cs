namespace CH.Native.Adbc;

/// <summary>
/// Option keys understood by <see cref="ClickHouseAdbcDriver.Open"/> when constructing a
/// ClickHouse connection from an ADBC parameter dictionary.
/// </summary>
/// <remarks>
/// The canonical ADBC keys (<c>username</c>, <c>password</c>) follow the Arrow ADBC spec.
/// In addition, a full CH.Native connection string can be supplied under
/// <see cref="ConnectionString"/>, in which case the remaining keys layer on top of it.
/// </remarks>
public static class AdbcOptionKeys
{
    /// <summary>A complete CH.Native connection string (e.g. <c>Host=localhost;Port=9000;...</c>).</summary>
    public const string ConnectionString = "ch.native.connection_string";

    /// <summary>ClickHouse server host. Defaults to <c>localhost</c>.</summary>
    public const string Host = "ch.native.host";

    /// <summary>ClickHouse native protocol port. Defaults to <c>9000</c>.</summary>
    public const string Port = "ch.native.port";

    /// <summary>Default database. Defaults to <c>default</c>.</summary>
    public const string Database = "ch.native.database";

    /// <summary>Username. The standard ADBC key <c>username</c> is also accepted.</summary>
    public const string Username = "username";

    /// <summary>Password. The standard ADBC key <c>password</c> is also accepted.</summary>
    public const string Password = "password";

    /// <summary>Enable TLS (<c>true</c>/<c>false</c>). Defaults to <c>false</c>.</summary>
    public const string UseTls = "ch.native.tls";
}
