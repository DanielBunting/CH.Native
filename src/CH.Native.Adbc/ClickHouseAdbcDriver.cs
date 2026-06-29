using Apache.Arrow.Adbc;
using CH.Native.Connection;

namespace CH.Native.Adbc;

/// <summary>
/// ADBC driver entry point for ClickHouse. Translates an ADBC parameter dictionary into
/// CH.Native connection settings and hands back an <see cref="AdbcDatabase"/>.
/// </summary>
public sealed class ClickHouseAdbcDriver : AdbcDriver
{
    /// <inheritdoc />
    public override AdbcDatabase Open(IReadOnlyDictionary<string, string> parameters)
    {
        ArgumentNullException.ThrowIfNull(parameters);

        var settings = BuildSettings(parameters);
        return new ClickHouseAdbcDatabase(settings);
    }

    internal static ClickHouseConnectionSettings BuildSettings(IReadOnlyDictionary<string, string> parameters)
    {
        // A full connection string takes precedence and is used as-is; discrete option
        // keys are the alternative for callers that prefer structured parameters.
        if (parameters.TryGetValue(AdbcOptionKeys.ConnectionString, out var connectionString)
            && !string.IsNullOrWhiteSpace(connectionString))
        {
            return ClickHouseConnectionSettings.Parse(connectionString);
        }

        var builder = new ClickHouseConnectionSettingsBuilder();

        if (TryGet(parameters, out var host, AdbcOptionKeys.Host))
            builder.WithHost(host);

        if (TryGet(parameters, out var portText, AdbcOptionKeys.Port))
        {
            if (!int.TryParse(portText, out var port))
                throw new ArgumentException($"Invalid '{AdbcOptionKeys.Port}' value: '{portText}'.", nameof(parameters));
            builder.WithPort(port);
        }

        if (TryGet(parameters, out var database, AdbcOptionKeys.Database))
            builder.WithDatabase(database);

        if (TryGet(parameters, out var username, AdbcOptionKeys.Username))
            builder.WithUsername(username);

        if (TryGet(parameters, out var password, AdbcOptionKeys.Password))
            builder.WithPassword(password);

        if (TryGet(parameters, out var tlsText, AdbcOptionKeys.UseTls))
            builder.WithTls(ParseBool(tlsText, AdbcOptionKeys.UseTls));

        return builder.Build();
    }

    private static bool TryGet(IReadOnlyDictionary<string, string> parameters, out string value, string key)
    {
        if (parameters.TryGetValue(key, out var found) && found is not null)
        {
            value = found;
            return true;
        }

        value = string.Empty;
        return false;
    }

    private static bool ParseBool(string value, string key) => value.Trim().ToLowerInvariant() switch
    {
        "1" or "true" or "yes" or "on" => true,
        "0" or "false" or "no" or "off" => false,
        _ => throw new ArgumentException($"Invalid boolean for '{key}': '{value}'."),
    };
}
