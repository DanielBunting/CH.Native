namespace CH.Native.Resilience;

/// <summary>
/// Represents a ClickHouse server address with host and port.
/// </summary>
/// <param name="Host">The server hostname or IP address.</param>
/// <param name="Port">The native protocol port (default 9000).</param>
public readonly record struct ServerAddress(string Host, int Port = 9000)
{
    /// <summary>
    /// Returns the server address as "host:port".
    /// </summary>
    public override string ToString() => $"{Host}:{Port}";

    /// <summary>
    /// Parses a server address from a string in the format "host" or "host:port".
    /// </summary>
    /// <param name="hostPort">The string to parse.</param>
    /// <returns>The parsed server address.</returns>
    /// <exception cref="ArgumentException">Thrown if the string format is invalid.</exception>
    public static ServerAddress Parse(string hostPort)
    {
        ArgumentNullException.ThrowIfNull(hostPort);

        var colonIndex = hostPort.LastIndexOf(':');

        // No colon - just host with default port
        if (colonIndex < 0)
        {
            return new ServerAddress(hostPort.Trim());
        }

        // Check for IPv6 address in brackets [::1]:port
        if (hostPort.StartsWith('['))
        {
            var closeBracket = hostPort.IndexOf(']');
            if (closeBracket < 0)
            {
                throw new ArgumentException($"Invalid server address format: {hostPort}. Missing closing bracket for IPv6 address.");
            }

            var ipv6Host = hostPort[1..closeBracket];

            // Check if there's a port after the bracket
            if (closeBracket + 1 < hostPort.Length && hostPort[closeBracket + 1] == ':')
            {
                var portStr = hostPort[(closeBracket + 2)..].Trim();
                if (!int.TryParse(portStr, out var port) || port < 1 || port > 65535)
                {
                    throw new ArgumentException($"Invalid port in server address: {hostPort}");
                }
                return new ServerAddress(ipv6Host, port);
            }

            return new ServerAddress(ipv6Host);
        }

        // Regular host:port format
        var host = hostPort[..colonIndex].Trim();
        var portString = hostPort[(colonIndex + 1)..].Trim();

        if (string.IsNullOrEmpty(host))
        {
            throw new ArgumentException($"Invalid server address format: {hostPort}. Host cannot be empty.");
        }

        if (string.IsNullOrEmpty(portString))
        {
            return new ServerAddress(host);
        }

        if (!int.TryParse(portString, out var parsedPort) || parsedPort < 1 || parsedPort > 65535)
        {
            throw new ArgumentException($"Invalid port in server address: {hostPort}. Port must be between 1 and 65535.");
        }

        return new ServerAddress(host, parsedPort);
    }

    /// <summary>
    /// Tries to parse a server address from a string.
    /// </summary>
    /// <param name="hostPort">The string to parse.</param>
    /// <param name="address">The parsed address if successful.</param>
    /// <returns>True if parsing succeeded, false otherwise.</returns>
    public static bool TryParse(string hostPort, out ServerAddress address)
    {
        try
        {
            address = Parse(hostPort);
            return true;
        }
        catch
        {
            address = default;
            return false;
        }
    }
}
