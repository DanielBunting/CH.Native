using CH.Native.Exceptions;

namespace CH.Native.Protocol.Messages;

/// <summary>
/// Server hello response received after successful connection handshake.
/// </summary>
public sealed class ServerHello
{
    /// <summary>
    /// Gets the server name (e.g., "ClickHouse").
    /// </summary>
    public required string ServerName { get; init; }

    /// <summary>
    /// Gets the server major version number.
    /// </summary>
    public required int VersionMajor { get; init; }

    /// <summary>
    /// Gets the server minor version number.
    /// </summary>
    public required int VersionMinor { get; init; }

    /// <summary>
    /// Gets the server's protocol revision.
    /// </summary>
    public required int ProtocolRevision { get; init; }

    /// <summary>
    /// Gets the server's timezone (only present if protocol revision >= 54423).
    /// </summary>
    public string? Timezone { get; init; }

    /// <summary>
    /// Gets the server's display name (only present if protocol revision >= 54423).
    /// </summary>
    public string? DisplayName { get; init; }

    /// <summary>
    /// Reads a ServerHello from the protocol reader.
    /// </summary>
    /// <param name="reader">The protocol reader to read from.</param>
    /// <returns>The parsed ServerHello message.</returns>
    /// <exception cref="ClickHouseConnectionException">Thrown if the message type is not Hello.</exception>
    public static ServerHello Read(ref ProtocolReader reader)
    {
        var messageType = (ServerMessageType)reader.ReadVarInt();
        if (messageType == ServerMessageType.Exception)
        {
            // Read exception details from server
            var errorCode = reader.ReadInt32();
            var errorName = reader.ReadString();
            var errorMessage = reader.ReadString();
            var stackTrace = reader.ReadString();

            throw new ClickHouseConnectionException(
                $"Server returned error during handshake: [{errorCode}] {errorName}: {errorMessage}");
        }

        if (messageType != ServerMessageType.Hello)
        {
            throw new ClickHouseConnectionException(
                $"Expected ServerHello (0), got message type {(int)messageType} ({messageType})");
        }

        var serverName = reader.ReadString();
        var versionMajor = (int)reader.ReadVarInt();
        var versionMinor = (int)reader.ReadVarInt();
        var protocolRevision = (int)reader.ReadVarInt();

        string? timezone = null;
        string? displayName = null;

        if (protocolRevision >= ProtocolVersion.WithTimezone)
        {
            timezone = reader.ReadString();
            displayName = reader.ReadString();
        }

        // Read version patch if supported
        if (protocolRevision >= ProtocolVersion.WithVersionPatch)
        {
            _ = reader.ReadVarInt(); // versionPatch - we don't use it
        }

        // Read password complexity rules if supported
        if (protocolRevision >= ProtocolVersion.WithPasswordComplexityRules)
        {
            var rulesCount = reader.ReadVarInt();
            for (ulong i = 0; i < rulesCount; i++)
            {
                _ = reader.ReadString(); // pattern
                _ = reader.ReadString(); // message
            }
        }

        // Read nonce if supported (for interserver secret v2)
        if (protocolRevision >= ProtocolVersion.WithInterServerSecretV2)
        {
            _ = reader.ReadUInt64(); // nonce - we don't use it for non-interserver connections
        }

        return new ServerHello
        {
            ServerName = serverName,
            VersionMajor = versionMajor,
            VersionMinor = versionMinor,
            ProtocolRevision = protocolRevision,
            Timezone = timezone,
            DisplayName = displayName
        };
    }
}
