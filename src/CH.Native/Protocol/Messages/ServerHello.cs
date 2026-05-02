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
    /// Server's declared send-side chunked-packets capability (revision 54470+),
    /// e.g. <c>"notchunked"</c>, <c>"chunked"</c>, or <c>"chunked_optional"</c>.
    /// <see langword="null"/> on older revisions.
    /// </summary>
    public string? ProtoSendChunkedServer { get; init; }

    /// <summary>
    /// Server's declared receive-side chunked-packets capability (revision 54470+).
    /// </summary>
    public string? ProtoRecvChunkedServer { get; init; }

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

            // Permanent auth/authorization failures during handshake are non-transient;
            // raise them as ClickHouseAuthenticationException so the retry layer
            // short-circuits instead of burning the retry budget on something that
            // can never succeed with the same credentials.
            if (IsAuthenticationErrorCode(errorCode))
            {
                throw new ClickHouseAuthenticationException(
                    $"Server rejected handshake: [{errorCode}] {errorName}: {errorMessage}")
                {
                    ErrorCode = errorCode,
                    ServerExceptionName = errorName,
                };
            }

            // Other server-side handshake errors get the structured server-exception
            // type so the retry policy can consult its transient-error-code list.
            throw new ClickHouseServerException(errorCode, errorName, errorMessage, stackTrace);
        }

        if (messageType != ServerMessageType.Hello)
        {
            throw new ClickHouseConnectionException(
                $"Expected ServerHello (0), got message type {(int)messageType} ({messageType})");
        }

        var serverName = reader.ReadString();
        var versionMajor = reader.ReadVarIntAsInt32("ServerHello versionMajor");
        var versionMinor = reader.ReadVarIntAsInt32("ServerHello versionMinor");
        var protocolRevision = reader.ReadVarIntAsInt32("ServerHello protocolRevision");

        // Server wire order (see TCPHandler::sendHello): parallel-replicas version (54471),
        // then timezone/display name (54423), then version patch, then chunked-packets
        // caps (54470), then password rules, then nonce.
        if (protocolRevision >= ProtocolVersion.WithVersionedParallelReplicas)
        {
            _ = reader.ReadVarInt(); // server parallel replicas protocol version
        }

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

        // Read the server's chunked-packets capability. We always advertise
        // "notchunked" in the client addendum; capture what the server says so
        // ClickHouseConnection can refuse a chunked-only server up-front
        // instead of silently sending un-chunked frames that desync the wire.
        string? protoSendChunkedSrv = null;
        string? protoRecvChunkedSrv = null;
        if (protocolRevision >= ProtocolVersion.WithChunkedPackets)
        {
            protoSendChunkedSrv = reader.ReadString();
            protoRecvChunkedSrv = reader.ReadString();
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
            DisplayName = displayName,
            ProtoSendChunkedServer = protoSendChunkedSrv,
            ProtoRecvChunkedServer = protoRecvChunkedSrv,
        };
    }

    // ClickHouse authentication / authorization error codes (src/Common/ErrorCodes.cpp).
    // These are permanent failures: same credentials will never succeed.
    private static bool IsAuthenticationErrorCode(int code) => code switch
    {
        192 => true, // UNKNOWN_USER
        193 => true, // WRONG_PASSWORD
        194 => true, // REQUIRED_PASSWORD
        195 => true, // IP_ADDRESS_NOT_ALLOWED
        196 => true, // UNKNOWN_ADDRESS_PATTERN_TYPE
        516 => true, // AUTHENTICATION_FAILED
        _ => false,
    };
}
