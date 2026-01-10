namespace CH.Native.Protocol;

/// <summary>
/// Client information sent with each query to identify the client to the server.
/// </summary>
public readonly struct ClientInfo
{
    /// <summary>
    /// Query kind: 0 = no query, 1 = initial query, 2 = secondary query.
    /// </summary>
    public byte QueryKind { get; init; }

    /// <summary>
    /// The initial user who started the query.
    /// </summary>
    public string InitialUser { get; init; }

    /// <summary>
    /// The initial query ID (for distributed queries).
    /// </summary>
    public string InitialQueryId { get; init; }

    /// <summary>
    /// The initial client address in "[::ffff:127.0.0.1]:port" format.
    /// </summary>
    public string InitialAddress { get; init; }

    /// <summary>
    /// The operating system username.
    /// </summary>
    public string OsUser { get; init; }

    /// <summary>
    /// The client machine hostname.
    /// </summary>
    public string ClientHostname { get; init; }

    /// <summary>
    /// The client application name.
    /// </summary>
    public string ClientName { get; init; }

    /// <summary>
    /// Client major version number.
    /// </summary>
    public int VersionMajor { get; init; }

    /// <summary>
    /// Client minor version number.
    /// </summary>
    public int VersionMinor { get; init; }

    /// <summary>
    /// Protocol revision number.
    /// </summary>
    public int ProtocolRevision { get; init; }

    /// <summary>
    /// Quota key for resource tracking (empty = default).
    /// </summary>
    public string QuotaKey { get; init; }

    /// <summary>
    /// Distributed query depth (0 for non-distributed).
    /// </summary>
    public int DistributedDepth { get; init; }

    /// <summary>
    /// OpenTelemetry trace context (empty if not used).
    /// </summary>
    public string ClientTraceContext { get; init; }

    /// <summary>
    /// Creates a ClientInfo for an initial query.
    /// </summary>
    /// <param name="clientName">The client application name.</param>
    /// <param name="username">The username for the query.</param>
    /// <param name="queryId">The query ID.</param>
    /// <param name="protocolRevision">The negotiated protocol revision.</param>
    /// <returns>A new ClientInfo instance.</returns>
    public static ClientInfo Create(string clientName, string username, string queryId, int protocolRevision)
    {
        return new ClientInfo
        {
            QueryKind = 1, // Initial query
            InitialUser = username,
            InitialQueryId = queryId,
            InitialAddress = "[::ffff:127.0.0.1]:0",
            OsUser = GetOsUser(),
            ClientHostname = GetClientHostname(),
            ClientName = clientName,
            VersionMajor = 1,
            VersionMinor = 0,
            ProtocolRevision = protocolRevision,
            QuotaKey = string.Empty,
            DistributedDepth = 0,
            ClientTraceContext = string.Empty
        };
    }

    /// <summary>
    /// Writes the client info to the protocol writer.
    /// </summary>
    /// <param name="writer">The protocol writer.</param>
    /// <param name="protocolRevision">The negotiated protocol revision.</param>
    public void Write(ref ProtocolWriter writer, int protocolRevision)
    {
        // 1. Query kind
        writer.WriteByte(QueryKind);

        // 2-4. Initial query info
        writer.WriteString(InitialUser);
        writer.WriteString(InitialQueryId);
        writer.WriteString(InitialAddress);

        // 5. Initial query start time (microseconds since epoch)
        if (protocolRevision >= ProtocolVersion.WithInitialQueryStartTime)
        {
            writer.WriteInt64(0); // Not tracked
        }

        // 6. Interface type: TCP = 1
        writer.WriteByte(1);

        // 7-12. TCP client info
        writer.WriteString(OsUser);
        writer.WriteString(ClientHostname);
        writer.WriteString(ClientName);
        writer.WriteVarInt((ulong)VersionMajor);
        writer.WriteVarInt((ulong)VersionMinor);
        writer.WriteVarInt((ulong)ProtocolRevision);

        // 13. Version patch (must come right after protocol version for TCP)
        if (protocolRevision >= ProtocolVersion.WithVersionPatch)
        {
            writer.WriteVarInt(0); // Version patch
        }

        // 14. Quota key
        if (protocolRevision >= ProtocolVersion.WithQuotaKey)
        {
            writer.WriteString(QuotaKey);
        }

        // 15. Distributed depth
        if (protocolRevision >= ProtocolVersion.WithDistributedDepth)
        {
            writer.WriteVarInt((ulong)DistributedDepth);
        }

        // 16. OpenTelemetry trace context
        if (protocolRevision >= ProtocolVersion.WithOpenTelemetry)
        {
            // Trace flag: 0 = not traced
            writer.WriteByte(0);
        }

        // 17. Parallel replicas fields
        if (protocolRevision >= ProtocolVersion.WithParallelReplicas)
        {
            writer.WriteByte(0); // collaborate_with_initiator = false (UInt8)
            writer.WriteVarInt(0); // count_participating_replicas = 0
            writer.WriteVarInt(0); // number_of_current_replica = 0
        }
    }

    private static string GetOsUser()
    {
        try
        {
            return Environment.UserName;
        }
        catch
        {
            return string.Empty;
        }
    }

    private static string GetClientHostname()
    {
        try
        {
            return Environment.MachineName;
        }
        catch
        {
            return string.Empty;
        }
    }
}
