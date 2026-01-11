namespace CH.Native.Protocol.Messages;

/// <summary>
/// Query message sent to execute SQL on the server.
/// </summary>
public readonly struct QueryMessage
{
    /// <summary>
    /// Query stage: 0 = FetchColumns, 1 = WithMergeableState, 2 = Complete.
    /// </summary>
    private const int QueryStageComplete = 2;

    /// <summary>
    /// Gets the unique query identifier.
    /// </summary>
    public string QueryId { get; init; }

    /// <summary>
    /// Gets the SQL query text to execute.
    /// </summary>
    public string QueryText { get; init; }

    /// <summary>
    /// Gets whether compression is enabled for data transfer.
    /// </summary>
    public bool UseCompression { get; init; }

    /// <summary>
    /// Gets the client info for this query.
    /// </summary>
    public ClientInfo ClientInfo { get; init; }

    /// <summary>
    /// Gets the query parameters (sent in the parameters section).
    /// </summary>
    public IReadOnlyDictionary<string, string>? Parameters { get; init; }

    /// <summary>
    /// Creates a new QueryMessage with an auto-generated query ID.
    /// </summary>
    /// <param name="sql">The SQL query to execute.</param>
    /// <param name="clientName">The client application name.</param>
    /// <param name="username">The username executing the query.</param>
    /// <param name="protocolRevision">The negotiated protocol revision.</param>
    /// <param name="useCompression">Whether to use compression.</param>
    /// <param name="parameters">Optional query parameters.</param>
    /// <returns>A new QueryMessage instance.</returns>
    public static QueryMessage Create(
        string sql,
        string clientName,
        string username,
        int protocolRevision,
        bool useCompression = false,
        IReadOnlyDictionary<string, string>? parameters = null)
    {
        var queryId = Guid.NewGuid().ToString("D");
        return new QueryMessage
        {
            QueryId = queryId,
            QueryText = sql,
            UseCompression = useCompression,
            Parameters = parameters,
            ClientInfo = ClientInfo.Create(clientName, username, queryId, protocolRevision)
        };
    }

    /// <summary>
    /// Writes the query message to the protocol writer.
    /// </summary>
    /// <param name="writer">The protocol writer.</param>
    /// <param name="protocolRevision">The negotiated protocol revision.</param>
    public void Write(ref ProtocolWriter writer, int protocolRevision)
    {
        // Message type
        writer.WriteVarInt((ulong)ClientMessageType.Query);

        // Query ID
        writer.WriteString(QueryId);

        // Client info
        ClientInfo.Write(ref writer, protocolRevision);

        // Settings section - empty for regular queries
        // (Parameters go in the dedicated parameters section below)
        writer.WriteString(string.Empty);

        // Inter-server secret (empty for regular clients, if protocol supports)
        if (protocolRevision >= ProtocolVersion.WithInterServerSecret)
        {
            writer.WriteString(string.Empty);
        }

        // Query stage
        writer.WriteVarInt(QueryStageComplete);

        // Compression: 0 = disabled, 1 = enabled
        writer.WriteVarInt(UseCompression ? 1ul : 0ul);

        // Query text
        writer.WriteString(QueryText);

        // Query parameters section - uses same format as settings (STRINGS_WITH_FLAGS)
        // Format: name (string) + flags (varint) + value (string), terminated by empty string
        // As per ClickHouse protocol and clickhouse-go implementation
        if (protocolRevision >= ProtocolVersion.WithParameters)
        {
            if (Parameters != null && Parameters.Count > 0)
            {
                foreach (var (name, value) in Parameters)
                {
                    writer.WriteString(name);
                    // Flags: 2 = settingFlagCustom (required for parameters)
                    writer.WriteVarInt(2);
                    writer.WriteString(value);
                }
            }
            // Empty string terminates parameters list
            writer.WriteString(string.Empty);
        }
    }
}
