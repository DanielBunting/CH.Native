namespace CH.Native.Protocol.Messages;

/// <summary>
/// Client hello message sent to initiate the connection handshake.
/// </summary>
public readonly struct ClientHello
{
    /// <summary>
    /// Gets the client name sent to the server.
    /// </summary>
    public string ClientName { get; init; }

    /// <summary>
    /// Gets the client major version number.
    /// </summary>
    public int VersionMajor { get; init; }

    /// <summary>
    /// Gets the client minor version number.
    /// </summary>
    public int VersionMinor { get; init; }

    /// <summary>
    /// Gets the protocol revision to negotiate with the server.
    /// </summary>
    public int ProtocolRevision { get; init; }

    /// <summary>
    /// Gets the database to connect to.
    /// </summary>
    public string Database { get; init; }

    /// <summary>
    /// Gets the username for authentication.
    /// </summary>
    public string Username { get; init; }

    /// <summary>
    /// Gets the password for authentication.
    /// </summary>
    public string Password { get; init; }

    /// <summary>
    /// Creates a default ClientHello for the specified database and credentials.
    /// </summary>
    /// <param name="clientName">The client name to identify as.</param>
    /// <param name="database">The database to connect to.</param>
    /// <param name="username">The username for authentication.</param>
    /// <param name="password">The password for authentication.</param>
    /// <returns>A new ClientHello instance.</returns>
    public static ClientHello Create(string clientName, string database, string username, string password)
    {
        return new ClientHello
        {
            ClientName = clientName,
            VersionMajor = 1,
            VersionMinor = 0,
            ProtocolRevision = ProtocolVersion.Current,
            Database = database,
            Username = username,
            Password = password
        };
    }

    /// <summary>
    /// Writes the client hello message to the protocol writer.
    /// </summary>
    /// <param name="writer">The protocol writer to write to.</param>
    public void Write(ref ProtocolWriter writer)
    {
        writer.WriteVarInt((ulong)ClientMessageType.Hello);
        writer.WriteString(ClientName);
        writer.WriteVarInt((ulong)VersionMajor);
        writer.WriteVarInt((ulong)VersionMinor);
        writer.WriteVarInt((ulong)ProtocolRevision);
        writer.WriteString(Database);
        writer.WriteString(Username);
        writer.WriteString(Password);
    }
}
