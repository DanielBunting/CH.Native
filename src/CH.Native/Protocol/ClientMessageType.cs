namespace CH.Native.Protocol;

/// <summary>
/// Message types sent from client to server.
/// </summary>
public enum ClientMessageType : byte
{
    /// <summary>
    /// Initial hello handshake message.
    /// </summary>
    Hello = 0,

    /// <summary>
    /// Query execution request.
    /// </summary>
    Query = 1,

    /// <summary>
    /// Data block for insert operations.
    /// </summary>
    Data = 2,

    /// <summary>
    /// Cancel current query.
    /// </summary>
    Cancel = 3,

    /// <summary>
    /// Ping message for keep-alive.
    /// </summary>
    Ping = 4,

    /// <summary>
    /// Request table status information.
    /// </summary>
    TablesStatusRequest = 5,

    /// <summary>
    /// Keep-alive message.
    /// </summary>
    KeepAlive = 6,
}
