namespace CH.Native.Protocol;

/// <summary>
/// Message types sent from server to client.
/// </summary>
public enum ServerMessageType : byte
{
    /// <summary>
    /// Server hello response.
    /// </summary>
    Hello = 0,

    /// <summary>
    /// Data block with query results.
    /// </summary>
    Data = 1,

    /// <summary>
    /// Server exception/error.
    /// </summary>
    Exception = 2,

    /// <summary>
    /// Query progress information.
    /// </summary>
    Progress = 3,

    /// <summary>
    /// Pong response to ping.
    /// </summary>
    Pong = 4,

    /// <summary>
    /// End of data stream.
    /// </summary>
    EndOfStream = 5,

    /// <summary>
    /// Query profile information.
    /// </summary>
    ProfileInfo = 6,

    /// <summary>
    /// Totals row for aggregation.
    /// </summary>
    Totals = 7,

    /// <summary>
    /// Extremes (min/max) row.
    /// </summary>
    Extremes = 8,

    /// <summary>
    /// Table status response.
    /// </summary>
    TablesStatusResponse = 9,

    /// <summary>
    /// Server log message.
    /// </summary>
    Log = 10,

    /// <summary>
    /// Table columns information.
    /// </summary>
    TableColumns = 11,

    /// <summary>
    /// Part UUIDs for distributed queries.
    /// </summary>
    PartUUIDs = 12,

    /// <summary>
    /// Read task request for parallel processing.
    /// </summary>
    ReadTaskRequest = 13,

    /// <summary>
    /// Profile events for monitoring.
    /// </summary>
    ProfileEvents = 14,
}
