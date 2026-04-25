namespace CH.Native.Exceptions;

/// <summary>
/// Thrown when the wire bytes from the server cannot be parsed as a valid
/// ClickHouse native-protocol message — e.g. a length field overflows Int32,
/// a column reader encounters an out-of-range discriminator, or a tuple
/// element under-reads its declared row count.
///
/// A connection that surfaces this exception is left at an unknown offset in
/// the protocol stream and MUST NOT be returned to the pool. Catch sites in
/// <c>ClickHouseConnection</c> close the underlying transport so subsequent
/// rents observe <c>IsOpen == false</c>.
/// </summary>
public sealed class ClickHouseProtocolException : ClickHouseException
{
    /// <summary>
    /// Initializes a new instance with a message describing the malformed field.
    /// </summary>
    public ClickHouseProtocolException(string message) : base(message) { }

    /// <summary>
    /// Initializes a new instance with a message and the underlying parse error.
    /// </summary>
    public ClickHouseProtocolException(string message, Exception inner) : base(message, inner) { }
}
