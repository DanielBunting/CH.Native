namespace CH.Native.Exceptions;

/// <summary>
/// Thrown by <c>ProtocolGuards.ValidateCountAgainstRemaining</c> instead of the
/// terminal <see cref="ClickHouseProtocolException"/> when the caller opted in
/// via <c>retryable: true</c>: the wire-declared item count's minimum footprint
/// exceeds the bytes currently available, but the caller is a compressed
/// multi-chunk accumulate loop that parses after EACH decompressed chunk, where
/// "count exceeds available" is the normal state of a legitimate large block
/// whose later chunks haven't arrived yet. Those loops catch this type and read
/// the next chunk instead of condemning the connection. Deliberately NOT part
/// of the public <see cref="ClickHouseException"/> hierarchy: it is an internal
/// control-flow signal that never escapes the accumulate loop, and keeping it
/// out lets <see cref="ClickHouseProtocolException"/> stay sealed. The
/// count-sized allocation the guard defends against stays deferred until
/// actual bytes back the declared count, so the defense is preserved.
/// </summary>
internal sealed class ClickHouseCountGuardException : Exception
{
    public ClickHouseCountGuardException(string message, long deficitBytes) : base(message)
    {
        DeficitBytes = deficitBytes;
    }

    /// <summary>
    /// How many more bytes the failing guard needs before its check can pass:
    /// <c>count × minBytesPerItem − remaining</c> at throw time. Accumulate
    /// loops use it to skip parse re-attempts until at least this many new
    /// bytes have been decompressed.
    /// </summary>
    public long DeficitBytes { get; }
}
