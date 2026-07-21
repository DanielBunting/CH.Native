using System.Runtime.CompilerServices;
using CH.Native.Exceptions;

namespace CH.Native.Protocol;

/// <summary>
/// Helpers for converting wire-supplied unsigned integers to signed sizes/indices,
/// surfacing oversized values as typed <see cref="ClickHouseProtocolException"/>s
/// instead of raw <see cref="OverflowException"/>s. Connection-layer catch sites
/// recognise the typed exception and tear the connection down so a corrupt
/// protocol stream is never returned to the pool.
/// </summary>
internal static class ProtocolGuards
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ToInt32(ulong value, string fieldName)
    {
        if (value > int.MaxValue)
            throw new ClickHouseProtocolException(
                $"Wire value {fieldName} = {value} exceeds Int32.MaxValue; protocol stream is malformed or hostile.");
        return (int)value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ToInt32(uint value, string fieldName)
    {
        if (value > int.MaxValue)
            throw new ClickHouseProtocolException(
                $"Wire value {fieldName} = {value} exceeds Int32.MaxValue; protocol stream is malformed or hostile.");
        return (int)value;
    }

    /// <summary>
    /// Validates that <paramref name="rowCount"/> is non-negative and that
    /// <c>rowCount * elementSize</c> fits in <see cref="int"/>. Returns the byte
    /// count for callers that bulk-read primitive columns. Adversarial / corrupt
    /// row counts otherwise wrap the multiplication and silently mis-size the
    /// downstream span / pool rental.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ValidateBulkReadByteCount(int rowCount, int elementSize, string fieldName)
    {
        if (rowCount < 0)
            throw new ClickHouseProtocolException(
                $"Negative rowCount {rowCount} for {fieldName}; protocol stream is malformed.");
        if ((uint)rowCount > (uint)(int.MaxValue / elementSize))
            throw new ClickHouseProtocolException(
                $"Wire rowCount {rowCount} for {fieldName} would overflow Int32 byte count " +
                $"({rowCount} × {elementSize}); protocol stream is malformed or hostile.");
        return rowCount * elementSize;
    }

    /// <summary>
    /// Validates that a wire-declared item count is plausible for the bytes
    /// actually available: each item costs at least
    /// <paramref name="minBytesPerItem"/> on the wire, so a count whose minimum
    /// footprint exceeds <paramref name="remaining"/> is malformed or hostile.
    /// The gate exists because a varint costs the sender ~5 bytes regardless of
    /// magnitude — without it, a corrupt or adversarial stream can declare ~2^31
    /// items and drive count-sized allocations (multi-GB arrays / pool rentals)
    /// before any per-item read underruns. The compressed read path has no
    /// pre-scan, so this is its only defense. Conservative by construction:
    /// <paramref name="remaining"/> may include bytes beyond the current message
    /// (uncompressed path), which can only make the gate more permissive — it
    /// never rejects a well-formed block.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ValidateCountAgainstRemaining(int count, int minBytesPerItem, long remaining, string fieldName)
    {
        if (count > 0 && (long)count * minBytesPerItem > remaining)
            throw new ClickHouseProtocolException(
                $"Wire value {fieldName} = {count} requires at least {(long)count * minBytesPerItem} bytes " +
                $"but only {remaining} are available; protocol stream is malformed or hostile.");
    }
}
