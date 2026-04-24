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
}
