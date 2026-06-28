using CH.Native.Data.Types;
using CH.Native.Protocol;

namespace CH.Native.Data.AggregateState;

/// <summary>
/// Per-row wire-format contract for a single <c>AggregateFunction</c> state value.
/// Each aggregate function family has its own format (fixed bytes, varuint, flag + fixed, …)
/// driven by the Phase 0 wire-format probe.
/// </summary>
internal interface IAggregateFunctionStateFormat
{
    /// <summary>Reads one state value from the protocol stream.</summary>
    byte[] ReadOneState(ref ProtocolReader reader);

    /// <summary>Writes one state value to the protocol stream, verbatim.</summary>
    void WriteOneState(ref ProtocolWriter writer, ReadOnlySpan<byte> state);

    /// <summary>Skips one state value (consumes the same bytes the reader would).</summary>
    bool TrySkipOneState(ref ProtocolReader reader);
}

/// <summary>
/// State is exactly <c>N</c> bytes per row. Used for <c>sum</c> over non-nullable
/// fixed-width numerics (Phase 0 findings § "FIXED").
/// </summary>
internal sealed class FixedSizeStateFormat : IAggregateFunctionStateFormat
{
    private readonly int _size;

    public FixedSizeStateFormat(int size) => _size = size;

    public byte[] ReadOneState(ref ProtocolReader reader)
    {
        // ProtocolReader.ReadBytes returns ReadOnlyMemory<byte> that may alias the
        // underlying pipe buffer; copy into our own array before the next read advances.
        return reader.ReadBytes(_size).ToArray();
    }

    public void WriteOneState(ref ProtocolWriter writer, ReadOnlySpan<byte> state)
    {
        if (state.Length != _size)
            throw new ArgumentException(
                $"Expected state of {_size} bytes, got {state.Length}.", nameof(state));
        writer.WriteBytes(state);
    }

    public bool TrySkipOneState(ref ProtocolReader reader) =>
        reader.TrySkipBytes(_size);
}

/// <summary>
/// State is a single VarUInt. Used for <c>count</c> (the state IS the running count
/// encoded as a varuint) — Phase 0 findings § "VARUINT".
/// </summary>
internal sealed class VarUIntStateFormat : IAggregateFunctionStateFormat
{
    public static readonly VarUIntStateFormat Instance = new();

    public byte[] ReadOneState(ref ProtocolReader reader)
    {
        // VarUInts are 1-9 bytes. Capture them by scanning ahead 1 byte at a time
        // until the continuation bit is clear.
        var bytes = new List<byte>(2);
        while (true)
        {
            var b = reader.ReadByte();
            bytes.Add(b);
            if ((b & 0x80) == 0)
                break;
            if (bytes.Count > 10)
                throw new FormatException("VarUInt state runaway: more than 10 bytes without terminator.");
        }
        return bytes.ToArray();
    }

    public void WriteOneState(ref ProtocolWriter writer, ReadOnlySpan<byte> state)
    {
        if (state.Length == 0 || state.Length > 10)
            throw new ArgumentException(
                $"VarUInt state must be 1-10 bytes, got {state.Length}.", nameof(state));
        writer.WriteBytes(state);
    }

    public bool TrySkipOneState(ref ProtocolReader reader) =>
        reader.TrySkipVarInt();
}

/// <summary>
/// State is a 1-byte "has value" flag, followed by <c>N</c> data bytes when the
/// flag is <c>0x01</c>. When the flag is <c>0x00</c> the state is just that single
/// byte (an empty group — rare in MV-populated columns but legal). Used for
/// <c>min</c>/<c>max</c>/<c>any</c>/<c>anyLast</c> over non-nullable fixed-width
/// primitives — Phase 0 findings § "FLAG + FIXED".
/// </summary>
internal sealed class FlagPlusFixedStateFormat : IAggregateFunctionStateFormat
{
    private readonly int _innerSize;

    public FlagPlusFixedStateFormat(int innerSize) => _innerSize = innerSize;

    public byte[] ReadOneState(ref ProtocolReader reader)
    {
        var flag = reader.ReadByte();
        if (flag == 0x00)
            return new byte[] { 0x00 };
        if (flag != 0x01)
            throw new FormatException(
                $"Unexpected aggregate-state flag byte 0x{flag:X2} (expected 0x00 or 0x01).");

        var result = new byte[1 + _innerSize];
        result[0] = 0x01;
        reader.ReadBytes(_innerSize).Span.CopyTo(result.AsSpan(1));
        return result;
    }

    public void WriteOneState(ref ProtocolWriter writer, ReadOnlySpan<byte> state)
    {
        if (state.Length == 1)
        {
            if (state[0] != 0x00)
                throw new ArgumentException(
                    $"Single-byte state must start with 0x00, got 0x{state[0]:X2}.", nameof(state));
            writer.WriteBytes(state);
            return;
        }

        if (state.Length != 1 + _innerSize)
            throw new ArgumentException(
                $"FlagPlusFixed state must be 1 or {1 + _innerSize} bytes, got {state.Length}.",
                nameof(state));

        if (state[0] != 0x01)
            throw new ArgumentException(
                $"Multi-byte state must start with 0x01, got 0x{state[0]:X2}.", nameof(state));

        writer.WriteBytes(state);
    }

    public bool TrySkipOneState(ref ProtocolReader reader)
    {
        if (!reader.TryReadByte(out var flag))
            return false;
        if (flag == 0x00)
            return true;
        if (flag != 0x01)
            return false;
        return reader.TrySkipBytes(_innerSize);
    }
}

/// <summary>
/// Resolves the per-row wire format for an <c>AggregateFunction(name, T...)</c> column.
/// Driven by the Phase 0 wire-format findings (see <c>.tmp/aggregate-functions/01-wire-format-findings.md</c>).
/// </summary>
internal static class AggregateFunctionStateFormatRegistry
{
    /// <summary>
    /// Returns the wire-format decoder for the given aggregate function and inner types,
    /// or throws <see cref="NotSupportedException"/> with workaround guidance for
    /// unsupported functions/inner-type combinations.
    /// </summary>
    public static IAggregateFunctionStateFormat Resolve(
        string functionName,
        IReadOnlyList<ClickHouseType> typeArguments)
    {
        return functionName switch
        {
            "sum" => ResolveSum(typeArguments),
            "count" => VarUIntStateFormat.Instance,
            "min" or "max" or "any" or "anyLast" => ResolveSingleValue(functionName, typeArguments),
            _ => throw Unsupported(functionName, typeArguments)
        };
    }

    private static IAggregateFunctionStateFormat ResolveSum(IReadOnlyList<ClickHouseType> typeArguments)
    {
        if (typeArguments.Count != 1)
            throw new FormatException(
                $"AggregateFunction(sum, ...) requires exactly one inner type, got {typeArguments.Count}.");

        var inner = typeArguments[0];
        if (inner.IsNullable)
            throw Unsupported("sum", typeArguments,
                "Nullable inner types add a per-row null bitmap that varies in size between rows.");

        var size = inner.BaseName switch
        {
            "Int8" or "Int16" or "Int32" or "Int64"
                or "UInt8" or "UInt16" or "UInt32" or "UInt64"
                or "Float32" or "Float64" => 8,
            "Int128" or "UInt128" => 16,
            "Int256" or "UInt256" => 32,
            _ => -1
        };

        // Decimal, either spelling. The server canonicalises every Decimal to
        // Decimal(P, S) (BaseName "Decimal"), so a BaseName switch on the sized
        // aliases alone never matches real wire data. sum PROMOTES the accumulator:
        // the state is 16 bytes for native widths up to 16 (Decimal32/64/128 → P ≤ 38)
        // and 32 bytes for Decimal256 (P 39–76). Verified against the server via
        // hex(sumState) length.
        if (size < 0)
        {
            var decimalNative = DecimalNativeSize(inner);
            if (decimalNative >= 0)
                size = decimalNative <= 16 ? 16 : 32;
        }

        if (size < 0)
            throw Unsupported("sum", typeArguments,
                $"inner type {inner.OriginalTypeName} is not in the tier-1 set.");

        return new FixedSizeStateFormat(size);
    }

    private static IAggregateFunctionStateFormat ResolveSingleValue(
        string functionName, IReadOnlyList<ClickHouseType> typeArguments)
    {
        if (typeArguments.Count != 1)
            throw new FormatException(
                $"AggregateFunction({functionName}, ...) requires exactly one inner type, got {typeArguments.Count}.");

        var inner = typeArguments[0];
        if (inner.IsNullable)
            throw Unsupported(functionName, typeArguments,
                "Nullable inner types add a per-row null bitmap that varies in size between rows.");

        var innerSize = inner.BaseName switch
        {
            "Bool" or "Int8" or "UInt8" => 1,
            "Int16" or "UInt16" or "Date" => 2,
            "Int32" or "UInt32" or "Float32" or "DateTime" or "Date32" => 4,
            "Int64" or "UInt64" or "Float64" or "DateTime64" => 8,
            "Int128" or "UInt128" or "UUID" => 16,
            "Int256" or "UInt256" => 32,
            _ => -1
        };

        // Decimal, either spelling (see ResolveSum). Unlike sum, the single-value
        // state stores the NATIVE-width value behind a one-byte "has value" flag
        // (FlagPlusFixedStateFormat adds the +1), so the inner size is the native
        // Decimal width (4/8/16/32 by precision). Verified against the server via
        // hex(minState)/hex(maxState) length (5/9/17/33 = 1 + native).
        if (innerSize < 0)
            innerSize = DecimalNativeSize(inner);

        if (innerSize < 0)
            throw Unsupported(functionName, typeArguments,
                $"inner type {inner.OriginalTypeName} is not in the tier-1 set.");

        return new FlagPlusFixedStateFormat(innerSize);
    }

    /// <summary>
    /// Native on-wire width (bytes) of a Decimal inner type, from either spelling, or
    /// <c>-1</c> if <paramref name="t"/> is not a Decimal. The sized aliases name the
    /// width directly; the canonical <c>Decimal(P, S)</c> form (the only one the server
    /// actually emits in an aggregate descriptor) derives it from the precision <c>P</c>:
    /// P ≤ 9 → 4, ≤ 18 → 8, ≤ 38 → 16, else 32.
    /// </summary>
    private static int DecimalNativeSize(ClickHouseType t) => t.BaseName switch
    {
        "Decimal32" => 4,
        "Decimal64" => 8,
        "Decimal128" => 16,
        "Decimal256" => 32,
        "Decimal" when t.Parameters.Count >= 1 && int.TryParse(t.Parameters[0], out var p)
            => p <= 9 ? 4 : p <= 18 ? 8 : p <= 38 ? 16 : 32,
        _ => -1
    };

    private static NotSupportedException Unsupported(
        string functionName,
        IReadOnlyList<ClickHouseType> typeArguments,
        string? extraDetail = null)
    {
        var args = string.Join(", ", typeArguments.Select(t => t.OriginalTypeName));
        var signature = args.Length > 0 ? $"AggregateFunction({functionName}, {args})" : $"AggregateFunction({functionName})";
        var detail = extraDetail is null ? string.Empty : $" Reason: {extraDetail}";
        return new NotSupportedException(
            $"{signature} is not supported by CH.Native yet.{detail} " +
            "Workaround: project the column server-side, e.g. " +
            "`SELECT finalizeAggregation(col)` to materialize the final scalar value, " +
            "or `SELECT hex(col)` to transfer the opaque state as a String. " +
            "Open an issue at https://github.com/DanielBunting/CH.Native with the function name to request support.");
    }
}
