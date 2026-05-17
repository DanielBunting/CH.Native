namespace CH.Native.Data.AggregateState;

/// <summary>
/// Opaque per-row state of a ClickHouse <c>AggregateFunction</c> column.
/// </summary>
/// <remarks>
/// The bytes are produced and consumed by ClickHouse's aggregate-function serializers;
/// no client-side interpretation is available. Use server-side <c>finalizeAggregation()</c>
/// or the corresponding <c>*Merge()</c> function to obtain a final scalar value.
/// <para>
/// Equality is byte-wise on <see cref="State"/> and string-wise on <see cref="FunctionName"/>,
/// so two instances with identical content compare equal regardless of array reference
/// identity. Matches the equality convention of <see cref="ClickHouseMap{TKey, TValue}"/>.
/// </para>
/// </remarks>
public sealed class ClickHouseAggregateState : IEquatable<ClickHouseAggregateState>
{
    /// <summary>
    /// Creates a new aggregate-state value.
    /// </summary>
    /// <param name="state">The opaque state bytes (non-null; may be empty).</param>
    /// <param name="functionName">The aggregate function name in its bare form
    /// (e.g. <c>"sum"</c>, not <c>"sumState"</c>).</param>
    public ClickHouseAggregateState(byte[] state, string functionName)
    {
        State = state ?? throw new ArgumentNullException(nameof(state));
        FunctionName = functionName ?? throw new ArgumentNullException(nameof(functionName));
    }

    /// <summary>The opaque state bytes as produced by ClickHouse.</summary>
    public byte[] State { get; }

    /// <summary>The aggregate function name (bare form — <c>sum</c> not <c>sumState</c>).</summary>
    public string FunctionName { get; }

    /// <summary>An empty/sentinel state value: zero bytes, empty function name.</summary>
    public static readonly ClickHouseAggregateState Empty = new(Array.Empty<byte>(), string.Empty);

    /// <inheritdoc />
    public bool Equals(ClickHouseAggregateState? other) =>
        other is not null
        && FunctionName == other.FunctionName
        && State.AsSpan().SequenceEqual(other.State);

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as ClickHouseAggregateState);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(FunctionName);
        hash.AddBytes(State);
        return hash.ToHashCode();
    }
}
