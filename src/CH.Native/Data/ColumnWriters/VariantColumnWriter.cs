using System.Buffers;
using CH.Native.Data.Variant;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for <c>Variant(T1, T2, …, Tn)</c> tagged-union values.
/// </summary>
/// <remarks>
/// Mirrors <see cref="ColumnReaders.VariantColumnReader"/>: writes a <see langword="UInt64"/>
/// version header followed by per-row discriminator bytes, then per-arm packed sub-columns.
/// <para>
/// Row-at-a-time writes are not supported because bucketing is block-level.
/// <see cref="WriteValue"/> throws.
/// </para>
/// </remarks>
internal sealed class VariantColumnWriter : IColumnWriter<ClickHouseVariant>
{
    private const ulong DiscriminatorVersion0 = 0;

    private readonly IColumnWriter[] _innerWriters;
    private readonly string _typeName;

    public VariantColumnWriter(IColumnWriter[] innerWriters)
    {
        if (innerWriters is null || innerWriters.Length == 0)
            throw new ArgumentException("Variant requires at least one inner writer.", nameof(innerWriters));
        if (innerWriters.Length > 254)
            throw new ArgumentException("Variant supports at most 254 arms.", nameof(innerWriters));

        _innerWriters = innerWriters;
        _typeName = $"Variant({string.Join(", ", innerWriters.Select(w => w.TypeName))})";
    }

    /// <inheritdoc />
    public string TypeName => _typeName;

    /// <inheritdoc />
    public Type ClrType => typeof(ClickHouseVariant);

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, ClickHouseVariant value)
    {
        throw new NotSupportedException(
            "VariantColumnWriter does not support row-at-a-time WriteValue because bucketing is block-level. " +
            "Build a 1-element array and call WriteColumn instead.");
    }

    /// <inheritdoc />
    // DiscriminatorVersion0 is the column-level state prefix for SerializationVariant —
    // must precede outer composite data (same shape as LowCardinality's KeysSerializationVersion).
    public void WritePrefix(ref ProtocolWriter writer) => writer.WriteUInt64(DiscriminatorVersion0);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, ClickHouseVariant[] values)
    {
        if (values.Length == 0)
            return;

        var armCount = _innerWriters.Length;

        // Single pass: emit discriminators to a pooled buffer while tallying per-arm counts.
        var discriminators = ArrayPool<byte>.Shared.Rent(values.Length);
        Span<int> counts = armCount <= 32 ? stackalloc int[armCount] : new int[armCount];

        try
        {
            for (int i = 0; i < values.Length; i++)
            {
                var disc = values[i].Discriminator;
                discriminators[i] = disc;
                if (disc == ClickHouseVariant.NullDiscriminator)
                    continue;
                if (disc >= armCount)
                    throw new ArgumentOutOfRangeException(
                        nameof(values),
                        $"Row {i} has discriminator {disc} but Variant declares only {armCount} arms.");
                counts[disc]++;
            }

            writer.WriteBytes(discriminators.AsSpan(0, values.Length));

            // Per-arm buckets must be exact-length because the inner writer interface iterates
            // `values.Length`. The outer array-of-arrays holder is pooled to avoid the jagged
            // header allocation.
            var buckets = ArrayPool<object?[]>.Shared.Rent(armCount);
            try
            {
                for (int arm = 0; arm < armCount; arm++)
                {
                    buckets[arm] = counts[arm] == 0
                        ? Array.Empty<object?>()
                        : new object?[counts[arm]];
                }

                Span<int> cursors = armCount <= 32 ? stackalloc int[armCount] : new int[armCount];
                for (int i = 0; i < values.Length; i++)
                {
                    var disc = values[i].Discriminator;
                    if (disc == ClickHouseVariant.NullDiscriminator)
                        continue;
                    buckets[disc][cursors[disc]++] = values[i].Value;
                }

                for (int arm = 0; arm < armCount; arm++)
                    _innerWriters[arm].WriteColumn(ref writer, buckets[arm]);
            }
            finally
            {
                ArrayPool<object?[]>.Shared.Return(buckets, clearArray: true);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(discriminators);
        }
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        var converted = new ClickHouseVariant[values.Length];
        for (int i = 0; i < values.Length; i++)
        {
            converted[i] = values[i] switch
            {
                null => ClickHouseVariant.Null,
                ClickHouseVariant v => v,
                _ => throw new ArgumentException(
                    $"Row {i}: expected ClickHouseVariant or null, got {values[i]!.GetType()}.", nameof(values)),
            };
        }
        WriteColumn(ref writer, converted);
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value) => WriteValue(ref writer, default);
}
