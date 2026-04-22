using System.Buffers;
using CH.Native.Data.Variant;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for <c>Variant(T1, T2, …, Tn)</c> tagged-union values.
/// </summary>
/// <remarks>
/// <para>Wire format per block:</para>
/// <list type="number">
/// <item><description><see langword="UInt64"/> discriminator version (currently <c>0</c>).</description></item>
/// <item><description><paramref name="rowCount"/> discriminator bytes — one per row, each either an arm index in <c>[0, N-1]</c> or <see cref="ClickHouseVariant.NullDiscriminator"/> (255) for NULL.</description></item>
/// <item><description>Per arm <c>i</c>, a packed column of only the non-NULL rows whose discriminator equals <c>i</c>, in row order.</description></item>
/// </list>
/// <para>The reader returns a <see cref="VariantTypedColumn"/> holding the raw
/// discriminators and per-arm <see cref="ITypedColumn"/> instances. <see cref="ClickHouseVariant"/>
/// values are materialised on demand via <see cref="VariantTypedColumn.GetValue(int)"/>.
/// Callers on the hot path should prefer <see cref="VariantTypedColumn.GetTyped{T0,T1}"/>
/// or <see cref="VariantTypedColumn.GetArm{T}"/> to avoid boxing.</para>
/// </remarks>
public sealed class VariantColumnReader : IColumnReader
{
    private const ulong DiscriminatorVersion0 = 0;

    private readonly IColumnReader[] _innerReaders;
    private readonly string _typeName;

    public VariantColumnReader(IColumnReader[] innerReaders)
    {
        if (innerReaders is null || innerReaders.Length == 0)
            throw new ArgumentException("Variant requires at least one inner reader.", nameof(innerReaders));
        if (innerReaders.Length > 254)
            throw new ArgumentException("Variant supports at most 254 arms (discriminator 255 is reserved for NULL).", nameof(innerReaders));

        _innerReaders = innerReaders;
        _typeName = $"Variant({string.Join(", ", innerReaders.Select(r => r.TypeName))})";
    }

    /// <inheritdoc />
    public string TypeName => _typeName;

    /// <inheritdoc />
    public Type ClrType => typeof(ClickHouseVariant);

    /// <inheritdoc />
    public ITypedColumn ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        var version = reader.ReadUInt64();
        if (version != DiscriminatorVersion0)
            throw new NotSupportedException(
                $"Variant discriminator serialization version {version} is not supported; expected {DiscriminatorVersion0}.");

        var armCount = _innerReaders.Length;

        if (rowCount == 0)
        {
            var emptyArms = new ITypedColumn[armCount];
            for (int i = 0; i < armCount; i++)
                emptyArms[i] = _innerReaders[i].ReadTypedColumn(ref reader, 0);
            return new VariantTypedColumn(Array.Empty<byte>(), 0, emptyArms, Array.Empty<int>());
        }

        // Copy discriminators into a pooled buffer that VariantTypedColumn owns for its lifetime.
        var discPool = ArrayPool<byte>.Shared;
        var discBuffer = discPool.Rent(rowCount);

        try
        {
            using (var pooled = reader.ReadPooledBytes(rowCount))
            {
                pooled.Span.CopyTo(discBuffer.AsSpan(0, rowCount));
            }

            var discriminators = discBuffer.AsSpan(0, rowCount);
            var counts = VariantBucketing.CountPerArm(discriminators, armCount);

            var armColumns = new ITypedColumn[armCount];
            var armColumnsBuilt = 0;
            try
            {
                for (int arm = 0; arm < armCount; arm++)
                {
                    armColumns[arm] = _innerReaders[arm].ReadTypedColumn(ref reader, counts[arm]);
                    armColumnsBuilt++;
                }

                var offsetPool = ArrayPool<int>.Shared;
                var rowToArmOffset = offsetPool.Rent(rowCount);
                try
                {
                    Span<int> cursors = armCount <= 32 ? stackalloc int[armCount] : new int[armCount];
                    cursors.Clear();

                    for (int i = 0; i < rowCount; i++)
                    {
                        var disc = discriminators[i];
                        if (disc == ClickHouseVariant.NullDiscriminator)
                        {
                            rowToArmOffset[i] = -1;
                            continue;
                        }
                        if (disc >= armCount)
                            throw new InvalidOperationException(
                                $"Discriminator {disc} out of range for {armCount}-arm Variant at row {i}.");
                        rowToArmOffset[i] = cursors[disc]++;
                    }

                    return new VariantTypedColumn(
                        discBuffer,
                        rowCount,
                        armColumns,
                        rowToArmOffset,
                        discPool,
                        offsetPool);
                }
                catch
                {
                    offsetPool.Return(rowToArmOffset);
                    throw;
                }
            }
            catch
            {
                for (int i = 0; i < armColumnsBuilt; i++)
                    armColumns[i]?.Dispose();
                throw;
            }
        }
        catch
        {
            discPool.Return(discBuffer);
            throw;
        }
    }
}
