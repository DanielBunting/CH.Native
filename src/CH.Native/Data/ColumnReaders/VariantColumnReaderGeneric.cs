using System.Buffers;
using CH.Native.Data.Variant;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Boxing-free column reader for the 2-arm <c>Variant(T0, T1)</c> shape.
/// </summary>
/// <remarks>
/// Produces <see cref="VariantValue{T0, T1}"/> rather than <see cref="ClickHouseVariant"/>,
/// so primitive arm values are stored unboxed. The wire format is identical to
/// <see cref="VariantColumnReader"/>.
/// </remarks>
public sealed class VariantColumnReader<T0, T1> : IColumnReader<VariantValue<T0, T1>>
{
    private const ulong DiscriminatorVersion0 = 0;

    private readonly IColumnReader<T0> _arm0;
    private readonly IColumnReader<T1> _arm1;
    private readonly string _typeName;

    public VariantColumnReader(IColumnReader<T0> arm0, IColumnReader<T1> arm1)
    {
        _arm0 = arm0 ?? throw new ArgumentNullException(nameof(arm0));
        _arm1 = arm1 ?? throw new ArgumentNullException(nameof(arm1));
        _typeName = $"Variant({arm0.TypeName}, {arm1.TypeName})";
    }

    /// <inheritdoc />
    public string TypeName => _typeName;

    /// <inheritdoc />
    public Type ClrType => typeof(VariantValue<T0, T1>);

    /// <inheritdoc />
    public VariantValue<T0, T1> ReadValue(ref ProtocolReader reader)
    {
        throw new NotSupportedException(
            "VariantColumnReader<T0, T1> does not support row-at-a-time ReadValue; call ReadTypedColumn(1).");
    }

    /// <inheritdoc />
    public TypedColumn<VariantValue<T0, T1>> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        var version = reader.ReadUInt64();
        if (version != DiscriminatorVersion0)
            throw new NotSupportedException(
                $"Variant discriminator serialization version {version} is not supported; expected {DiscriminatorVersion0}.");

        if (rowCount == 0)
            return new TypedColumn<VariantValue<T0, T1>>(Array.Empty<VariantValue<T0, T1>>());

        using var discriminatorsBytes = reader.ReadPooledBytes(rowCount);
        var discriminators = discriminatorsBytes.Span;

        // Counts per arm
        int count0 = 0, count1 = 0;
        for (int i = 0; i < rowCount; i++)
        {
            var d = discriminators[i];
            if (d == VariantValue<T0, T1>.NullDiscriminator) continue;
            if (d == 0) count0++;
            else if (d == 1) count1++;
            else throw new InvalidOperationException(
                $"Discriminator {d} out of range for 2-arm Variant at row {i}.");
        }

        using var arm0Col = _arm0.ReadTypedColumn(ref reader, count0);
        using var arm1Col = _arm1.ReadTypedColumn(ref reader, count1);

        var resultPool = ArrayPool<VariantValue<T0, T1>>.Shared;
        var result = resultPool.Rent(rowCount);

        try
        {
            int cursor0 = 0, cursor1 = 0;
            var arm0Span = arm0Col.Values;
            var arm1Span = arm1Col.Values;

            for (int i = 0; i < rowCount; i++)
            {
                var d = discriminators[i];
                if (d == VariantValue<T0, T1>.NullDiscriminator)
                {
                    result[i] = VariantValue<T0, T1>.Null;
                    continue;
                }
                if (d == 0)
                    result[i] = VariantValue<T0, T1>.FromArm0(arm0Span[cursor0++]);
                else
                    result[i] = VariantValue<T0, T1>.FromArm1(arm1Span[cursor1++]);
            }

            return new TypedColumn<VariantValue<T0, T1>>(result, rowCount, resultPool);
        }
        catch
        {
            resultPool.Return(result);
            throw;
        }
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
        => ReadTypedColumn(ref reader, rowCount);
}
