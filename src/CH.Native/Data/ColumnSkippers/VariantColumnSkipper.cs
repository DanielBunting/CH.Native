using System.Buffers;
using CH.Native.Data.Variant;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnSkippers;

/// <summary>
/// Column skipper for <c>Variant(T1, T2, …, Tn)</c> values.
/// </summary>
public sealed class VariantColumnSkipper : IColumnSkipper
{
    private readonly IColumnSkipper[] _innerSkippers;
    private readonly string _typeName;

    public VariantColumnSkipper(IColumnSkipper[] innerSkippers, string[] innerTypeNames)
    {
        _innerSkippers = innerSkippers;
        _typeName = $"Variant({string.Join(", ", innerTypeNames)})";
    }

    /// <inheritdoc />
    public string TypeName => _typeName;

    /// <inheritdoc />
    public bool TrySkipColumn(ref ProtocolReader reader, int rowCount)
    {
        if (!reader.TryReadUInt64(out _)) // discriminator version
            return false;

        if (rowCount == 0)
            return true;

        var armCount = _innerSkippers.Length;

        var discriminators = ArrayPool<byte>.Shared.Rent(rowCount);
        try
        {
            for (int i = 0; i < rowCount; i++)
            {
                if (!reader.TryReadByte(out discriminators[i]))
                    return false;
            }

            var counts = new int[armCount];
            for (int i = 0; i < rowCount; i++)
            {
                var disc = discriminators[i];
                if (disc == ClickHouseVariant.NullDiscriminator)
                    continue;
                if (disc >= armCount)
                    return false;
                counts[disc]++;
            }

            for (int arm = 0; arm < armCount; arm++)
            {
                if (!_innerSkippers[arm].TrySkipColumn(ref reader, counts[arm]))
                    return false;
            }

            return true;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(discriminators);
        }
    }
}
