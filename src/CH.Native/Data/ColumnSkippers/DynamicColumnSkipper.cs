using System.Buffers;
using CH.Native.Data.Dynamic;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnSkippers;

/// <summary>
/// Column skipper for <c>Dynamic</c> values.
/// </summary>
public sealed class DynamicColumnSkipper : IColumnSkipper
{
    private readonly ColumnSkipperFactory _factory;
    private readonly string _typeName;

    public DynamicColumnSkipper(ColumnSkipperFactory factory, string typeName)
    {
        _factory = factory;
        _typeName = typeName;
    }

    /// <inheritdoc />
    public string TypeName => _typeName;

    /// <inheritdoc />
    public bool TrySkipColumn(ref ProtocolReader reader, int rowCount)
    {
        if (!reader.TryReadUInt64(out _)) return false; // structure version
        if (!reader.TryReadUInt64(out _)) return false; // max_types
        if (!reader.TryReadUInt64(out var numberOfTypesU)) return false;
        var numberOfTypes = (int)numberOfTypesU;

        var typeNames = new string[numberOfTypes];
        for (int i = 0; i < numberOfTypes; i++)
        {
            // We have to materialise the string to resolve the skipper — no TryReadString.
            try { typeNames[i] = reader.ReadString(); }
            catch { return false; }
        }

        if (!reader.TryReadUInt64(out _)) return false; // variant discriminator version

        var armCount = numberOfTypes + 1;
        var sharedArm = numberOfTypes;

        if (rowCount == 0)
            return true;

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
                if (disc == ClickHouseDynamic.NullDiscriminator) continue;
                if (disc >= armCount) return false;
                counts[disc]++;
            }

            for (int arm = 0; arm < numberOfTypes; arm++)
            {
                IColumnSkipper inner;
                try { inner = _factory.CreateSkipper(typeNames[arm]); }
                catch { return false; }
                if (!inner.TrySkipColumn(ref reader, counts[arm])) return false;
            }

            for (int r = 0; r < counts[sharedArm]; r++)
            {
                string rowTypeName;
                try { rowTypeName = reader.ReadString(); }
                catch { return false; }

                IColumnSkipper inner;
                try { inner = _factory.CreateSkipper(rowTypeName); }
                catch { return false; }
                if (!inner.TrySkipColumn(ref reader, 1)) return false;
            }

            return true;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(discriminators);
        }
    }
}
