using CH.Native.Protocol;

namespace CH.Native.Data.ColumnSkippers;

/// <summary>
/// Column skipper for FLATTENED <c>Dynamic</c> values (structure version 3, ClickHouse 25.6+).
/// </summary>
public sealed class DynamicColumnSkipper : IColumnSkipper
{
    private const ulong StructureVersionFlattened = 3;

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
        if (!reader.TryReadUInt64(out var structureVersion)) return false;
        if (structureVersion != StructureVersionFlattened) return false;

        if (!reader.TryReadVarInt(out var numberOfTypesU)) return false;
        var numberOfTypes = ProtocolGuards.ToInt32(numberOfTypesU, "Dynamic numberOfTypes");

        var typeNames = new string[numberOfTypes];
        for (int i = 0; i < numberOfTypes; i++)
        {
            try { typeNames[i] = reader.ReadString(); }
            catch { return false; }
        }

        var totalIndexValues = numberOfTypes + 1;

        if (rowCount == 0)
        {
            for (int arm = 0; arm < numberOfTypes; arm++)
            {
                IColumnSkipper inner;
                try { inner = _factory.CreateSkipper(typeNames[arm]); }
                catch { return false; }
                if (!inner.TrySkipColumn(ref reader, 0)) return false;
            }
            return true;
        }

        Span<int> counts = totalIndexValues <= 32 ? stackalloc int[totalIndexValues] : new int[totalIndexValues];

        try
        {
            if (totalIndexValues <= byte.MaxValue + 1)
            {
                for (int i = 0; i < rowCount; i++)
                {
                    if (!reader.TryReadByte(out var idx)) return false;
                    if (idx >= totalIndexValues) return false;
                    counts[idx]++;
                }
            }
            else if (totalIndexValues <= ushort.MaxValue + 1)
            {
                for (int i = 0; i < rowCount; i++)
                {
                    var idx = reader.ReadUInt16();
                    if (idx >= totalIndexValues) return false;
                    counts[idx]++;
                }
            }
            else
            {
                for (int i = 0; i < rowCount; i++)
                {
                    var idx = reader.ReadUInt32AsInt32("Dynamic index");
                    if (idx >= totalIndexValues) return false;
                    counts[idx]++;
                }
            }
        }
        catch
        {
            return false;
        }

        for (int arm = 0; arm < numberOfTypes; arm++)
        {
            IColumnSkipper inner;
            try { inner = _factory.CreateSkipper(typeNames[arm]); }
            catch { return false; }
            if (!inner.TrySkipColumn(ref reader, counts[arm])) return false;
        }

        return true;
    }
}
