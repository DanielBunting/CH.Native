using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for UInt32 (uint) values.
/// </summary>
public sealed class UInt32ColumnReader : IColumnReader<uint>
{
    /// <inheritdoc />
    public string TypeName => "UInt32";

    /// <inheritdoc />
    public Type ClrType => typeof(uint);

    /// <inheritdoc />
    public uint ReadValue(ref ProtocolReader reader)
    {
        return reader.ReadUInt32();
    }

    /// <inheritdoc />
    public TypedColumn<uint> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        var pool = ArrayPool<uint>.Shared;
        var values = pool.Rent(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            values[i] = reader.ReadUInt32();
        }
        return new TypedColumn<uint>(values, rowCount, pool);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
