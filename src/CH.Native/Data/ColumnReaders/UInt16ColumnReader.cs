using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for UInt16 (ushort) values.
/// </summary>
public sealed class UInt16ColumnReader : IColumnReader<ushort>
{
    /// <inheritdoc />
    public string TypeName => "UInt16";

    /// <inheritdoc />
    public Type ClrType => typeof(ushort);

    /// <inheritdoc />
    public ushort ReadValue(ref ProtocolReader reader)
    {
        return reader.ReadUInt16();
    }

    /// <inheritdoc />
    public TypedColumn<ushort> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        var pool = ArrayPool<ushort>.Shared;
        var values = pool.Rent(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            values[i] = reader.ReadUInt16();
        }
        return new TypedColumn<ushort>(values, rowCount, pool);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
