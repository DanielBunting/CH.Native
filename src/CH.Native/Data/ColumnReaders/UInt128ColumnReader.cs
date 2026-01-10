using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for UInt128 values.
/// </summary>
public sealed class UInt128ColumnReader : IColumnReader<UInt128>
{
    /// <inheritdoc />
    public string TypeName => "UInt128";

    /// <inheritdoc />
    public Type ClrType => typeof(UInt128);

    /// <inheritdoc />
    public UInt128 ReadValue(ref ProtocolReader reader)
    {
        return reader.ReadUInt128();
    }

    /// <inheritdoc />
    public TypedColumn<UInt128> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        var pool = ArrayPool<UInt128>.Shared;
        var values = pool.Rent(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            values[i] = reader.ReadUInt128();
        }
        return new TypedColumn<UInt128>(values, rowCount, pool);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
