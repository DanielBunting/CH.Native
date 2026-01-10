using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for Int128 values.
/// </summary>
public sealed class Int128ColumnReader : IColumnReader<Int128>
{
    /// <inheritdoc />
    public string TypeName => "Int128";

    /// <inheritdoc />
    public Type ClrType => typeof(Int128);

    /// <inheritdoc />
    public Int128 ReadValue(ref ProtocolReader reader)
    {
        return reader.ReadInt128();
    }

    /// <inheritdoc />
    public TypedColumn<Int128> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        var pool = ArrayPool<Int128>.Shared;
        var values = pool.Rent(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            values[i] = reader.ReadInt128();
        }
        return new TypedColumn<Int128>(values, rowCount, pool);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
