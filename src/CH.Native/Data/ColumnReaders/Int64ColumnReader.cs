using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for Int64 (long) values.
/// </summary>
public sealed class Int64ColumnReader : IColumnReader<long>
{
    /// <inheritdoc />
    public string TypeName => "Int64";

    /// <inheritdoc />
    public Type ClrType => typeof(long);

    /// <inheritdoc />
    public long ReadValue(ref ProtocolReader reader)
    {
        return reader.ReadInt64();
    }

    /// <inheritdoc />
    public TypedColumn<long> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        var pool = ArrayPool<long>.Shared;
        var values = pool.Rent(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            values[i] = reader.ReadInt64();
        }
        return new TypedColumn<long>(values, rowCount, pool);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
