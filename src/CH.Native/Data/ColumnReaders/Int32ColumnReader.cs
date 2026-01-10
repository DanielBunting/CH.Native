using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for Int32 (int) values.
/// </summary>
public sealed class Int32ColumnReader : IColumnReader<int>
{
    /// <inheritdoc />
    public string TypeName => "Int32";

    /// <inheritdoc />
    public Type ClrType => typeof(int);

    /// <inheritdoc />
    public int ReadValue(ref ProtocolReader reader)
    {
        return reader.ReadInt32();
    }

    /// <inheritdoc />
    public TypedColumn<int> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        var pool = ArrayPool<int>.Shared;
        var values = pool.Rent(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            values[i] = reader.ReadInt32();
        }
        return new TypedColumn<int>(values, rowCount, pool);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
