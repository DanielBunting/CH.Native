using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for Int16 (short) values.
/// </summary>
public sealed class Int16ColumnReader : IColumnReader<short>
{
    /// <inheritdoc />
    public string TypeName => "Int16";

    /// <inheritdoc />
    public Type ClrType => typeof(short);

    /// <inheritdoc />
    public short ReadValue(ref ProtocolReader reader)
    {
        return reader.ReadInt16();
    }

    /// <inheritdoc />
    public TypedColumn<short> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        var pool = ArrayPool<short>.Shared;
        var values = pool.Rent(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            values[i] = reader.ReadInt16();
        }
        return new TypedColumn<short>(values, rowCount, pool);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
