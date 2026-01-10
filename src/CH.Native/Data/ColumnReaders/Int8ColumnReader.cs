using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for Int8 (sbyte) values.
/// </summary>
public sealed class Int8ColumnReader : IColumnReader<sbyte>
{
    /// <inheritdoc />
    public string TypeName => "Int8";

    /// <inheritdoc />
    public Type ClrType => typeof(sbyte);

    /// <inheritdoc />
    public sbyte ReadValue(ref ProtocolReader reader)
    {
        return (sbyte)reader.ReadByte();
    }

    /// <inheritdoc />
    public TypedColumn<sbyte> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        var pool = ArrayPool<sbyte>.Shared;
        var values = pool.Rent(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            values[i] = (sbyte)reader.ReadByte();
        }
        return new TypedColumn<sbyte>(values, rowCount, pool);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
