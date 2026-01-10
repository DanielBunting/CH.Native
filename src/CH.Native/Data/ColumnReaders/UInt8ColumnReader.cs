using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for UInt8 (byte) values.
/// </summary>
public sealed class UInt8ColumnReader : IColumnReader<byte>
{
    /// <inheritdoc />
    public string TypeName => "UInt8";

    /// <inheritdoc />
    public Type ClrType => typeof(byte);

    /// <inheritdoc />
    public byte ReadValue(ref ProtocolReader reader)
    {
        return reader.ReadByte();
    }

    /// <inheritdoc />
    public TypedColumn<byte> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        var pool = ArrayPool<byte>.Shared;
        var values = pool.Rent(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            values[i] = reader.ReadByte();
        }
        return new TypedColumn<byte>(values, rowCount, pool);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
