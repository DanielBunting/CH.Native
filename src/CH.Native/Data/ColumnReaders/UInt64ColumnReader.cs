using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for UInt64 (ulong) values.
/// </summary>
public sealed class UInt64ColumnReader : IColumnReader<ulong>
{
    /// <inheritdoc />
    public string TypeName => "UInt64";

    /// <inheritdoc />
    public Type ClrType => typeof(ulong);

    /// <inheritdoc />
    public ulong ReadValue(ref ProtocolReader reader)
    {
        return reader.ReadUInt64();
    }

    /// <inheritdoc />
    public TypedColumn<ulong> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        var pool = ArrayPool<ulong>.Shared;
        var values = pool.Rent(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            values[i] = reader.ReadUInt64();
        }
        return new TypedColumn<ulong>(values, rowCount, pool);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
