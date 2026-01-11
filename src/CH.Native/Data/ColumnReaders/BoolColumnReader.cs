using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for Bool values.
/// Bool in ClickHouse is stored as UInt8 (0 = false, non-zero = true).
/// </summary>
public sealed class BoolColumnReader : IColumnReader<bool>
{
    /// <inheritdoc />
    public string TypeName => "Bool";

    /// <inheritdoc />
    public Type ClrType => typeof(bool);

    /// <inheritdoc />
    public bool ReadValue(ref ProtocolReader reader)
    {
        return reader.ReadByte() != 0;
    }

    /// <inheritdoc />
    public TypedColumn<bool> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        var pool = ArrayPool<bool>.Shared;
        var values = pool.Rent(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            values[i] = reader.ReadByte() != 0;
        }
        return new TypedColumn<bool>(values, rowCount, pool);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
