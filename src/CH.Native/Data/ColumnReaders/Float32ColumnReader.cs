using System.Buffers;
using System.Buffers.Binary;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for Float32 (float) values.
/// </summary>
public sealed class Float32ColumnReader : IColumnReader<float>
{
    /// <inheritdoc />
    public string TypeName => "Float32";

    /// <inheritdoc />
    public Type ClrType => typeof(float);

    /// <inheritdoc />
    public float ReadValue(ref ProtocolReader reader)
    {
        var bytes = reader.ReadBytes(sizeof(float));
        return BinaryPrimitives.ReadSingleLittleEndian(bytes.Span);
    }

    /// <inheritdoc />
    public TypedColumn<float> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        var pool = ArrayPool<float>.Shared;
        var values = pool.Rent(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            values[i] = ReadValue(ref reader);
        }
        return new TypedColumn<float>(values, rowCount, pool);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
