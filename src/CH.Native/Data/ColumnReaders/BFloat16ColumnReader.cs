using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for BFloat16 (brain float) values.
/// BFloat16 is the high 16 bits of IEEE-754 binary32 — 1 sign bit, 8 exponent bits, 7 mantissa bits.
/// Returned as float by zero-extending the low 16 mantissa bits.
/// </summary>
internal sealed class BFloat16ColumnReader : IColumnReader<float>
{
    /// <inheritdoc />
    public string TypeName => "BFloat16";

    /// <inheritdoc />
    public Type ClrType => typeof(float);

    /// <inheritdoc />
    public float ReadValue(ref ProtocolReader reader)
    {
        var raw = reader.ReadUInt16();
        var bits = ((uint)raw) << 16;
        return BitConverter.UInt32BitsToSingle(bits);
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
