using System.Numerics;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for Int256 values (stored as BigInteger).
/// </summary>
public sealed class Int256ColumnWriter : IColumnWriter<BigInteger>
{
    /// <inheritdoc />
    public string TypeName => "Int256";

    /// <inheritdoc />
    public Type ClrType => typeof(BigInteger);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, BigInteger[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteInt256(values[i]);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, BigInteger value)
    {
        writer.WriteInt256(value);
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteInt256((BigInteger)values[i]!);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        writer.WriteInt256((BigInteger)value!);
    }
}
