using System.Numerics;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for Int256 values.
/// </summary>
/// <remarks>
/// Since .NET doesn't have a native 256-bit integer type,
/// BigInteger is used to represent Int256 values.
/// </remarks>
public sealed class Int256ColumnReader : IColumnReader<BigInteger>
{
    /// <inheritdoc />
    public string TypeName => "Int256";

    /// <inheritdoc />
    public Type ClrType => typeof(BigInteger);

    /// <inheritdoc />
    public BigInteger ReadValue(ref ProtocolReader reader)
    {
        return reader.ReadInt256();
    }

    /// <inheritdoc />
    public TypedColumn<BigInteger> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        var values = new BigInteger[rowCount];
        for (int i = 0; i < rowCount; i++)
        {
            values[i] = reader.ReadInt256();
        }
        return new TypedColumn<BigInteger>(values);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
