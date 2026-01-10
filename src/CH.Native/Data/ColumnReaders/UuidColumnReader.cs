using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for UUID values.
/// ClickHouse stores UUID as 16 bytes with each 8-byte half reversed.
/// This reader handles the byte order transformation to produce correct .NET Guid values.
/// </summary>
public sealed class UuidColumnReader : IColumnReader<Guid>
{
    /// <inheritdoc />
    public string TypeName => "UUID";

    /// <inheritdoc />
    public Type ClrType => typeof(Guid);

    /// <inheritdoc />
    public Guid ReadValue(ref ProtocolReader reader)
    {
        // ClickHouse stores UUID as 16 bytes with each 8-byte half reversed.
        // Wire format: [first_half_reversed][second_half_reversed]
        //
        // .NET Guid constructor expects bytes in a specific layout:
        // - Bytes 0-3: Data1 as little-endian Int32
        // - Bytes 4-5: Data2 as little-endian Int16
        // - Bytes 6-7: Data3 as little-endian Int16
        // - Bytes 8-15: Data4 as raw bytes
        //
        // The mapping accounts for both the ClickHouse reversal and .NET's LE expectations.

        var rawBytes = reader.ReadBytes(16);
        Span<byte> wire = stackalloc byte[16];
        rawBytes.Span.CopyTo(wire);

        Span<byte> result = stackalloc byte[16];

        // Data1 (bytes 0-3): UUID bytes 0-3 reversed for LE, from wire bytes 4-7
        result[0] = wire[4];
        result[1] = wire[5];
        result[2] = wire[6];
        result[3] = wire[7];

        // Data2 (bytes 4-5): UUID bytes 4-5 reversed for LE, from wire bytes 2-3
        result[4] = wire[2];
        result[5] = wire[3];

        // Data3 (bytes 6-7): UUID bytes 6-7 reversed for LE, from wire bytes 0-1
        result[6] = wire[0];
        result[7] = wire[1];

        // Data4 (bytes 8-15): UUID bytes 8-15 as-is, from wire bytes 8-15 reversed
        result[8] = wire[15];
        result[9] = wire[14];
        result[10] = wire[13];
        result[11] = wire[12];
        result[12] = wire[11];
        result[13] = wire[10];
        result[14] = wire[9];
        result[15] = wire[8];

        return new Guid(result);
    }

    /// <inheritdoc />
    public TypedColumn<Guid> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        var pool = ArrayPool<Guid>.Shared;
        var values = pool.Rent(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            values[i] = ReadValue(ref reader);
        }
        return new TypedColumn<Guid>(values, rowCount, pool);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
