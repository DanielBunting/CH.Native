using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for UUID values.
/// ClickHouse stores UUID as 16 bytes with each 8-byte half reversed.
/// This writer handles the byte order transformation from .NET Guid to ClickHouse wire format.
/// </summary>
public sealed class UuidColumnWriter : IColumnWriter<Guid>
{
    /// <inheritdoc />
    public string TypeName => "UUID";

    /// <inheritdoc />
    public Type ClrType => typeof(Guid);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, Guid[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            WriteValue(ref writer, values[i]);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, Guid value)
    {
        // This is the inverse of the reader transformation.
        // .NET Guid bytes layout:
        // - Bytes 0-3: Data1 as little-endian Int32
        // - Bytes 4-5: Data2 as little-endian Int16
        // - Bytes 6-7: Data3 as little-endian Int16
        // - Bytes 8-15: Data4 as raw bytes
        //
        // ClickHouse wire format: [first_half_reversed][second_half_reversed]

        var guidBytes = value.ToByteArray();

        // Wire bytes 0-1: from result bytes 6-7 (Data3 reversed)
        writer.WriteByte(guidBytes[6]);
        writer.WriteByte(guidBytes[7]);

        // Wire bytes 2-3: from result bytes 4-5 (Data2 reversed)
        writer.WriteByte(guidBytes[4]);
        writer.WriteByte(guidBytes[5]);

        // Wire bytes 4-7: from result bytes 0-3 (Data1 reversed)
        writer.WriteByte(guidBytes[0]);
        writer.WriteByte(guidBytes[1]);
        writer.WriteByte(guidBytes[2]);
        writer.WriteByte(guidBytes[3]);

        // Wire bytes 8-15: from result bytes 8-15 reversed
        writer.WriteByte(guidBytes[15]);
        writer.WriteByte(guidBytes[14]);
        writer.WriteByte(guidBytes[13]);
        writer.WriteByte(guidBytes[12]);
        writer.WriteByte(guidBytes[11]);
        writer.WriteByte(guidBytes[10]);
        writer.WriteByte(guidBytes[9]);
        writer.WriteByte(guidBytes[8]);
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            WriteValue(ref writer, (Guid)values[i]!);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        WriteValue(ref writer, (Guid)value!);
    }
}
