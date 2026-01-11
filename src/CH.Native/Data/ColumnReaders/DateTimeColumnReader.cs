using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for DateTime values.
/// DateTime in ClickHouse is stored as a 32-bit Unix timestamp (seconds since epoch).
/// </summary>
public sealed class DateTimeColumnReader : IColumnReader<DateTime>
{
    private static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    /// <inheritdoc />
    public string TypeName => "DateTime";

    /// <inheritdoc />
    public Type ClrType => typeof(DateTime);

    /// <inheritdoc />
    public DateTime ReadValue(ref ProtocolReader reader)
    {
        var timestamp = reader.ReadUInt32();
        return UnixEpoch.AddSeconds(timestamp);
    }

    /// <inheritdoc />
    public TypedColumn<DateTime> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        var pool = ArrayPool<DateTime>.Shared;
        var values = pool.Rent(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            values[i] = ReadValue(ref reader);
        }
        return new TypedColumn<DateTime>(values, rowCount, pool);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
