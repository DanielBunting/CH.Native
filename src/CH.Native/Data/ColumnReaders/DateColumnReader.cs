using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for Date values.
/// Date in ClickHouse is stored as UInt16 representing days since 1970-01-01.
/// </summary>
public sealed class DateColumnReader : IColumnReader<DateOnly>
{
    private static readonly DateOnly UnixEpoch = new(1970, 1, 1);

    /// <inheritdoc />
    public string TypeName => "Date";

    /// <inheritdoc />
    public Type ClrType => typeof(DateOnly);

    /// <inheritdoc />
    public DateOnly ReadValue(ref ProtocolReader reader)
    {
        var days = reader.ReadUInt16();
        return UnixEpoch.AddDays(days);
    }

    /// <inheritdoc />
    public TypedColumn<DateOnly> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        var pool = ArrayPool<DateOnly>.Shared;
        var values = pool.Rent(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            values[i] = ReadValue(ref reader);
        }
        return new TypedColumn<DateOnly>(values, rowCount, pool);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
