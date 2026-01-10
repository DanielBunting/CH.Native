using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for Date32 values.
/// Date32 in ClickHouse is stored as Int32 representing days since 1970-01-01.
/// This allows for dates before 1970 (negative values) and dates beyond 2149.
/// </summary>
public sealed class Date32ColumnReader : IColumnReader<DateOnly>
{
    private static readonly DateOnly UnixEpoch = new(1970, 1, 1);

    /// <inheritdoc />
    public string TypeName => "Date32";

    /// <inheritdoc />
    public Type ClrType => typeof(DateOnly);

    /// <inheritdoc />
    public DateOnly ReadValue(ref ProtocolReader reader)
    {
        var days = reader.ReadInt32();
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
