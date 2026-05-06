using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for Time values.
/// Time in ClickHouse is stored as Int32 seconds since 00:00:00 (wall-clock, no date).
/// </summary>
internal sealed class TimeColumnReader : IColumnReader<TimeOnly>
{
    private const int SecondsPerDay = 86_400;

    /// <inheritdoc />
    public string TypeName => "Time";

    /// <inheritdoc />
    public Type ClrType => typeof(TimeOnly);

    /// <inheritdoc />
    public TimeOnly ReadValue(ref ProtocolReader reader)
    {
        var seconds = reader.ReadInt32();
        if ((uint)seconds >= SecondsPerDay)
            throw new OverflowException(
                $"Time value {seconds}s is outside the representable range [0, {SecondsPerDay}); use the raw Int32 reader for negative or wrap-around times.");
        return new TimeOnly(seconds * TimeSpan.TicksPerSecond);
    }

    /// <inheritdoc />
    public TypedColumn<TimeOnly> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        var pool = ArrayPool<TimeOnly>.Shared;
        var values = pool.Rent(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            values[i] = ReadValue(ref reader);
        }
        return new TypedColumn<TimeOnly>(values, rowCount, pool);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
