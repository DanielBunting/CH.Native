using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for Date values.
/// Date in ClickHouse is stored as UInt16 representing days since 1970-01-01.
/// </summary>
public sealed class DateColumnWriter : IColumnWriter<DateOnly>
{
    private static readonly DateOnly UnixEpoch = new(1970, 1, 1);

    /// <inheritdoc />
    public string TypeName => "Date";

    /// <inheritdoc />
    public Type ClrType => typeof(DateOnly);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, DateOnly[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            WriteValue(ref writer, values[i]);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, DateOnly value)
    {
        var days = value.DayNumber - UnixEpoch.DayNumber;
        writer.WriteUInt16((ushort)Math.Max(0, Math.Min(days, ushort.MaxValue)));
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            WriteValue(ref writer, (DateOnly)values[i]!);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        WriteValue(ref writer, (DateOnly)value!);
    }
}
