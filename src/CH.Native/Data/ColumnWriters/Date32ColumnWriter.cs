using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for Date32 values.
/// Date32 in ClickHouse is stored as Int32 representing days since 1970-01-01.
/// This allows for dates before 1970 (negative values) and dates beyond 2149.
/// </summary>
public sealed class Date32ColumnWriter : IColumnWriter<DateOnly>
{
    private static readonly DateOnly UnixEpoch = new(1970, 1, 1);

    /// <inheritdoc />
    public string TypeName => "Date32";

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
        writer.WriteInt32(days);
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
