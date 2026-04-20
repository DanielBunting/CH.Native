using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for Time values.
/// Time in ClickHouse is stored as Int32 seconds since 00:00:00 (wall-clock, no date).
/// </summary>
public sealed class TimeColumnWriter : IColumnWriter<TimeOnly>
{
    /// <inheritdoc />
    public string TypeName => "Time";

    /// <inheritdoc />
    public Type ClrType => typeof(TimeOnly);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, TimeOnly[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            WriteValue(ref writer, values[i]);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, TimeOnly value)
    {
        var seconds = (int)(value.Ticks / TimeSpan.TicksPerSecond);
        writer.WriteInt32(seconds);
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            WriteValue(ref writer, (TimeOnly)values[i]!);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        WriteValue(ref writer, (TimeOnly)value!);
    }
}
