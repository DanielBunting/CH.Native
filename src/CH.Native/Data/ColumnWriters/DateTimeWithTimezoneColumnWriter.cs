using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for DateTime values with timezone parameter.
/// Wire format is the same as DateTime (UInt32 Unix timestamp).
/// </summary>
public sealed class DateTimeWithTimezoneColumnWriter : IColumnWriter<DateTimeOffset>
{
    private readonly string _timezone;

    /// <summary>
    /// Creates a DateTime writer with the specified timezone.
    /// </summary>
    /// <param name="timezone">The IANA or Windows timezone name (e.g., "UTC", "America/New_York").</param>
    public DateTimeWithTimezoneColumnWriter(string timezone)
    {
        _timezone = timezone;
    }

    /// <inheritdoc />
    public string TypeName => $"DateTime('{_timezone}')";

    /// <inheritdoc />
    public Type ClrType => typeof(DateTimeOffset);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, DateTimeOffset[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            WriteValue(ref writer, values[i]);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, DateTimeOffset value)
    {
        var seconds = value.ToUnixTimeSeconds();
        writer.WriteUInt32((uint)Math.Max(0, Math.Min(seconds, uint.MaxValue)));
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            WriteValue(ref writer, (DateTimeOffset)values[i]!);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        WriteValue(ref writer, (DateTimeOffset)value!);
    }
}
