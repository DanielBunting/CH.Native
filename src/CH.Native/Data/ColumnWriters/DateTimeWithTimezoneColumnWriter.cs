using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for DateTime values with timezone parameter.
/// Wire format is the same as DateTime (UInt32 Unix timestamp).
/// </summary>
internal sealed class DateTimeWithTimezoneColumnWriter : IColumnWriter<DateTimeOffset>
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

    /// <summary>
    /// Placeholder used by <see cref="NullableColumnWriter{T}"/> for null
    /// slots. <c>default(DateTimeOffset)</c> is below the Unix epoch and would
    /// fail the range check in <see cref="WriteValue"/>; the epoch is the
    /// minimum representable value on the wire.
    /// </summary>
    public DateTimeOffset NullPlaceholder => DateTimeOffset.UnixEpoch;

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
        if (seconds < 0 || seconds > uint.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(value),
                $"DateTimeOffset {value:O} is outside the legacy DateTime range " +
                $"[1970-01-01, 2106-02-07 UTC]. Use DateTime64 for wider ranges.");
        writer.WriteUInt32((uint)seconds);
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
