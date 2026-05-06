using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for DateTime values.
/// DateTime in ClickHouse is stored as a 32-bit Unix timestamp (seconds since epoch).
/// </summary>
internal sealed class DateTimeColumnWriter : IColumnWriter<DateTime>
{
    private static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    /// <inheritdoc />
    public string TypeName => "DateTime";

    /// <inheritdoc />
    public Type ClrType => typeof(DateTime);

    /// <summary>
    /// Placeholder used by <see cref="NullableColumnWriter{T}"/> for null
    /// slots. <c>default(DateTime)</c> is <c>0001-01-01</c>, which is below
    /// <see cref="UnixEpoch"/> and would fail the range check in
    /// <see cref="WriteValue"/>; the epoch is the minimum representable
    /// DateTime on the wire.
    /// </summary>
    public DateTime NullPlaceholder => UnixEpoch;

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, DateTime[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            WriteValue(ref writer, values[i]);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, DateTime value)
    {
        var utcValue = value.Kind == DateTimeKind.Local ? value.ToUniversalTime() : value;
        var seconds = (long)(utcValue - UnixEpoch).TotalSeconds;
        if (seconds < 0 || seconds > uint.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(value),
                $"DateTime {value:O} is outside the legacy DateTime range " +
                $"[1970-01-01, 2106-02-07 UTC]. Use DateTime64 for wider ranges.");
        writer.WriteUInt32((uint)seconds);
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            WriteValue(ref writer, (DateTime)values[i]!);
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        WriteValue(ref writer, (DateTime)value!);
    }
}
