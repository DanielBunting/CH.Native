using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for DateTime values.
/// DateTime in ClickHouse is stored as a 32-bit Unix timestamp (seconds since epoch).
/// </summary>
public sealed class DateTimeColumnWriter : IColumnWriter<DateTime>
{
    private static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    /// <inheritdoc />
    public string TypeName => "DateTime";

    /// <inheritdoc />
    public Type ClrType => typeof(DateTime);

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
        writer.WriteUInt32((uint)Math.Max(0, Math.Min(seconds, uint.MaxValue)));
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
