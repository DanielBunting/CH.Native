using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for ClickHouse <c>Date</c> values.
/// </summary>
/// <remarks>
/// <para>
/// ClickHouse <c>Date</c> is a <see cref="ushort"/> count of days from
/// 1970-01-01, so the column can only represent <c>1970-01-01</c> through
/// <c>2149-06-06</c> (<see cref="ushort.MaxValue"/> days).
/// </para>
/// <para>
/// Values outside that range raise <see cref="ArgumentOutOfRangeException"/>
/// — pre-fix they were silently clamped, producing the wrong year on
/// round-trip with no diagnostic. Callers needing the wider 1900-01-01 …
/// 2299-12-31 window should declare the column as <c>Date32</c> and use
/// <see cref="Date32ColumnWriter"/>.
/// </para>
/// </remarks>
internal sealed class DateColumnWriter : IColumnWriter<DateOnly>
{
    private static readonly DateOnly UnixEpoch = new(1970, 1, 1);
    private static readonly DateOnly Saturation = UnixEpoch.AddDays(ushort.MaxValue); // 2149-06-06

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
        if (value < UnixEpoch || value > Saturation)
        {
            throw new ArgumentOutOfRangeException(
                nameof(value), value,
                $"Date column accepts {UnixEpoch:O} through {Saturation:O}; " +
                "use Date32 (Date32ColumnWriter) for the wider 1900-01-01 to 2299-12-31 range.");
        }

        var days = value.DayNumber - UnixEpoch.DayNumber;
        writer.WriteUInt16((ushort)days);
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
