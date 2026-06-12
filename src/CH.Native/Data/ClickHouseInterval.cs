namespace CH.Native.Data;

/// <summary>
/// The unit of a ClickHouse Interval value. Matches the server's eleven
/// <c>Interval*</c> data types (system.data_type_families).
/// </summary>
public enum IntervalUnit
{
    /// <summary>IntervalNanosecond.</summary>
    Nanosecond = 0,

    /// <summary>IntervalMicrosecond.</summary>
    Microsecond = 1,

    /// <summary>IntervalMillisecond.</summary>
    Millisecond = 2,

    /// <summary>IntervalSecond.</summary>
    Second = 3,

    /// <summary>IntervalMinute.</summary>
    Minute = 4,

    /// <summary>IntervalHour.</summary>
    Hour = 5,

    /// <summary>IntervalDay.</summary>
    Day = 6,

    /// <summary>IntervalWeek.</summary>
    Week = 7,

    /// <summary>IntervalMonth (calendar unit — no fixed duration).</summary>
    Month = 8,

    /// <summary>IntervalQuarter (calendar unit — no fixed duration).</summary>
    Quarter = 9,

    /// <summary>IntervalYear (calendar unit — no fixed duration).</summary>
    Year = 10,
}

/// <summary>
/// A ClickHouse Interval value: a signed count of <see cref="Unit"/>s, e.g.
/// <c>SELECT INTERVAL 3 DAY</c> yields <c>(3, Day)</c>. The wire format is Int64.
/// </summary>
/// <remarks>
/// Intervals are deliberately NOT mapped to <see cref="TimeSpan"/>: Month, Quarter,
/// and Year are calendar units with no fixed duration. Use <see cref="ToTimeSpan"/>
/// for time-based units; it throws for calendar units.
/// </remarks>
public readonly struct ClickHouseInterval : IEquatable<ClickHouseInterval>
{
    /// <summary>
    /// Creates an interval value.
    /// </summary>
    /// <param name="value">The signed count of units.</param>
    /// <param name="unit">The interval unit.</param>
    public ClickHouseInterval(long value, IntervalUnit unit)
    {
        Value = value;
        Unit = unit;
    }

    /// <summary>
    /// Gets the signed count of <see cref="Unit"/>s.
    /// </summary>
    public long Value { get; }

    /// <summary>
    /// Gets the interval unit.
    /// </summary>
    public IntervalUnit Unit { get; }

    /// <summary>
    /// Gets whether <see cref="Unit"/> is a calendar unit (Month, Quarter, Year) with
    /// no fixed duration — <see cref="ToTimeSpan"/> throws for these.
    /// </summary>
    public bool IsCalendarUnit => Unit is IntervalUnit.Month or IntervalUnit.Quarter or IntervalUnit.Year;

    /// <summary>
    /// Converts a time-based interval to a <see cref="TimeSpan"/>.
    /// Nanosecond values are truncated toward zero to .NET's 100 ns tick resolution.
    /// </summary>
    /// <returns>The equivalent time span.</returns>
    /// <exception cref="NotSupportedException">
    /// The unit is a calendar unit (Month, Quarter, Year) with no fixed duration.
    /// </exception>
    /// <exception cref="OverflowException">The value does not fit in a TimeSpan.</exception>
    public TimeSpan ToTimeSpan() => Unit switch
    {
        IntervalUnit.Nanosecond => TimeSpan.FromTicks(Value / TimeSpan.NanosecondsPerTick),
        IntervalUnit.Microsecond => TimeSpan.FromTicks(checked(Value * TimeSpan.TicksPerMicrosecond)),
        IntervalUnit.Millisecond => TimeSpan.FromTicks(checked(Value * TimeSpan.TicksPerMillisecond)),
        IntervalUnit.Second => TimeSpan.FromTicks(checked(Value * TimeSpan.TicksPerSecond)),
        IntervalUnit.Minute => TimeSpan.FromTicks(checked(Value * TimeSpan.TicksPerMinute)),
        IntervalUnit.Hour => TimeSpan.FromTicks(checked(Value * TimeSpan.TicksPerHour)),
        IntervalUnit.Day => TimeSpan.FromTicks(checked(Value * TimeSpan.TicksPerDay)),
        IntervalUnit.Week => TimeSpan.FromTicks(checked(Value * (7 * TimeSpan.TicksPerDay))),
        _ => throw new NotSupportedException(
            $"Interval unit {Unit} is a calendar unit with no fixed duration and cannot be converted to a TimeSpan."),
    };

    /// <inheritdoc />
    public bool Equals(ClickHouseInterval other) => Value == other.Value && Unit == other.Unit;

    /// <inheritdoc />
    public override bool Equals(object? obj) => obj is ClickHouseInterval other && Equals(other);

    /// <inheritdoc />
    public override int GetHashCode() => HashCode.Combine(Value, Unit);

    /// <summary>
    /// Equality operator.
    /// </summary>
    public static bool operator ==(ClickHouseInterval left, ClickHouseInterval right) => left.Equals(right);

    /// <summary>
    /// Inequality operator.
    /// </summary>
    public static bool operator !=(ClickHouseInterval left, ClickHouseInterval right) => !left.Equals(right);

    /// <inheritdoc />
    public override string ToString() => $"{Value} {Unit}";
}
