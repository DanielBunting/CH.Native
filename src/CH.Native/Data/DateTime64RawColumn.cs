using System.Buffers;

namespace CH.Native.Data;

/// <summary>
/// Column storage for DateTime64 with precision 8 or 9, where one wire unit is smaller
/// than a .NET 100 ns tick. <see cref="GetValue"/> returns <see cref="DateTime"/>
/// truncated toward zero to tick resolution (the long-standing default), while
/// <see cref="GetRawValue"/> exposes the exact Int64 wire value — a signed count of
/// 10^-<see cref="Precision"/> second units since the Unix epoch — for callers that
/// need the sub-tick digits (reachable via <c>GetFieldValue&lt;long&gt;</c>).
/// </summary>
public sealed class DateTime64RawColumn : ITypedColumn
{
    private static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private long[]? _values;
    private readonly int _count;
    private readonly long _divisor;

    internal DateTime64RawColumn(long[] values, int count, int precision, string? timezone)
    {
        _values = values;
        _count = count;
        Precision = precision;
        Timezone = timezone;
        _divisor = (long)Math.Pow(10, precision - 7);
    }

    /// <summary>
    /// Gets the column precision (8 or 9 — lower precisions are tick-exact and use the
    /// plain DateTime storage).
    /// </summary>
    public int Precision { get; }

    /// <summary>
    /// Gets the column timezone name, if specified.
    /// </summary>
    public string? Timezone { get; }

    /// <inheritdoc />
    public Type ElementType => typeof(DateTime);

    /// <inheritdoc />
    public int Count => _count;

    /// <inheritdoc />
    public object? GetValue(int index)
    {
        if ((uint)index >= (uint)_count)
            throw new ArgumentOutOfRangeException(nameof(index));
        ObjectDisposedException.ThrowIf(_values is null, this);

        return UnixEpoch.AddTicks(_values[index] / _divisor);
    }

    /// <summary>
    /// Returns the exact Int64 wire value at <paramref name="index"/>: a signed count of
    /// 10^-<see cref="Precision"/> second units since the Unix epoch, with no precision
    /// loss (e.g. for DateTime64(9) this is <c>toUnixTimestamp64Nano</c>).
    /// </summary>
    /// <param name="index">The zero-based row index.</param>
    /// <returns>The raw wire value.</returns>
    public long GetRawValue(int index)
    {
        if ((uint)index >= (uint)_count)
            throw new ArgumentOutOfRangeException(nameof(index));
        ObjectDisposedException.ThrowIf(_values is null, this);

        return _values[index];
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_values != null)
        {
            ArrayPool<long>.Shared.Return(_values);
            _values = null;
        }
    }
}
