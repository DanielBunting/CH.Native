using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for the eleven Interval* types (IntervalNanosecond … IntervalYear).
/// The wire format is a signed Int64 count of units; values surface as
/// <see cref="ClickHouseInterval"/> so the unit travels with the count (Month, Quarter,
/// and Year are calendar units that must not silently collapse into a fixed duration).
/// </summary>
internal sealed class IntervalColumnReader : IColumnReader<ClickHouseInterval>
{
    private readonly IntervalUnit _unit;
    private readonly string _typeName;

    public IntervalColumnReader(IntervalUnit unit)
    {
        _unit = unit;
        _typeName = $"Interval{unit}";
    }

    /// <inheritdoc />
    public string TypeName => _typeName;

    /// <inheritdoc />
    public Type ClrType => typeof(ClickHouseInterval);

    /// <inheritdoc />
    public ClickHouseInterval ReadValue(ref ProtocolReader reader)
    {
        return new ClickHouseInterval(reader.ReadInt64(), _unit);
    }

    /// <inheritdoc />
    public TypedColumn<ClickHouseInterval> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        var pool = ArrayPool<ClickHouseInterval>.Shared;
        var values = pool.Rent(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            values[i] = new ClickHouseInterval(reader.ReadInt64(), _unit);
        }
        return new TypedColumn<ClickHouseInterval>(values, rowCount, pool);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
