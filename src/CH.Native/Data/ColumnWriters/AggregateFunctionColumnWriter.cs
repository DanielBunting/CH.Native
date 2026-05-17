using CH.Native.Data.AggregateState;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for <c>AggregateFunction(name, T...)</c> values. Writes each row's
/// state bytes verbatim via the resolved <see cref="IAggregateFunctionStateFormat"/>,
/// which validates the byte-count matches the expected per-row shape.
/// </summary>
internal sealed class AggregateFunctionColumnWriter : IColumnWriter<ClickHouseAggregateState>
{
    private readonly IAggregateFunctionStateFormat _format;

    public AggregateFunctionColumnWriter(string typeName, IAggregateFunctionStateFormat format)
    {
        TypeName = typeName;
        _format = format;
    }

    public string TypeName { get; }

    public Type ClrType => typeof(ClickHouseAggregateState);

    public ClickHouseAggregateState NullPlaceholder => ClickHouseAggregateState.Empty;

    public void WriteValue(ref ProtocolWriter writer, ClickHouseAggregateState value)
    {
        ArgumentNullException.ThrowIfNull(value);
        _format.WriteOneState(ref writer, value.State);
    }

    public void WriteColumn(ref ProtocolWriter writer, ClickHouseAggregateState[] values)
    {
        for (int i = 0; i < values.Length; i++)
            WriteValue(ref writer, values[i]);
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        if (value is null)
            throw new ArgumentNullException(nameof(value));
        WriteValue(ref writer, (ClickHouseAggregateState)value);
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is null)
                throw new ArgumentNullException(nameof(values), $"AggregateFunction state at index {i} is null.");
            WriteValue(ref writer, (ClickHouseAggregateState)values[i]!);
        }
    }
}
