using CH.Native.Data.AggregateState;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for <c>AggregateFunction(name, T...)</c> values. Delegates per-row
/// decoding to an <see cref="IAggregateFunctionStateFormat"/> resolved from the
/// state-format registry.
/// </summary>
internal sealed class AggregateFunctionColumnReader : IColumnReader<ClickHouseAggregateState>
{
    private readonly string _functionName;
    private readonly IAggregateFunctionStateFormat _format;

    public AggregateFunctionColumnReader(
        string typeName,
        string functionName,
        IAggregateFunctionStateFormat format)
    {
        TypeName = typeName;
        _functionName = functionName;
        _format = format;
    }

    public string TypeName { get; }

    public Type ClrType => typeof(ClickHouseAggregateState);

    public ClickHouseAggregateState ReadValue(ref ProtocolReader reader)
        => new(_format.ReadOneState(ref reader), _functionName);

    public TypedColumn<ClickHouseAggregateState> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        var data = new ClickHouseAggregateState[rowCount];
        for (int i = 0; i < rowCount; i++)
            data[i] = ReadValue(ref reader);
        return new TypedColumn<ClickHouseAggregateState>(data);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
        => ReadTypedColumn(ref reader, rowCount);
}
