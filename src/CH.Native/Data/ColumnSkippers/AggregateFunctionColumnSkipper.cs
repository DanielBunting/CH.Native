using CH.Native.Data.AggregateState;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnSkippers;

/// <summary>
/// Column skipper for <c>AggregateFunction(name, T...)</c> values. Delegates per-row
/// skipping to the resolved <see cref="IAggregateFunctionStateFormat"/>.
/// </summary>
internal sealed class AggregateFunctionColumnSkipper : IColumnSkipper
{
    private readonly IAggregateFunctionStateFormat _format;

    public AggregateFunctionColumnSkipper(string typeName, IAggregateFunctionStateFormat format)
    {
        TypeName = typeName;
        _format = format;
    }

    public string TypeName { get; }

    public bool TrySkipColumn(ref ProtocolReader reader, int rowCount)
    {
        for (int i = 0; i < rowCount; i++)
        {
            if (!_format.TrySkipOneState(ref reader))
                return false;
        }
        return true;
    }
}
