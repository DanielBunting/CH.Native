using CH.Native.Mapping;

namespace CH.Native.SystemTests.BulkInsertFailures.Helpers;

/// <summary>
/// POCO whose <see cref="Payload"/> getter throws on the row whose <see cref="Id"/>
/// matches a configured fail-index. Used to exercise the row-mapper failure path
/// without needing to rebuild the extractor pipeline. The exception type is
/// <see cref="FailingRowException"/> so tests can assert the precise propagation
/// pattern (i.e. the inserter does not wrap the exception, just lets it bubble).
/// </summary>
public sealed class FailingRow
{
    private static readonly AsyncLocal<int> FailIndexAsync = new();

    /// <summary>
    /// Sets the row index whose getter will throw. Set to a negative value to
    /// disable the failure (every row succeeds).
    /// </summary>
    public static IDisposable WithFailIndex(int index)
    {
        var prior = FailIndexAsync.Value;
        FailIndexAsync.Value = index;
        return new Reset(prior);
    }

    [ClickHouseColumn(Name = "id", Order = 0)]
    public int Id { get; set; }

    [ClickHouseColumn(Name = "payload", Order = 1)]
    public string Payload
    {
        get
        {
            if (FailIndexAsync.Value > 0 && Id == FailIndexAsync.Value)
                throw new FailingRowException(Id);
            return _payload;
        }
        set => _payload = value;
    }

    private string _payload = "";

    private sealed class Reset : IDisposable
    {
        private readonly int _prior;
        public Reset(int prior) => _prior = prior;
        public void Dispose() => FailIndexAsync.Value = _prior;
    }
}

public sealed class FailingRowException : Exception
{
    public int RowIndex { get; }
    public FailingRowException(int rowIndex)
        : base($"Row mapper deliberately failed at row index {rowIndex}.")
    {
        RowIndex = rowIndex;
    }
}
