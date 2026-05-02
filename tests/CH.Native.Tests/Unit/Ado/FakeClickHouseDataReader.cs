using CH.Native.Results;

namespace CH.Native.Tests.Unit.Ado;

/// <summary>
/// In-memory <see cref="IClickHouseDataReader"/> for unit tests of
/// <see cref="CH.Native.Ado.ClickHouseDbDataReader"/>. Scripts column metadata
/// and a row sequence; tracks dispose state so tests can assert ownership
/// transfer.
/// </summary>
internal sealed class FakeClickHouseDataReader : IClickHouseDataReader, IDisposable
{
    public sealed record ColumnDef(string Name, Type ClrType, string ChType, bool IsNullable);

    public void Dispose() => Disposed = true;

    private readonly ColumnDef[] _columns;
    private readonly object?[][] _rows;
    private int _rowIndex = -1;
    public bool Disposed { get; private set; }
    public int ReadCount { get; private set; }

    public FakeClickHouseDataReader(IEnumerable<ColumnDef> columns, IEnumerable<object?[]>? rows = null, string? queryId = "test-query-id")
    {
        _columns = columns.ToArray();
        _rows = (rows ?? Array.Empty<object?[]>()).ToArray();
        QueryId = queryId;
    }

    public string? QueryId { get; }
    public int FieldCount => _columns.Length;
    public bool HasRows => _rows.Length > 0;

    public ValueTask<bool> ReadAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ReadCount++;
        _rowIndex++;
        return ValueTask.FromResult(_rowIndex < _rows.Length);
    }

    public object? GetValue(int ordinal)
    {
        ValidateOrdinal(ordinal);
        EnsureRow();
        return _rows[_rowIndex][ordinal];
    }

    public T GetFieldValue<T>(int ordinal)
    {
        var value = GetValue(ordinal);
        if (value is null)
        {
            if (default(T) is null) return default!;
            throw new InvalidCastException($"Cannot convert null to non-nullable {typeof(T).Name}");
        }
        if (value is T typed) return typed;
        return (T)Convert.ChangeType(value, typeof(T));
    }

    public int GetOrdinal(string name)
    {
        for (int i = 0; i < _columns.Length; i++)
        {
            if (string.Equals(_columns[i].Name, name, StringComparison.OrdinalIgnoreCase))
                return i;
        }
        throw new ArgumentException($"Column '{name}' not found.", nameof(name));
    }

    public string GetName(int ordinal)
    {
        ValidateOrdinal(ordinal);
        return _columns[ordinal].Name;
    }

    public string GetTypeName(int ordinal)
    {
        ValidateOrdinal(ordinal);
        return _columns[ordinal].ChType;
    }

    public Type GetFieldType(int ordinal)
    {
        ValidateOrdinal(ordinal);
        return _columns[ordinal].ClrType;
    }

    public bool IsDBNull(int ordinal) => GetValue(ordinal) is null;

    public ValueTask DisposeAsync()
    {
        Disposed = true;
        return ValueTask.CompletedTask;
    }

    private void ValidateOrdinal(int ordinal)
    {
        if (ordinal < 0 || ordinal >= _columns.Length)
            throw new IndexOutOfRangeException($"Ordinal {ordinal} is out of range [0, {_columns.Length - 1}].");
    }

    private void EnsureRow()
    {
        if (_rowIndex < 0 || _rowIndex >= _rows.Length)
            throw new InvalidOperationException("No current row — call ReadAsync first.");
    }
}
