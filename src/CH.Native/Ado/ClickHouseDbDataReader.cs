using System.Collections;
using System.Data;
using System.Data.Common;
using CH.Native.Results;

namespace CH.Native.Ado;

/// <summary>
/// A ClickHouse-specific implementation of <see cref="DbDataReader"/>.
/// </summary>
public sealed class ClickHouseDbDataReader : DbDataReader
{
    private readonly ClickHouseDataReader _inner;
    private bool _closed;
    private bool _initialized;
    private bool _hasFirstRow;
    private bool _firstRowConsumed;

    internal ClickHouseDbDataReader(ClickHouseDataReader inner)
    {
        _inner = inner;
    }

    /// <summary>
    /// Ensures the reader is initialized (has schema info).
    /// The underlying reader requires ReadAsync() to be called to get schema.
    /// We eagerly init and track the first row so ADO.NET semantics are correct.
    /// </summary>
    private void EnsureInitialized()
    {
        if (!_initialized)
        {
            // Synchronously initialize - this is needed for ADO.NET compatibility
            // where FieldCount, GetName, etc. must work before Read() is called
            _hasFirstRow = _inner.ReadAsync().GetAwaiter().GetResult();
            _initialized = true;
        }
    }

    private async ValueTask EnsureInitializedAsync(CancellationToken cancellationToken)
    {
        if (!_initialized)
        {
            _hasFirstRow = await _inner.ReadAsync(cancellationToken).ConfigureAwait(false);
            _initialized = true;
        }
    }

    /// <inheritdoc />
    public override int FieldCount
    {
        get
        {
            EnsureInitialized();
            return _inner.FieldCount;
        }
    }

    /// <inheritdoc />
    public override bool HasRows
    {
        get
        {
            EnsureInitialized();
            return _hasFirstRow || _inner.HasRows;
        }
    }

    /// <inheritdoc />
    public override bool IsClosed => _closed;

    /// <inheritdoc />
    public override int Depth => 0;

    /// <inheritdoc />
    public override int RecordsAffected => -1;

    /// <inheritdoc />
    public override object this[int ordinal] => GetValue(ordinal);

    /// <inheritdoc />
    public override object this[string name] => GetValue(GetOrdinal(name));

    /// <inheritdoc />
    public override bool Read()
    {
        return ReadAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <inheritdoc />
    public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
    {
        // First ensure we're initialized (gets schema + first row if any)
        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        // If this is the first Read() call and we have a first row buffered, return it
        if (!_firstRowConsumed && _hasFirstRow)
        {
            _firstRowConsumed = true;
            return true;
        }

        // Otherwise, read the next row
        return await _inner.ReadAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public override bool NextResult() => false;

    /// <inheritdoc />
    public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
        => Task.FromResult(false);

    /// <inheritdoc />
    public override void Close()
    {
        CloseAsync().GetAwaiter().GetResult();
    }

    /// <inheritdoc />
    public override async Task CloseAsync()
    {
        if (!_closed)
        {
            await _inner.DisposeAsync().ConfigureAwait(false);
            _closed = true;
        }
    }

    // Type getters

    /// <inheritdoc />
    public override bool GetBoolean(int ordinal) => _inner.GetFieldValue<bool>(ordinal);

    /// <inheritdoc />
    public override byte GetByte(int ordinal) => _inner.GetFieldValue<byte>(ordinal);

    /// <inheritdoc />
    public override char GetChar(int ordinal) => _inner.GetFieldValue<char>(ordinal);

    /// <inheritdoc />
    public override DateTime GetDateTime(int ordinal) => _inner.GetFieldValue<DateTime>(ordinal);

    /// <inheritdoc />
    public override decimal GetDecimal(int ordinal) => _inner.GetFieldValue<decimal>(ordinal);

    /// <inheritdoc />
    public override double GetDouble(int ordinal) => _inner.GetFieldValue<double>(ordinal);

    /// <inheritdoc />
    public override float GetFloat(int ordinal) => _inner.GetFieldValue<float>(ordinal);

    /// <inheritdoc />
    public override Guid GetGuid(int ordinal) => _inner.GetFieldValue<Guid>(ordinal);

    /// <inheritdoc />
    public override short GetInt16(int ordinal) => _inner.GetFieldValue<short>(ordinal);

    /// <inheritdoc />
    public override int GetInt32(int ordinal) => _inner.GetFieldValue<int>(ordinal);

    /// <inheritdoc />
    public override long GetInt64(int ordinal) => _inner.GetFieldValue<long>(ordinal);

    /// <inheritdoc />
    public override string GetString(int ordinal) => _inner.GetFieldValue<string>(ordinal);

    /// <inheritdoc />
    public override T GetFieldValue<T>(int ordinal) => _inner.GetFieldValue<T>(ordinal);

    /// <inheritdoc />
    public override object GetValue(int ordinal) => _inner.GetValue(ordinal) ?? DBNull.Value;

    /// <inheritdoc />
    public override int GetValues(object[] values)
    {
        var count = Math.Min(values.Length, FieldCount);
        for (int i = 0; i < count; i++)
            values[i] = GetValue(i);
        return count;
    }

    /// <inheritdoc />
    public override bool IsDBNull(int ordinal) => _inner.IsDBNull(ordinal);

    /// <inheritdoc />
    public override int GetOrdinal(string name)
    {
        EnsureInitialized();
        return _inner.GetOrdinal(name);
    }

    /// <inheritdoc />
    public override string GetName(int ordinal)
    {
        EnsureInitialized();
        return _inner.GetName(ordinal);
    }

    /// <inheritdoc />
    public override string GetDataTypeName(int ordinal)
    {
        EnsureInitialized();
        return _inner.GetTypeName(ordinal);
    }

    /// <inheritdoc />
    public override Type GetFieldType(int ordinal)
    {
        EnsureInitialized();
        return _inner.GetFieldType(ordinal);
    }

    /// <inheritdoc />
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        throw new NotSupportedException("GetBytes is not supported. Use GetFieldValue<byte[]>() instead.");
    }

    /// <inheritdoc />
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        throw new NotSupportedException("GetChars is not supported. Use GetString() instead.");
    }

    /// <inheritdoc />
    public override IEnumerator GetEnumerator()
    {
        return new DbEnumerator(this);
    }

    /// <inheritdoc />
    public override DataTable GetSchemaTable()
    {
        EnsureInitialized();
        var table = new DataTable("SchemaTable");

        table.Columns.Add("ColumnName", typeof(string));
        table.Columns.Add("ColumnOrdinal", typeof(int));
        table.Columns.Add("DataType", typeof(Type));
        table.Columns.Add("ProviderType", typeof(string));
        table.Columns.Add("AllowDBNull", typeof(bool));

        for (int i = 0; i < FieldCount; i++)
        {
            var row = table.NewRow();
            row["ColumnName"] = GetName(i);
            row["ColumnOrdinal"] = i;
            row["DataType"] = GetFieldType(i);
            var typeName = GetDataTypeName(i);
            row["ProviderType"] = typeName;
            row["AllowDBNull"] = typeName.StartsWith("Nullable(", StringComparison.Ordinal);
            table.Rows.Add(row);
        }

        return table;
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing)
            Close();
        base.Dispose(disposing);
    }

    /// <inheritdoc />
    public override async ValueTask DisposeAsync()
    {
        await CloseAsync().ConfigureAwait(false);
        await base.DisposeAsync().ConfigureAwait(false);
    }
}
