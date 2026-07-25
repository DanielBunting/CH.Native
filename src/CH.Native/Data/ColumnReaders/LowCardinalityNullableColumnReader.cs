using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for <c>LowCardinality(Nullable(T))</c> where the inner value is a
/// value type (e.g. Int64, Date, Enum8). Unlike the base
/// <see cref="LowCardinalityColumnReader{T}"/> — whose generic parameter is the
/// non-nullable base type and whose <see cref="LowCardinalityColumnReader{T}.ReadTypedColumn"/>
/// collapses the index-0 null sentinel to <c>default(T)</c> — this reader is
/// genuinely an <see cref="IColumnReader{T}"/> of <c>T?</c>. That matters because
/// composite readers (Array/Map/Nested) build their element storage from
/// <see cref="IColumnReader.ClrType"/> and then re-check <c>is IColumnReader&lt;T&gt;</c>;
/// only a reader that IS <c>IColumnReader&lt;T?&gt;</c> makes them carry the nullable
/// element type and preserve NULLs (mirroring how <see cref="NullableColumnReader{T}"/>
/// lets <c>Array(Nullable(Int32))</c> work).
/// </summary>
/// <remarks>
/// All wire reading is delegated to an inner <see cref="LowCardinalityColumnReader{T}"/>
/// (built with <c>isNullable: true</c>) via its <see cref="LowCardinalityColumnReader{T}.ReadDictionaryEncodedColumn"/>,
/// so the LowCardinality frame format lives in exactly one place. This reader only
/// projects that intermediate to a materialised <see cref="TypedColumn{T}"/> of
/// <c>T?</c>; dictionary encoding is not preserved (nested composites materialise
/// element values regardless).
/// </remarks>
/// <typeparam name="T">The underlying value type (the Nullable wrapper is applied here).</typeparam>
internal sealed class LowCardinalityNullableColumnReader<T> : IColumnReader<T?>
    where T : struct
{
    private readonly LowCardinalityColumnReader<T> _inner;
    private readonly ArrayPool<T?> _resultPool;

    /// <summary>
    /// Creates the reader from the non-generic base value reader (e.g.
    /// <c>Int64ColumnReader</c>). The base reader reads the dictionary values;
    /// nullability is represented by dictionary index 0.
    /// </summary>
    public LowCardinalityNullableColumnReader(IColumnReader innerReader)
        : this(innerReader, ArrayPool<T?>.Shared)
    {
    }

    internal LowCardinalityNullableColumnReader(IColumnReader innerReader, ArrayPool<T?> resultPool)
    {
        // The base ctor validates innerReader is IColumnReader<T>.
        _inner = new LowCardinalityColumnReader<T>(innerReader, isNullable: true);
        _resultPool = resultPool ?? throw new ArgumentNullException(nameof(resultPool));
    }

    /// <inheritdoc />
    public string TypeName => _inner.TypeName;

    /// <inheritdoc />
    public Type ClrType => typeof(T?);

    /// <inheritdoc />
    public void ReadPrefix(ref ProtocolReader reader) => _inner.ReadPrefix(ref reader);

    /// <inheritdoc />
    public T? ReadValue(ref ProtocolReader reader)
    {
        using var values = ReadTypedColumn(ref reader, 1);
        return values[0];
    }

    /// <inheritdoc />
    public TypedColumn<T?> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        if (rowCount == 0)
            return new TypedColumn<T?>(Array.Empty<T?>());

        // Reuse the base reader's frame decode. The intermediate carries the
        // null sentinel via its (already-fixed) IsNull / indexer.
        using var enc = _inner.ReadDictionaryEncodedColumn(ref reader, rowCount);

        // Result rental happens before the loop, so any throw between them must
        // return it — otherwise the pool ledger drifts on a malformed read.
        var result = _resultPool.Rent(rowCount);
        try
        {
            for (int i = 0; i < rowCount; i++)
            {
                result[i] = enc.IsNull(i) ? (T?)null : enc[i];
            }

            return new TypedColumn<T?>(result, rowCount, _resultPool);
        }
        catch
        {
            _resultPool.Return(result);
            throw;
        }
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
