using System.Buffers;

namespace CH.Native.Data;

/// <summary>
/// Lazy nullable string column that wraps a null bitmap and a <see cref="RawStringColumn"/>.
/// Returns null for null rows, delegates to the inner column for non-null rows.
/// </summary>
public sealed class NullableRawStringColumn : ITypedColumn
{
    private byte[]? _nullBitmap;
    private readonly RawStringColumn _inner;
    private readonly int _count;

    /// <summary>
    /// Creates a new NullableRawStringColumn.
    /// </summary>
    /// <param name="nullBitmap">Pooled byte array where 0 = not null, non-zero = null. One byte per row.</param>
    /// <param name="inner">The inner RawStringColumn containing the string data.</param>
    /// <param name="count">The number of rows.</param>
    internal NullableRawStringColumn(byte[] nullBitmap, RawStringColumn inner, int count)
    {
        _nullBitmap = nullBitmap;
        _inner = inner;
        _count = count;
    }

    /// <inheritdoc />
    public Type ElementType => typeof(string);

    /// <inheritdoc />
    public int Count => _count;

    /// <inheritdoc />
    public object? GetValue(int index)
    {
        if ((uint)index >= (uint)_count)
            throw new ArgumentOutOfRangeException(nameof(index));

        return _nullBitmap![index] != 0 ? null : _inner.GetValue(index);
    }

    /// <summary>
    /// Returns a copy of the raw, un-decoded bytes of the value at
    /// <paramref name="index"/>, or <see langword="null"/> for a SQL null row.
    /// Preserves bytes that are not valid UTF-8 (which <see cref="GetValue"/> would
    /// replace with U+FFFD). The copy remains valid after the column is disposed.
    /// </summary>
    /// <param name="index">The zero-based row index.</param>
    /// <returns>The raw value bytes, empty for an empty string, or null for SQL null.</returns>
    public byte[]? GetBytesCopy(int index)
    {
        if ((uint)index >= (uint)_count)
            throw new ArgumentOutOfRangeException(nameof(index));
        ObjectDisposedException.ThrowIf(_nullBitmap is null, this);

        return _nullBitmap[index] != 0 ? null : _inner.GetBytesCopy(index);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_nullBitmap != null)
        {
            ArrayPool<byte>.Shared.Return(_nullBitmap);
            _nullBitmap = null;
        }

        _inner.Dispose();
    }
}
