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
