using System.Buffers;
using System.Text;

namespace CH.Native.Data;

/// <summary>
/// Lazy string column that stores raw UTF-8 bytes and only materializes
/// System.String objects on demand via GetValue(). This reduces memory usage
/// when not all string values are accessed (e.g., streaming with ClickHouseDataReader).
/// </summary>
public sealed class RawStringColumn : ITypedColumn
{
    private byte[]? _rawData;
    private int[]? _offsets;
    private int[]? _lengths;
    private readonly int _count;

    /// <summary>
    /// Creates a new RawStringColumn from pooled arrays.
    /// </summary>
    /// <param name="rawData">Pooled byte array containing all string bytes contiguously.</param>
    /// <param name="offsets">Pooled int array of byte offsets per string.</param>
    /// <param name="lengths">Pooled int array of byte lengths per string.</param>
    /// <param name="count">The number of strings in this column.</param>
    internal RawStringColumn(byte[] rawData, int[] offsets, int[] lengths, int count)
    {
        _rawData = rawData;
        _offsets = offsets;
        _lengths = lengths;
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

        var length = _lengths![index];
        if (length == 0)
            return string.Empty;

        return Encoding.UTF8.GetString(_rawData!, _offsets![index], length);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_rawData != null)
        {
            ArrayPool<byte>.Shared.Return(_rawData);
            _rawData = null;
        }

        if (_offsets != null)
        {
            ArrayPool<int>.Shared.Return(_offsets);
            _offsets = null;
        }

        if (_lengths != null)
        {
            ArrayPool<int>.Shared.Return(_lengths);
            _lengths = null;
        }
    }
}
