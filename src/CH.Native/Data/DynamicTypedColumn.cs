using System.Buffers;
using CH.Native.Data.Dynamic;

namespace CH.Native.Data;

/// <summary>
/// Raw-storage column for FLATTENED <c>Dynamic</c> / <c>Dynamic(max_types=N)</c>. Holds the
/// block-local type list, per-row indexes, and per-type <see cref="ITypedColumn"/>s.
/// CLR-level <see cref="ClickHouseDynamic"/> values are materialised on demand.
/// </summary>
/// <remarks>
/// Index values <c>0..armColumns.Length-1</c> reference a typed arm; index
/// <c>armColumns.Length</c> marks NULL. Arm columns are sized per-arm (not per-row), and
/// <c>rowToArmOffset</c> maps each row back into its arm column.
/// </remarks>
public sealed class DynamicTypedColumn : ITypedColumn
{
    private int[]? _indexes;
    private int[]? _rowToArmOffset;
    private readonly int _rowCount;
    private readonly ITypedColumn[] _armColumns;
    private readonly string[] _armTypeNames;
    private readonly ArrayPool<int>? _indexPool;
    private readonly ArrayPool<int>? _offsetPool;

    public DynamicTypedColumn(
        int[] indexes,
        int rowCount,
        ITypedColumn[] armColumns,
        string[] armTypeNames,
        int[] rowToArmOffset,
        ArrayPool<int>? indexPool = null,
        ArrayPool<int>? offsetPool = null)
    {
        _indexes = indexes;
        _rowCount = rowCount;
        _armColumns = armColumns;
        _armTypeNames = armTypeNames;
        _rowToArmOffset = rowToArmOffset;
        _indexPool = indexPool;
        _offsetPool = offsetPool;
    }

    /// <inheritdoc />
    public Type ElementType => typeof(ClickHouseDynamic);

    /// <inheritdoc />
    public int Count => _rowCount;

    /// <inheritdoc />
    public object? GetValue(int index)
    {
        if (_indexes is null)
            throw new ObjectDisposedException(nameof(DynamicTypedColumn));
        if ((uint)index >= (uint)_rowCount)
            throw new ArgumentOutOfRangeException(nameof(index));

        var typeIdx = _indexes[index];
        if (typeIdx == _armColumns.Length)
            return ClickHouseDynamic.Null;

        var offset = _rowToArmOffset![index];
        var armValue = _armColumns[typeIdx].GetValue(offset);
        return new ClickHouseDynamic((byte)typeIdx, armValue, _armTypeNames[typeIdx]);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_indexes is null)
            return;

        _indexPool?.Return(_indexes);
        _indexes = null;

        if (_rowToArmOffset is not null)
        {
            _offsetPool?.Return(_rowToArmOffset);
            _rowToArmOffset = null;
        }

        for (int i = 0; i < _armColumns.Length; i++)
            _armColumns[i].Dispose();
    }
}
