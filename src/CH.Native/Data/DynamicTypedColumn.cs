using System.Buffers;
using CH.Native.Data.Dynamic;

namespace CH.Native.Data;

/// <summary>
/// Raw-storage column for <c>Dynamic</c> / <c>Dynamic(max_types=N)</c>. Holds the
/// block-local arm type list, wire discriminators, per-arm <see cref="ITypedColumn"/>s,
/// and the shared-arm payload. CLR-level <see cref="ClickHouseDynamic"/> values are
/// materialised on demand — the eager <c>ClickHouseDynamic[]</c> materialisation that
/// previously lived in <see cref="ColumnReaders.DynamicColumnReader"/> is gone.
/// </summary>
public sealed class DynamicTypedColumn : ITypedColumn
{
    private byte[]? _discriminators;
    private int[]? _rowToArmOffset;
    private readonly int _rowCount;
    private readonly ITypedColumn[] _armColumns;
    private readonly string[] _armTypeNames;
    private readonly string[]? _sharedArmTypeNames;
    private readonly object?[]? _sharedArmValues;
    private readonly ArrayPool<byte>? _discriminatorPool;
    private readonly ArrayPool<int>? _offsetPool;

    public DynamicTypedColumn(
        byte[] discriminators,
        int rowCount,
        ITypedColumn[] armColumns,
        string[] armTypeNames,
        string[]? sharedArmTypeNames,
        object?[]? sharedArmValues,
        int[] rowToArmOffset,
        ArrayPool<byte>? discriminatorPool = null,
        ArrayPool<int>? offsetPool = null)
    {
        _discriminators = discriminators;
        _rowCount = rowCount;
        _armColumns = armColumns;
        _armTypeNames = armTypeNames;
        _sharedArmTypeNames = sharedArmTypeNames;
        _sharedArmValues = sharedArmValues;
        _rowToArmOffset = rowToArmOffset;
        _discriminatorPool = discriminatorPool;
        _offsetPool = offsetPool;
    }

    /// <inheritdoc />
    public Type ElementType => typeof(ClickHouseDynamic);

    /// <inheritdoc />
    public int Count => _rowCount;

    /// <inheritdoc />
    public object? GetValue(int index)
    {
        if (_discriminators is null)
            throw new ObjectDisposedException(nameof(DynamicTypedColumn));
        if ((uint)index >= (uint)_rowCount)
            throw new ArgumentOutOfRangeException(nameof(index));

        var disc = _discriminators[index];
        if (disc == ClickHouseDynamic.NullDiscriminator)
            return ClickHouseDynamic.Null;

        var offset = _rowToArmOffset![index];
        var sharedArm = _armColumns.Length; // shared arm discriminator == numberOfTypes

        if (disc == sharedArm)
        {
            var typeName = _sharedArmTypeNames![offset];
            var value = _sharedArmValues![offset];
            return new ClickHouseDynamic(disc, value, typeName);
        }

        var armValue = _armColumns[disc].GetValue(offset);
        return new ClickHouseDynamic(disc, armValue, _armTypeNames[disc]);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_discriminators is null)
            return;

        _discriminatorPool?.Return(_discriminators);
        _discriminators = null;

        if (_rowToArmOffset is not null)
        {
            _offsetPool?.Return(_rowToArmOffset);
            _rowToArmOffset = null;
        }

        for (int i = 0; i < _armColumns.Length; i++)
            _armColumns[i].Dispose();
    }
}
