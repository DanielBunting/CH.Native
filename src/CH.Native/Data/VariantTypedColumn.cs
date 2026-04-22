using System.Buffers;
using CH.Native.Data.Variant;

namespace CH.Native.Data;

/// <summary>
/// Raw-storage column for <c>Variant(T1, …, Tn)</c>. Holds the wire-level discriminator
/// array plus one <see cref="ITypedColumn"/> per arm, materialising CLR-level values on
/// demand.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="GetValue(int)"/> returns the boxed <see cref="ClickHouseVariant"/> form for
/// compatibility with the existing untyped <c>GetValue</c> path. Hot paths should call
/// <see cref="GetTyped{T0,T1}"/> or access <see cref="GetArm{T}"/> directly to skip the
/// box.
/// </para>
/// </remarks>
public sealed class VariantTypedColumn : ITypedColumn
{
    private byte[]? _discriminators;
    private int[]? _rowToArmOffset;
    private readonly int _rowCount;
    private readonly ITypedColumn[] _armColumns;
    private readonly ArrayPool<byte>? _discriminatorPool;
    private readonly ArrayPool<int>? _offsetPool;

    public VariantTypedColumn(
        byte[] discriminators,
        int rowCount,
        ITypedColumn[] armColumns,
        int[] rowToArmOffset,
        ArrayPool<byte>? discriminatorPool = null,
        ArrayPool<int>? offsetPool = null)
    {
        _discriminators = discriminators;
        _rowCount = rowCount;
        _armColumns = armColumns;
        _rowToArmOffset = rowToArmOffset;
        _discriminatorPool = discriminatorPool;
        _offsetPool = offsetPool;
    }

    /// <inheritdoc />
    public Type ElementType => typeof(ClickHouseVariant);

    /// <inheritdoc />
    public int Count => _rowCount;

    /// <summary>
    /// Number of arms in the Variant (excluding NULL).
    /// </summary>
    public int ArmCount => _armColumns.Length;

    /// <summary>
    /// Discriminator byte for the given row. <see cref="ClickHouseVariant.NullDiscriminator"/>
    /// for NULL.
    /// </summary>
    public byte GetDiscriminator(int row)
    {
        EnsureLive();
        return _discriminators![row];
    }

    /// <summary>
    /// The arm <see cref="ITypedColumn"/> for the given arm index.
    /// </summary>
    public ITypedColumn GetArmColumn(int armIndex) => _armColumns[armIndex];

    /// <summary>
    /// The strongly-typed arm column. Throws if the declared element type does not match.
    /// </summary>
    public TypedColumn<T> GetArm<T>(int armIndex)
    {
        if (_armColumns[armIndex] is not TypedColumn<T> typed)
            throw new InvalidCastException(
                $"Variant arm {armIndex} has element type {_armColumns[armIndex].ElementType}, not {typeof(T)}.");
        return typed;
    }

    /// <inheritdoc />
    public object? GetValue(int index)
    {
        EnsureLive();
        if ((uint)index >= (uint)_rowCount)
            throw new ArgumentOutOfRangeException(nameof(index));

        var disc = _discriminators![index];
        if (disc == ClickHouseVariant.NullDiscriminator)
            return ClickHouseVariant.Null;

        var armValue = _armColumns[disc].GetValue(_rowToArmOffset![index]);
        return new ClickHouseVariant(disc, armValue);
    }

    /// <summary>
    /// Reads a row as <see cref="VariantValue{T0,T1}"/> without boxing. Arm element types
    /// must match <typeparamref name="T0"/> and <typeparamref name="T1"/>.
    /// </summary>
    public VariantValue<T0, T1> GetTyped<T0, T1>(int row)
    {
        EnsureLive();
        if ((uint)row >= (uint)_rowCount)
            throw new ArgumentOutOfRangeException(nameof(row));
        if (_armColumns.Length != 2)
            throw new InvalidOperationException(
                $"GetTyped<T0,T1> requires a 2-arm Variant; this column has {_armColumns.Length} arms.");

        var disc = _discriminators![row];
        if (disc == ClickHouseVariant.NullDiscriminator)
            return VariantValue<T0, T1>.Null;

        var armOffset = _rowToArmOffset![row];
        if (disc == 0)
        {
            if (_armColumns[0] is not TypedColumn<T0> arm0)
                throw new InvalidCastException(
                    $"Variant arm 0 has element type {_armColumns[0].ElementType}, not {typeof(T0)}.");
            return VariantValue<T0, T1>.FromArm0(arm0[armOffset]);
        }
        if (disc == 1)
        {
            if (_armColumns[1] is not TypedColumn<T1> arm1)
                throw new InvalidCastException(
                    $"Variant arm 1 has element type {_armColumns[1].ElementType}, not {typeof(T1)}.");
            return VariantValue<T0, T1>.FromArm1(arm1[armOffset]);
        }
        throw new InvalidOperationException($"Discriminator {disc} out of range for 2-arm Variant.");
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

    private void EnsureLive()
    {
        if (_discriminators is null)
            throw new ObjectDisposedException(nameof(VariantTypedColumn));
    }
}
