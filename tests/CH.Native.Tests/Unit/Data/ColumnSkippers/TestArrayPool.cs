using System.Buffers;

namespace CH.Native.Tests.Unit.Data.ColumnSkippers;

/// <summary>
/// ArrayPool decorator that counts rent/return pairs so tests can detect leaks on
/// failure paths. Wraps <see cref="ArrayPool{T}.Shared"/>.
/// </summary>
internal sealed class TestArrayPool<T> : ArrayPool<T>
{
    public int Rents { get; private set; }
    public int Returns { get; private set; }

    public override T[] Rent(int minimumLength)
    {
        Rents++;
        return ArrayPool<T>.Shared.Rent(minimumLength);
    }

    public override void Return(T[] array, bool clearArray = false)
    {
        Returns++;
        ArrayPool<T>.Shared.Return(array, clearArray);
    }
}
