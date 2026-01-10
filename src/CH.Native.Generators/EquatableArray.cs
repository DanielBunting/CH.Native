using System;
using System.Collections;
using System.Collections.Generic;

namespace CH.Native.Generators;

/// <summary>
/// A wrapper around an array that provides value equality semantics.
/// This is essential for incremental generator caching.
/// </summary>
/// <typeparam name="T">The element type.</typeparam>
internal readonly struct EquatableArray<T> : IEquatable<EquatableArray<T>>, IEnumerable<T>
    where T : IEquatable<T>
{
    private readonly T[]? _array;

    public EquatableArray(T[]? array)
    {
        _array = array;
    }

    public int Length => _array?.Length ?? 0;

    public T this[int index] => _array![index];

    public bool Equals(EquatableArray<T> other)
    {
        if (_array is null && other._array is null)
            return true;
        if (_array is null || other._array is null)
            return false;
        if (_array.Length != other._array.Length)
            return false;

        for (int i = 0; i < _array.Length; i++)
        {
            if (!_array[i].Equals(other._array[i]))
                return false;
        }

        return true;
    }

    public override bool Equals(object? obj)
    {
        return obj is EquatableArray<T> other && Equals(other);
    }

    public override int GetHashCode()
    {
        if (_array is null)
            return 0;

        unchecked
        {
            int hash = 17;
            foreach (var item in _array)
            {
                hash = hash * 31 + item?.GetHashCode() ?? 0;
            }
            return hash;
        }
    }

    public IEnumerator<T> GetEnumerator()
    {
        return ((IEnumerable<T>)(_array ?? Array.Empty<T>())).GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public static implicit operator EquatableArray<T>(T[]? array) => new(array);
}
