using System.Buffers;
using CH.Native.Data;
using Xunit;

namespace CH.Native.Tests.Unit.Data;

/// <summary>
/// TypedColumn returns its rented backing array on Dispose. For reference-type or
/// reference-containing T (string, IPAddress, byte[], structs holding refs) the
/// returned array must be cleared so the previous renter's references don't pin
/// objects in memory until the next renter overwrites the slot. Pre-fix
/// <c>_pool.Return(_values)</c> never cleared, leaving references live in the
/// pool's freelist for an unbounded duration.
/// </summary>
public class TypedColumnDisposeClearsArrayTests
{
    [Fact]
    public void Dispose_ReferenceTypeT_ReturnsArrayWithClear()
    {
        var pool = new RecordingPool<string>();
        var col = new TypedColumn<string>(pool.Rent(8), length: 4, pool: pool);

        col.Dispose();

        Assert.Equal(1, pool.ReturnCount);
        Assert.True(pool.LastClearArray, "reference-type pool returns must clear array references");
    }

    [Fact]
    public void Dispose_ValueTypeT_DoesNotPayClearCost()
    {
        var pool = new RecordingPool<int>();
        var col = new TypedColumn<int>(pool.Rent(8), length: 4, pool: pool);

        col.Dispose();

        Assert.Equal(1, pool.ReturnCount);
        Assert.False(pool.LastClearArray, "value-type pool returns should skip Array.Clear cost");
    }

    [Fact]
    public void Dispose_StructWithReferenceField_ReturnsArrayWithClear()
    {
        var pool = new RecordingPool<HoldsReference>();
        var col = new TypedColumn<HoldsReference>(pool.Rent(8), length: 4, pool: pool);

        col.Dispose();

        Assert.Equal(1, pool.ReturnCount);
        Assert.True(pool.LastClearArray,
            "structs containing references must clear so the held reference is released");
    }

    private struct HoldsReference
    {
        public string? Value;
    }

    private sealed class RecordingPool<T> : ArrayPool<T>
    {
        public int ReturnCount;
        public bool LastClearArray;

        public override T[] Rent(int minimumLength) => new T[minimumLength];

        public override void Return(T[] array, bool clearArray = false)
        {
            ReturnCount++;
            LastClearArray = clearArray;
        }
    }
}
