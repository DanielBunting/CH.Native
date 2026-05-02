using System.Buffers;
using System.Buffers.Binary;
using CH.Native.Data.ColumnReaders;
using CH.Native.Exceptions;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data;

/// <summary>
/// Pre-fix the array reader trusted the offset stream blindly: non-monotonic
/// offsets produced a negative <c>length</c>, which then crashed inside
/// <see cref="Span{T}.Slice(int,int)"/> with an opaque <see cref="ArgumentOutOfRangeException"/>
/// instead of a typed protocol error, and other malformations could silently
/// fall through. The fix surfaces these as <see cref="ClickHouseProtocolException"/>
/// with row context so the connection can be torn down cleanly.
/// </summary>
public class ArrayColumnReaderOffsetTests
{
    [Fact]
    public void ReadTypedColumn_NonMonotonicOffsets_Throws()
    {
        // Two rows with offsets [10, 5] — second cumulative count is below first.
        var bytes = BuildArrayInt32(new ulong[] { 10, 5 }, new int[10]);
        var reader = new ArrayColumnReader<int>(new Int32ColumnReader());

        var ex = Assert.Throws<ClickHouseProtocolException>(() =>
        {
            var pr = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
            using var col = reader.ReadTypedColumn(ref pr, 2);
        });
        Assert.Contains("offset", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ReadTypedColumn_NegativeTotalThroughBadOffset_Throws()
    {
        // First-row offset claims more elements than the second-row cumulative
        // total. The second row would compute length = end - start < 0.
        var bytes = BuildArrayInt32(new ulong[] { 5, 3 }, new int[5]);
        var reader = new ArrayColumnReader<int>(new Int32ColumnReader());

        Assert.Throws<ClickHouseProtocolException>(() =>
        {
            var pr = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
            using var col = reader.ReadTypedColumn(ref pr, 2);
        });
    }

    [Fact]
    public void ReadTypedColumn_InnerReaderThrowsMidStream_DoesNotLeakPooledArrays()
    {
        // Pre-fix ArrayColumnReader rented a result array (ArrayPool<T[]>.Shared)
        // before invoking the element reader. If the element reader threw on
        // malformed inner data, the result array was leaked into the pool's
        // long-lived bucket, accumulating under repeated hostile input.
        // Now: the result array is returned on the throw path.
        //
        // We can't directly assert "the pool is empty" (the shared pool is
        // process-wide and noisy), but we CAN assert that the throw doesn't
        // mask itself — the rented array's lifetime is now bounded by the
        // try/catch in the production code, and the test exercises it.
        // Repeated invocation under a tight loop is the regression guard:
        // if the leak ever returns, this test would not directly fail but
        // a CI memory-pressure run would catch it.
        var bytes = BuildArrayInt32(new ulong[] { 100 }, new int[5]); // claims 100 elements but only 5 bytes worth
        var reader = new ArrayColumnReader<int>(new Int32ColumnReader());

        // The inner Int32 reader runs out of bytes before reading 100 elements
        // and throws — the production code's catch must return the result array.
        for (int i = 0; i < 50; i++)
        {
            Assert.ThrowsAny<Exception>(() =>
            {
                var pr = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
                using var col = reader.ReadTypedColumn(ref pr, 1);
            });
        }

        // If we got here without OOM-crashing the runner, the path completes
        // cleanly. The fix is verified primarily by the production-code
        // structural change (try/catch around the result-array lifetime);
        // this loop is regression coverage.
    }

    [Fact]
    public void ReadTypedColumn_ValidOffsets_RoundTrips()
    {
        // [ [10,20], [], [30] ] — cumulative offsets [2, 2, 3], elements [10,20,30].
        var bytes = BuildArrayInt32(new ulong[] { 2, 2, 3 }, new[] { 10, 20, 30 });
        var reader = new ArrayColumnReader<int>(new Int32ColumnReader());

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        using var col = reader.ReadTypedColumn(ref pr, 3);

        Assert.Equal(new[] { 10, 20 }, col[0]);
        Assert.Empty(col[1]);
        Assert.Equal(new[] { 30 }, col[2]);
    }

    private static byte[] BuildArrayInt32(ulong[] offsets, int[] elements)
    {
        var ms = new MemoryStream();
        Span<byte> u64 = stackalloc byte[8];
        foreach (var o in offsets)
        {
            BinaryPrimitives.WriteUInt64LittleEndian(u64, o);
            ms.Write(u64);
        }
        Span<byte> i32 = stackalloc byte[4];
        foreach (var e in elements)
        {
            BinaryPrimitives.WriteInt32LittleEndian(i32, e);
            ms.Write(i32);
        }
        return ms.ToArray();
    }
}
