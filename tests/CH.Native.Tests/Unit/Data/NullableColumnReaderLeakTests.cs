using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data;

/// <summary>
/// Pre-fix the result array was rented from the pool BEFORE the inner-values
/// projection loop, but if that loop threw — e.g. when the inner reader returns
/// a column shorter than the declared row count — the rented array was never
/// returned to the pool. Each malformed read leaked one rental's worth of
/// memory, drifting the pool's ledger over time.
/// </summary>
public class NullableColumnReaderLeakTests
{
    [Fact]
    public void ThrowingInnerProjection_ReturnsRentedResultBuffer()
    {
        var pool = new CountingPool<int?>();
        var inner = new ShortInnerReader(); // returns Count=1 regardless of asked rowCount
        var reader = new NullableColumnReader<int>(inner, pool);

        // Buffer: 5 zero bytes (no-null bitmap) plus 4 bytes for the inner Int32 value.
        var bytes = new byte[5 + 4];

        Assert.ThrowsAny<Exception>(() =>
        {
            var pr = new ProtocolReader(new System.Buffers.ReadOnlySequence<byte>(bytes));
            using var col = reader.ReadTypedColumn(ref pr, 5);
        });

        Assert.Equal(pool.RentCount, pool.ReturnCount);
        Assert.True(pool.RentCount > 0, "expected at least one rental");
    }

    [Fact]
    public void HappyPath_DisposingTypedColumn_BalancesPool()
    {
        var pool = new CountingPool<int?>();
        var inner = new ConstantInnerReader();
        var reader = new NullableColumnReader<int>(inner, pool);

        var bytes = new byte[3 + 12]; // 3 bitmap bytes (0,1,0) + 3 ints
        bytes[1] = 1; // mark row 1 null
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(3, 4), 11);
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(7, 4), 22);
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(11, 4), 33);

        var pr = new ProtocolReader(new System.Buffers.ReadOnlySequence<byte>(bytes));
        using (var col = reader.ReadTypedColumn(ref pr, 3))
        {
            Assert.Equal(11, col[0]);
            Assert.Null(col[1]);
            Assert.Equal(33, col[2]);
        }

        Assert.Equal(pool.RentCount, pool.ReturnCount);
    }

    private sealed class CountingPool<T> : ArrayPool<T>
    {
        public int RentCount;
        public int ReturnCount;

        public override T[] Rent(int minimumLength)
        {
            RentCount++;
            return new T[minimumLength];
        }

        public override void Return(T[] array, bool clearArray = false)
        {
            ReturnCount++;
        }
    }

    private sealed class ShortInnerReader : IColumnReader<int>
    {
        public string TypeName => "Int32";
        public Type ClrType => typeof(int);
        public void ReadPrefix(ref ProtocolReader reader) { }
        public int ReadValue(ref ProtocolReader reader) => reader.ReadInt32();

        public TypedColumn<int> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
        {
            // Drain a single int from the wire so the test buffer is well-formed
            // up to the point we deliberately under-supply the column.
            _ = reader.ReadInt32();
            return new TypedColumn<int>(new[] { 0 }, length: 1);
        }

        ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
            => ReadTypedColumn(ref reader, rowCount);
    }

    private sealed class ConstantInnerReader : IColumnReader<int>
    {
        public string TypeName => "Int32";
        public Type ClrType => typeof(int);
        public void ReadPrefix(ref ProtocolReader reader) { }
        public int ReadValue(ref ProtocolReader reader) => reader.ReadInt32();

        public TypedColumn<int> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
        {
            var arr = new int[rowCount];
            for (int i = 0; i < rowCount; i++)
                arr[i] = reader.ReadInt32();
            return new TypedColumn<int>(arr);
        }

        ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
            => ReadTypedColumn(ref reader, rowCount);
    }
}
