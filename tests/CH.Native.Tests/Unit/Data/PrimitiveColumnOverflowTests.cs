using System.Buffers;
using CH.Native.Data.ColumnReaders;
using CH.Native.Exceptions;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data;

/// <summary>
/// Pre-fix the bulk read path computed <c>byteCount = rowCount * sizeof(T)</c> in
/// unchecked arithmetic. With an adversarial / corrupted <c>rowCount</c> close to
/// <see cref="int.MaxValue"/>, the multiplication wrapped to a negative value and
/// the readers silently fell back to a per-value loop driven by the bogus count —
/// so a malformed reply could trigger a multi-gigabyte allocation attempt or, in
/// some CLR builds, a buffer-mis-sized fast path. The fix is to refuse the read
/// up front when <c>rowCount</c> is too large to fit the byte span.
/// </summary>
public class PrimitiveColumnOverflowTests
{
    [Fact]
    public void Int32Reader_RowCountOverflowingByteSpan_Throws()
    {
        var reader = new Int32ColumnReader();
        var ex = Assert.Throws<ClickHouseProtocolException>(() =>
        {
            var pr = new ProtocolReader(new ReadOnlySequence<byte>(new byte[16]));
            using var col = reader.ReadTypedColumn(ref pr, (int.MaxValue / sizeof(int)) + 1);
        });
        Assert.Contains("rowCount", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Int32Reader_ValidSmallRead_StillWorks()
    {
        var reader = new Int32ColumnReader();
        var bytes = new byte[8];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(0, 4), 42);
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(4, 4), -7);
        var pr = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        using var col = reader.ReadTypedColumn(ref pr, 2);
        Assert.Equal(42, col[0]);
        Assert.Equal(-7, col[1]);
    }

    [Fact]
    public void Int64Reader_RowCountOverflowingByteSpan_Throws()
    {
        var reader = new Int64ColumnReader();
        Assert.Throws<ClickHouseProtocolException>(() =>
        {
            var pr = new ProtocolReader(new ReadOnlySequence<byte>(new byte[16]));
            using var col = reader.ReadTypedColumn(ref pr, (int.MaxValue / sizeof(long)) + 1);
        });
    }

    [Fact]
    public void Float64Reader_RowCountOverflowingByteSpan_Throws()
    {
        var reader = new Float64ColumnReader();
        Assert.Throws<ClickHouseProtocolException>(() =>
        {
            var pr = new ProtocolReader(new ReadOnlySequence<byte>(new byte[16]));
            using var col = reader.ReadTypedColumn(ref pr, (int.MaxValue / sizeof(double)) + 1);
        });
    }

    [Fact]
    public void Int32Reader_NegativeRowCount_Throws()
    {
        var reader = new Int32ColumnReader();
        Assert.Throws<ClickHouseProtocolException>(() =>
        {
            var pr = new ProtocolReader(new ReadOnlySequence<byte>(new byte[16]));
            using var col = reader.ReadTypedColumn(ref pr, -1);
        });
    }
}
