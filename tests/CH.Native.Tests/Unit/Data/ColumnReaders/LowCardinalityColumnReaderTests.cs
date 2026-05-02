using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

/// <summary>
/// LowCardinality wire format:
///   prefix:    UInt64 KeysSerializationVersion (== 1)
///   per-block: UInt64 flags (low byte = index width 0/1/2/3 for U8/U16/U32/U64)
///              UInt64 dictSize
///              dictSize × inner values
///              UInt64 indexCount
///              indexCount × index values (width per flags)
/// For LowCardinality(Nullable(T)), index 0 is the null sentinel.
/// </summary>
public class LowCardinalityColumnReaderTests
{
    [Fact]
    public void TypeName_NamespacesInnerType()
    {
        Assert.Equal("LowCardinality(String)",
            new LowCardinalityColumnReader<string>(new StringColumnReader()).TypeName);
    }

    [Fact]
    public void TypeName_WithNullableFlag_AddsNullableWrapper()
    {
        Assert.Equal("LowCardinality(Nullable(String))",
            new LowCardinalityColumnReader<string>(new StringColumnReader(), isNullable: true).TypeName);
    }

    [Fact]
    public void ReadPrefix_RejectsUnknownVersion()
    {
        var bytes = new byte[8];
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(bytes, 99);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        var sut = new LowCardinalityColumnReader<string>(new StringColumnReader());
        InvalidDataException? caught = null;
        try { sut.ReadPrefix(ref reader); }
        catch (InvalidDataException ex) { caught = ex; }
        Assert.NotNull(caught);
    }

    private static byte[] BuildLcColumn(string[] dictionary, int[] indices, int indexWidth)
    {
        // indexWidth: 0=u8, 1=u16, 2=u32, 3=u64
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        // Per-block header: flags (only low byte matters for index width)
        writer.WriteUInt64((ulong)indexWidth);
        writer.WriteUInt64((ulong)dictionary.Length);
        foreach (var s in dictionary) writer.WriteString(s);
        writer.WriteUInt64((ulong)indices.Length);
        foreach (var idx in indices)
        {
            switch (indexWidth)
            {
                case 0: writer.WriteByte((byte)idx); break;
                case 1: writer.WriteUInt16((ushort)idx); break;
                case 2: writer.WriteUInt32((uint)idx); break;
                case 3: writer.WriteUInt64((ulong)idx); break;
            }
        }
        return buffer.WrittenSpan.ToArray();
    }

    [Theory]
    [InlineData(0)]  // UInt8 indices
    [InlineData(1)]  // UInt16
    [InlineData(2)]  // UInt32
    [InlineData(3)]  // UInt64
    public void ReadTypedColumn_DecodesAllIndexWidths(int indexWidth)
    {
        var dict = new[] { "alpha", "beta", "gamma" };
        var indices = new[] { 0, 2, 1, 0, 1 };
        var bytes = BuildLcColumn(dict, indices, indexWidth);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        var sut = new LowCardinalityColumnReader<string>(new StringColumnReader());
        // Note: ReadPrefix is NOT called here — these per-block tests start
        // from the flags word. Use ReadTypedColumn directly.
        using var column = sut.ReadTypedColumn(ref reader, indices.Length);

        for (int i = 0; i < indices.Length; i++)
            Assert.Equal(dict[indices[i]], column[i]);
    }

    [Fact]
    public void ReadTypedColumn_NullableAtIndexZero_ReturnsNull()
    {
        // For LowCardinality(Nullable(T)), index 0 is the null sentinel.
        // Dictionary slot 0 is a placeholder the reader must NOT surface.
        var dict = new[] { "<placeholder>", "yes", "no" };
        var indices = new[] { 0, 1, 0, 2 };
        var bytes = BuildLcColumn(dict, indices, indexWidth: 0);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        var sut = new LowCardinalityColumnReader<string>(new StringColumnReader(), isNullable: true);
        using var column = sut.ReadTypedColumn(ref reader, indices.Length);

        Assert.Null(column[0]);
        Assert.Equal("yes", column[1]);
        Assert.Null(column[2]);
        Assert.Equal("no", column[3]);
    }

    [Fact]
    public void ReadTypedColumn_OutOfRangeIndex_Throws()
    {
        var dict = new[] { "a", "b" };
        var indices = new[] { 0, 5, 1 };  // 5 is out of range
        var bytes = BuildLcColumn(dict, indices, indexWidth: 0);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        var sut = new LowCardinalityColumnReader<string>(new StringColumnReader());

        InvalidDataException? caught = null;
        try { _ = sut.ReadTypedColumn(ref reader, indices.Length); }
        catch (InvalidDataException ex) { caught = ex; }
        Assert.NotNull(caught);
    }

    [Fact]
    public void ReadTypedColumn_ZeroRows_ReturnsEmptyWithoutReadingBytes()
    {
        var sut = new LowCardinalityColumnReader<string>(new StringColumnReader());
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(Array.Empty<byte>()));

        using var column = sut.ReadTypedColumn(ref reader, 0);

        Assert.Equal(0, column.Count);
    }

    [Fact]
    public void Constructor_NonGeneric_RejectsMismatchedInner()
    {
        Assert.Throws<ArgumentException>(() =>
            new LowCardinalityColumnReader<string>((IColumnReader)new Int32ColumnReader()));
    }
}
