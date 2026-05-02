using System.Buffers;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.Variant;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

/// <summary>
/// Boxing-free 2-arm Variant. Wire format:
///   prefix:  UInt64 discriminator-version (== 0)
///   block:   N discriminator bytes (0, 1, or 255 for null), then arm-0
///            values packed (one per discriminator==0), then arm-1 values.
/// </summary>
public class VariantColumnReaderGenericTests
{
    [Fact]
    public void TypeName_NamespacesArms()
    {
        var sut = new VariantColumnReader<int, string>(new Int32ColumnReader(), new StringColumnReader());
        Assert.Equal("Variant(Int32, String)", sut.TypeName);
    }

    [Fact]
    public void ClrType_IsVariantValue()
    {
        var sut = new VariantColumnReader<int, string>(new Int32ColumnReader(), new StringColumnReader());
        Assert.Equal(typeof(VariantValue<int, string>), sut.ClrType);
    }

    [Fact]
    public void ReadValue_AlwaysThrows_NotSupported()
    {
        var bytes = new byte[1];
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        var sut = new VariantColumnReader<int, string>(new Int32ColumnReader(), new StringColumnReader());
        NotSupportedException? caught = null;
        try { _ = sut.ReadValue(ref reader); }
        catch (NotSupportedException ex) { caught = ex; }
        Assert.NotNull(caught);
    }

    [Fact]
    public void ReadPrefix_RejectsUnknownVersion()
    {
        var bytes = new byte[8];
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(bytes, 99);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        var sut = new VariantColumnReader<int, string>(new Int32ColumnReader(), new StringColumnReader());
        NotSupportedException? caught = null;
        try { sut.ReadPrefix(ref reader); }
        catch (NotSupportedException ex) { caught = ex; }
        Assert.NotNull(caught);
    }

    private static byte[] BuildBlock(byte[] discriminators, int[] arm0Values, string[] arm1Values)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        foreach (var d in discriminators) writer.WriteByte(d);
        foreach (var v in arm0Values) writer.WriteInt32(v);
        foreach (var v in arm1Values) writer.WriteString(v);
        return buffer.WrittenSpan.ToArray();
    }

    [Fact]
    public void ReadTypedColumn_ZeroRows_ReturnsEmpty()
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(Array.Empty<byte>()));
        var sut = new VariantColumnReader<int, string>(new Int32ColumnReader(), new StringColumnReader());

        using var column = sut.ReadTypedColumn(ref reader, 0);
        Assert.Equal(0, column.Count);
    }

    [Fact]
    public void ReadTypedColumn_MixedDiscriminators_RoutesValuesToArms()
    {
        // 5 rows: arm0, arm1, null, arm0, arm1
        var bytes = BuildBlock(
            new byte[] { 0, 1, VariantValue<int, string>.NullDiscriminator, 0, 1 },
            new[] { 42, 99 },
            new[] { "first", "second" });

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        var sut = new VariantColumnReader<int, string>(new Int32ColumnReader(), new StringColumnReader());
        using var column = sut.ReadTypedColumn(ref reader, 5);

        Assert.Equal(42, column[0].Arm0);
        Assert.Equal("first", column[1].Arm1);
        Assert.True(column[2].IsNull);
        Assert.Equal(99, column[3].Arm0);
        Assert.Equal("second", column[4].Arm1);
    }

    [Fact]
    public void ReadTypedColumn_AllNull_AllSlotsAreNull()
    {
        var bytes = BuildBlock(
            new byte[]
            {
                VariantValue<int, string>.NullDiscriminator,
                VariantValue<int, string>.NullDiscriminator,
                VariantValue<int, string>.NullDiscriminator,
            },
            Array.Empty<int>(),
            Array.Empty<string>());

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        var sut = new VariantColumnReader<int, string>(new Int32ColumnReader(), new StringColumnReader());
        using var column = sut.ReadTypedColumn(ref reader, 3);

        Assert.True(column[0].IsNull);
        Assert.True(column[1].IsNull);
        Assert.True(column[2].IsNull);
    }

    [Fact]
    public void ReadTypedColumn_OutOfRangeDiscriminator_Throws()
    {
        var bytes = BuildBlock(
            new byte[] { 0, 7 },  // 7 is invalid for a 2-arm Variant
            new[] { 1 },
            Array.Empty<string>());

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        var sut = new VariantColumnReader<int, string>(new Int32ColumnReader(), new StringColumnReader());

        InvalidOperationException? caught = null;
        try { _ = sut.ReadTypedColumn(ref reader, 2); }
        catch (InvalidOperationException ex) { caught = ex; }
        Assert.NotNull(caught);
    }
}
