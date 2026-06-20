using System.Buffers;
using System.Text;
using CH.Native.Data;
using Xunit;

namespace CH.Native.Tests.Unit.Data;

// GetRawBytes/GetBytesCopy exist so callers can recover String values whose bytes are
// not valid UTF-8 — GetValue() decodes with U+FFFD replacement and loses them.
public class RawStringColumnByteAccessTests
{
    private static RawStringColumn Create(params byte[][] values)
    {
        var total = values.Sum(v => v.Length);
        var rawData = ArrayPool<byte>.Shared.Rent(Math.Max(total, 1));
        var offsets = ArrayPool<int>.Shared.Rent(values.Length);
        var lengths = ArrayPool<int>.Shared.Rent(values.Length);

        var position = 0;
        for (int i = 0; i < values.Length; i++)
        {
            offsets[i] = position;
            lengths[i] = values[i].Length;
            values[i].CopyTo(rawData.AsSpan(position));
            position += values[i].Length;
        }

        return new RawStringColumn(rawData, offsets, lengths, values.Length);
    }

    [Fact]
    public void GetRawBytes_InvalidUtf8_PreservesOriginalBytes()
    {
        byte[] invalid = [0xFF, 0x61];
        using var column = Create(invalid);

        Assert.Equal(invalid, column.GetRawBytes(0).ToArray());
        Assert.Equal(invalid, column.GetBytesCopy(0));

        // The string accessor still applies U+FFFD replacement — pinned default behavior.
        Assert.Equal("�a", column.GetValue(0));
    }

    [Fact]
    public void GetRawBytes_ValidUtf8_MatchesEncodedString()
    {
        var bytes = Encoding.UTF8.GetBytes("тест 🦀");
        using var column = Create(bytes);

        Assert.Equal(bytes, column.GetRawBytes(0).ToArray());
        Assert.Equal("тест 🦀", column.GetValue(0));
    }

    [Fact]
    public void GetRawBytes_EmptyString_ReturnsEmpty()
    {
        using var column = Create(new byte[][] { [] });

        Assert.True(column.GetRawBytes(0).IsEmpty);
        Assert.Empty(column.GetBytesCopy(0));
    }

    [Fact]
    public void GetRawBytes_EmbeddedNul_Preserved()
    {
        byte[] withNul = [(byte)'a', 0x00, (byte)'b'];
        using var column = Create(withNul);

        Assert.Equal(withNul, column.GetBytesCopy(0));
    }

    [Fact]
    public void GetRawBytes_MultipleValues_CorrectSlices()
    {
        using var column = Create("abc"u8.ToArray(), [0xFF], "de"u8.ToArray());

        Assert.Equal("abc"u8.ToArray(), column.GetBytesCopy(0));
        Assert.Equal(new byte[] { 0xFF }, column.GetBytesCopy(1));
        Assert.Equal("de"u8.ToArray(), column.GetBytesCopy(2));
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(1)]
    public void GetRawBytes_IndexOutOfRange_Throws(int index)
    {
        using var column = Create("a"u8.ToArray());

        Assert.Throws<ArgumentOutOfRangeException>(() => column.GetRawBytes(index));
        Assert.Throws<ArgumentOutOfRangeException>(() => column.GetBytesCopy(index));
    }

    [Fact]
    public void GetRawBytes_AfterDispose_ThrowsObjectDisposed()
    {
        var column = Create("a"u8.ToArray());
        column.Dispose();

        Assert.Throws<ObjectDisposedException>(() => column.GetRawBytes(0));
        Assert.Throws<ObjectDisposedException>(() => column.GetBytesCopy(0));
    }

    [Fact]
    public void GetBytesCopy_SurvivesDispose()
    {
        var column = Create([0xFF, 0x61]);
        var copy = column.GetBytesCopy(0);
        column.Dispose();

        Assert.Equal(new byte[] { 0xFF, 0x61 }, copy);
    }

    [Fact]
    public void NullableColumn_GetBytesCopy_NullRowReturnsNull_DataRowReturnsBytes()
    {
        var inner = Create([0xFF, 0x61], []);
        var bitmap = ArrayPool<byte>.Shared.Rent(2);
        bitmap[0] = 0; // not null
        bitmap[1] = 1; // null
        using var column = new NullableRawStringColumn(bitmap, inner, 2);

        Assert.Equal(new byte[] { 0xFF, 0x61 }, column.GetBytesCopy(0));
        Assert.Null(column.GetBytesCopy(1));
        Assert.Throws<ArgumentOutOfRangeException>(() => column.GetBytesCopy(2));
    }

    [Fact]
    public void NullableColumn_GetBytesCopy_AfterDispose_Throws()
    {
        var inner = Create("a"u8.ToArray());
        var bitmap = ArrayPool<byte>.Shared.Rent(1);
        bitmap[0] = 0;
        var column = new NullableRawStringColumn(bitmap, inner, 1);
        column.Dispose();

        Assert.Throws<ObjectDisposedException>(() => column.GetBytesCopy(0));
    }
}
