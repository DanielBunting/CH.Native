using System.Buffers;
using System.Text;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data;

/// <summary>
/// Unit tests for lazy string materialization: RawStringColumn, NullableRawStringColumn,
/// StringColumnReader lazy mode, and ColumnReaderRegistry.LazyStrings.
/// </summary>
public class LazyStringColumnTests
{
    #region Helpers

    private static int GetVarIntLength(int value)
    {
        int length = 0;
        do { length++; value >>= 7; } while (value > 0);
        return length;
    }

    private static int WriteVarInt(Span<byte> span, int value)
    {
        int written = 0;
        do
        {
            byte b = (byte)(value & 0x7F);
            value >>= 7;
            if (value > 0) b |= 0x80;
            span[written++] = b;
        } while (value > 0);
        return written;
    }

    /// <summary>
    /// Builds protocol bytes for a String column: [VarInt length + UTF-8 bytes] per row.
    /// </summary>
    private static byte[] BuildStringColumnBytes(params string[] values)
    {
        var totalSize = 0;
        foreach (var v in values)
        {
            var byteLen = Encoding.UTF8.GetByteCount(v);
            totalSize += GetVarIntLength(byteLen) + byteLen;
        }

        var result = new byte[totalSize];
        var offset = 0;
        foreach (var v in values)
        {
            var utf8 = Encoding.UTF8.GetBytes(v);
            offset += WriteVarInt(result.AsSpan(offset), utf8.Length);
            utf8.CopyTo(result.AsSpan(offset));
            offset += utf8.Length;
        }

        return result;
    }

    /// <summary>
    /// Builds protocol bytes for a Nullable(String) column:
    /// [null bitmap: 1 byte per row] + [VarInt length + UTF-8 bytes per row].
    /// </summary>
    private static byte[] BuildNullableStringColumnBytes(params string?[] values)
    {
        // Null bitmap
        var bitmapSize = values.Length;

        // String data (including placeholders for null rows)
        var stringDataSize = 0;
        foreach (var v in values)
        {
            var str = v ?? "";
            var byteLen = Encoding.UTF8.GetByteCount(str);
            stringDataSize += GetVarIntLength(byteLen) + byteLen;
        }

        var result = new byte[bitmapSize + stringDataSize];
        var offset = 0;

        // Write null bitmap
        foreach (var v in values)
        {
            result[offset++] = (byte)(v == null ? 1 : 0);
        }

        // Write string data
        foreach (var v in values)
        {
            var str = v ?? "";
            var utf8 = Encoding.UTF8.GetBytes(str);
            offset += WriteVarInt(result.AsSpan(offset), utf8.Length);
            utf8.CopyTo(result.AsSpan(offset));
            offset += utf8.Length;
        }

        return result;
    }

    #endregion

    #region RawStringColumn Tests

    [Fact]
    public void RawStringColumn_GetValue_ReturnsCorrectStrings()
    {
        var reader = new StringColumnReader(lazy: true);
        var bytes = BuildStringColumnBytes("hello", "world", "test");
        var protocolReader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        using var column = reader.ReadRawColumn(ref protocolReader, 3);

        Assert.Equal(3, column.Count);
        Assert.Equal(typeof(string), column.ElementType);
        Assert.Equal("hello", column.GetValue(0));
        Assert.Equal("world", column.GetValue(1));
        Assert.Equal("test", column.GetValue(2));
    }

    [Fact]
    public void RawStringColumn_EmptyStrings_ReturnsStringEmpty()
    {
        var reader = new StringColumnReader(lazy: true);
        var bytes = BuildStringColumnBytes("", "notempty", "");
        var protocolReader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        using var column = reader.ReadRawColumn(ref protocolReader, 3);

        Assert.Same(string.Empty, column.GetValue(0));
        Assert.Equal("notempty", column.GetValue(1));
        Assert.Same(string.Empty, column.GetValue(2));
    }

    [Fact]
    public void RawStringColumn_UnicodeStrings_DecodesCorrectly()
    {
        var reader = new StringColumnReader(lazy: true);
        var bytes = BuildStringColumnBytes("日本語", "émojis 🎉", "über");
        var protocolReader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        using var column = reader.ReadRawColumn(ref protocolReader, 3);

        Assert.Equal("日本語", column.GetValue(0));
        Assert.Equal("émojis 🎉", column.GetValue(1));
        Assert.Equal("über", column.GetValue(2));
    }

    [Fact]
    public void RawStringColumn_ZeroRows_ReturnsEmptyColumn()
    {
        var reader = new StringColumnReader(lazy: true);
        var protocolReader = new ProtocolReader(new ReadOnlySequence<byte>(Array.Empty<byte>()));

        using var column = reader.ReadRawColumn(ref protocolReader, 0);

        Assert.Equal(0, column.Count);
    }

    [Fact]
    public void RawStringColumn_IndexOutOfRange_Throws()
    {
        var reader = new StringColumnReader(lazy: true);
        var bytes = BuildStringColumnBytes("a", "b");
        var protocolReader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        using var column = reader.ReadRawColumn(ref protocolReader, 2);

        Assert.Throws<ArgumentOutOfRangeException>(() => column.GetValue(2));
        Assert.Throws<ArgumentOutOfRangeException>(() => column.GetValue(-1));
    }

    [Fact]
    public void RawStringColumn_LargeStrings_BufferGrows()
    {
        var reader = new StringColumnReader(lazy: true);
        var longString = new string('x', 1000);
        var bytes = BuildStringColumnBytes(longString, longString, longString);
        var protocolReader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        using var column = reader.ReadRawColumn(ref protocolReader, 3);

        Assert.Equal(longString, column.GetValue(0));
        Assert.Equal(longString, column.GetValue(1));
        Assert.Equal(longString, column.GetValue(2));
    }

    #endregion

    #region NullableRawStringColumn Tests

    [Fact]
    public void NullableRawStringColumn_MixedNulls_ReturnsCorrectly()
    {
        var reader = new StringColumnReader(lazy: true);
        var lazyNullableReader = new LazyNullableStringColumnReader(reader);
        var bytes = BuildNullableStringColumnBytes("hello", null, "world", null);
        var protocolReader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        using var column = lazyNullableReader.ReadTypedColumn(ref protocolReader, 4);

        Assert.Equal(4, column.Count);
        Assert.Equal("hello", column.GetValue(0));
        Assert.Null(column.GetValue(1));
        Assert.Equal("world", column.GetValue(2));
        Assert.Null(column.GetValue(3));
    }

    [Fact]
    public void NullableRawStringColumn_AllNulls_ReturnsAllNull()
    {
        var reader = new StringColumnReader(lazy: true);
        var lazyNullableReader = new LazyNullableStringColumnReader(reader);
        var bytes = BuildNullableStringColumnBytes(null, null, null);
        var protocolReader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        using var column = lazyNullableReader.ReadTypedColumn(ref protocolReader, 3);

        Assert.Null(column.GetValue(0));
        Assert.Null(column.GetValue(1));
        Assert.Null(column.GetValue(2));
    }

    [Fact]
    public void NullableRawStringColumn_NoNulls_ReturnsAllValues()
    {
        var reader = new StringColumnReader(lazy: true);
        var lazyNullableReader = new LazyNullableStringColumnReader(reader);
        var bytes = BuildNullableStringColumnBytes("a", "b", "c");
        var protocolReader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        using var column = lazyNullableReader.ReadTypedColumn(ref protocolReader, 3);

        Assert.Equal("a", column.GetValue(0));
        Assert.Equal("b", column.GetValue(1));
        Assert.Equal("c", column.GetValue(2));
    }

    [Fact]
    public void NullableRawStringColumn_ZeroRows_ReturnsEmptyColumn()
    {
        var reader = new StringColumnReader(lazy: true);
        var lazyNullableReader = new LazyNullableStringColumnReader(reader);
        var protocolReader = new ProtocolReader(new ReadOnlySequence<byte>(Array.Empty<byte>()));

        using var column = lazyNullableReader.ReadTypedColumn(ref protocolReader, 0);

        Assert.Equal(0, column.Count);
    }

    #endregion

    #region StringColumnReader Dual-Mode Tests

    [Fact]
    public void StringColumnReader_EagerMode_ReturnsTypedColumn()
    {
        var reader = new StringColumnReader(lazy: false);
        var bytes = BuildStringColumnBytes("hello", "world");
        var protocolReader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        using var column = ((IColumnReader)reader).ReadTypedColumn(ref protocolReader, 2);

        Assert.IsType<TypedColumn<string>>(column);
        Assert.Equal("hello", column.GetValue(0));
        Assert.Equal("world", column.GetValue(1));
    }

    [Fact]
    public void StringColumnReader_LazyMode_ReturnsRawStringColumn()
    {
        var reader = new StringColumnReader(lazy: true);
        var bytes = BuildStringColumnBytes("hello", "world");
        var protocolReader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        using var column = ((IColumnReader)reader).ReadTypedColumn(ref protocolReader, 2);

        Assert.IsType<RawStringColumn>(column);
        Assert.Equal("hello", column.GetValue(0));
        Assert.Equal("world", column.GetValue(1));
    }

    [Fact]
    public void StringColumnReader_LazyMode_GenericReadTypedColumn_StillReturnsEager()
    {
        var reader = new StringColumnReader(lazy: true);
        var bytes = BuildStringColumnBytes("hello", "world");
        var protocolReader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        // The generic path always returns eager TypedColumn<string>
        using var column = reader.ReadTypedColumn(ref protocolReader, 2);

        Assert.IsType<TypedColumn<string>>(column);
        Assert.Equal("hello", column.GetValue(0));
        Assert.Equal("world", column.GetValue(1));
    }

    [Fact]
    public void StringColumnReader_IsLazy_ReflectsConstructorParam()
    {
        Assert.False(new StringColumnReader().IsLazy);
        Assert.False(new StringColumnReader(lazy: false).IsLazy);
        Assert.True(new StringColumnReader(lazy: true).IsLazy);
    }

    #endregion

    #region ColumnReaderRegistry Tests

    [Fact]
    public void ColumnReaderRegistry_Default_HasEagerStrategy()
    {
        Assert.Equal(StringMaterialization.Eager, ColumnReaderRegistry.Default.Strategy);
    }

    [Fact]
    public void ColumnReaderRegistry_LazyStrings_HasLazyStrategy()
    {
        Assert.Equal(StringMaterialization.Lazy, ColumnReaderRegistry.LazyStrings.Strategy);
    }

    [Fact]
    public void ColumnReaderRegistry_LazyStrings_StringReaderIsLazy()
    {
        var registry = ColumnReaderRegistry.LazyStrings;
        registry.TryGetReader("String", out var reader);

        Assert.NotNull(reader);
        var stringReader = Assert.IsType<StringColumnReader>(reader);
        Assert.True(stringReader.IsLazy);
    }

    [Fact]
    public void ColumnReaderRegistry_Default_StringReaderIsEager()
    {
        var registry = ColumnReaderRegistry.Default;
        registry.TryGetReader("String", out var reader);

        Assert.NotNull(reader);
        var stringReader = Assert.IsType<StringColumnReader>(reader);
        Assert.False(stringReader.IsLazy);
    }

    [Fact]
    public void ColumnReaderRegistry_LazyStrings_NullableString_ReturnsLazyReader()
    {
        var registry = ColumnReaderRegistry.LazyStrings;
        var reader = registry.GetReader("Nullable(String)");

        Assert.IsType<LazyNullableStringColumnReader>(reader);
    }

    [Fact]
    public void ColumnReaderRegistry_Default_NullableString_ReturnsNullableRefReader()
    {
        var registry = ColumnReaderRegistry.Default;
        var reader = registry.GetReader("Nullable(String)");

        Assert.IsType<NullableRefColumnReader<string>>(reader);
    }

    #endregion

    #region LowCardinality Compatibility Tests

    /// <summary>
    /// Builds wire-format bytes for a LowCardinality(Nullable(String)) column.
    /// Format: Version(UInt64) + Flags(UInt64) + DictSize(UInt64) + Dict(Nullable(String)) + IndexCount(UInt64) + Indices(UInt8)
    /// </summary>
    private static byte[] BuildLowCardinalityNullableStringBytes(string?[] dictionaryValues, byte[] indices)
    {
        const ulong HasAdditionalKeysBit = 1UL << 9;

        // Build the dictionary payload as Nullable(String) format
        var dictPayload = BuildNullableStringColumnBytes(dictionaryValues);

        // Calculate total size:
        // Version (8) + Flags (8) + DictSize (8) + dictPayload + IndexCount (8) + indices
        var totalSize = 8 + 8 + 8 + dictPayload.Length + 8 + indices.Length;
        var result = new byte[totalSize];
        var offset = 0;

        // Version = 1
        BitConverter.TryWriteBytes(result.AsSpan(offset), (ulong)1);
        offset += 8;

        // Flags: index type UInt8 (0) | HasAdditionalKeysBit
        BitConverter.TryWriteBytes(result.AsSpan(offset), HasAdditionalKeysBit | 0UL);
        offset += 8;

        // Dictionary size
        BitConverter.TryWriteBytes(result.AsSpan(offset), (ulong)dictionaryValues.Length);
        offset += 8;

        // Dictionary values (Nullable(String) wire format)
        dictPayload.CopyTo(result.AsSpan(offset));
        offset += dictPayload.Length;

        // Index count
        BitConverter.TryWriteBytes(result.AsSpan(offset), (ulong)indices.Length);
        offset += 8;

        // Indices (UInt8)
        indices.CopyTo(result.AsSpan(offset));

        return result;
    }

    [Fact]
    public void LowCardinality_CanConstructWithLazyNullableStringReader()
    {
        var stringReader = new StringColumnReader(lazy: true);
        var lazyNullable = new LazyNullableStringColumnReader(stringReader);

        // This should not throw — previously threw ArgumentException
        var lowCard = new LowCardinalityColumnReader<string>(lazyNullable);

        Assert.NotNull(lowCard);
        Assert.Equal("LowCardinality(Nullable(String))", lowCard.TypeName);
    }

    [Fact]
    public void LazyNullableStringReader_ReadValue_ReturnsNullAndNonNull()
    {
        var stringReader = new StringColumnReader(lazy: true);
        var lazyNullable = new LazyNullableStringColumnReader(stringReader);
        IColumnReader<string> typedReader = (IColumnReader<string>)lazyNullable;

        // Non-null value: isNull=0 + string "hello"
        var helloBytes = BuildNullableStringColumnBytes(new[] { "hello" });
        var reader1 = new ProtocolReader(new ReadOnlySequence<byte>(helloBytes));
        Assert.Equal("hello", typedReader.ReadValue(ref reader1));

        // Null value: isNull=1 + empty string placeholder
        var nullBytes = BuildNullableStringColumnBytes(new string?[] { null });
        var reader2 = new ProtocolReader(new ReadOnlySequence<byte>(nullBytes));
        Assert.Null(typedReader.ReadValue(ref reader2));
    }

    [Fact]
    public void LazyNullableStringReader_GenericReadTypedColumn_ReturnsTypedColumnWithCorrectValues()
    {
        var stringReader = new StringColumnReader(lazy: true);
        var lazyNullable = new LazyNullableStringColumnReader(stringReader);
        IColumnReader<string> typedReader = (IColumnReader<string>)lazyNullable;

        var bytes = BuildNullableStringColumnBytes(new string?[] { "alpha", null, "gamma", null });
        var protocolReader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        using var column = typedReader.ReadTypedColumn(ref protocolReader, 4);

        Assert.IsType<TypedColumn<string>>(column);
        Assert.Equal(4, column.Count);
        Assert.Equal("alpha", column[0]);
        Assert.Null(column[1]);
        Assert.Equal("gamma", column[2]);
        Assert.Null(column[3]);
    }

    [Fact]
    public void LowCardinality_NullableString_FullRoundTrip_WithLazyReader()
    {
        // Dictionary: [null, "foo", "bar"]
        var dictValues = new string?[] { null, "foo", "bar" };
        // Rows: "bar", null, "foo", "foo", null, "bar"
        var indices = new byte[] { 2, 0, 1, 1, 0, 2 };

        var wireBytes = BuildLowCardinalityNullableStringBytes(dictValues, indices);

        var stringReader = new StringColumnReader(lazy: true);
        var lazyNullable = new LazyNullableStringColumnReader(stringReader);
        var lowCard = new LowCardinalityColumnReader<string>(lazyNullable);

        var protocolReader = new ProtocolReader(new ReadOnlySequence<byte>(wireBytes));
        using var column = lowCard.ReadTypedColumn(ref protocolReader, indices.Length);

        Assert.Equal(6, column.Count);
        Assert.Equal("bar", column[0]);
        Assert.Null(column[1]);
        Assert.Equal("foo", column[2]);
        Assert.Equal("foo", column[3]);
        Assert.Null(column[4]);
        Assert.Equal("bar", column[5]);
    }

    [Fact]
    public void ColumnReaderRegistry_LazyStrings_LowCardinalityNullableString_Resolves()
    {
        var registry = ColumnReaderRegistry.LazyStrings;

        // This should not throw — previously threw ArgumentException
        var reader = registry.GetReader("LowCardinality(Nullable(String))");

        Assert.NotNull(reader);
        Assert.IsType<LowCardinalityColumnReader<string>>(reader);
    }

    #endregion

    #region Eager vs Lazy Equivalence

    [Fact]
    public void EagerAndLazy_ProduceSameResults()
    {
        var testStrings = new[] { "hello", "", "world", "日本語", "a longer string with spaces" };
        var bytes = BuildStringColumnBytes(testStrings);

        // Eager
        var eagerReader = new StringColumnReader(lazy: false);
        var eagerProtocol = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        using var eagerColumn = ((IColumnReader)eagerReader).ReadTypedColumn(ref eagerProtocol, testStrings.Length);

        // Lazy
        var lazyReader = new StringColumnReader(lazy: true);
        var lazyProtocol = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        using var lazyColumn = ((IColumnReader)lazyReader).ReadTypedColumn(ref lazyProtocol, testStrings.Length);

        Assert.Equal(eagerColumn.Count, lazyColumn.Count);
        for (int i = 0; i < testStrings.Length; i++)
        {
            Assert.Equal(eagerColumn.GetValue(i), lazyColumn.GetValue(i));
        }
    }

    [Fact]
    public void EagerAndLazy_NullableString_ProduceSameResults()
    {
        var testStrings = new string?[] { "hello", null, "", "world", null };
        var bytes = BuildNullableStringColumnBytes(testStrings);

        // Eager
        var eagerInner = new StringColumnReader(lazy: false);
        var eagerNullable = new NullableRefColumnReader<string>(eagerInner);
        var eagerProtocol = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        using var eagerColumn = ((IColumnReader)eagerNullable).ReadTypedColumn(ref eagerProtocol, testStrings.Length);

        // Lazy
        var lazyInner = new StringColumnReader(lazy: true);
        var lazyNullable = new LazyNullableStringColumnReader(lazyInner);
        var lazyProtocol = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        using var lazyColumn = lazyNullable.ReadTypedColumn(ref lazyProtocol, testStrings.Length);

        Assert.Equal(eagerColumn.Count, lazyColumn.Count);
        for (int i = 0; i < testStrings.Length; i++)
        {
            Assert.Equal(eagerColumn.GetValue(i), lazyColumn.GetValue(i));
        }
    }

    #endregion
}
