using System.Buffers;
using System.Buffers.Binary;
using CH.Native.Data.ColumnReaders;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data;

/// <summary>
/// Pre-fix the typed-column path silently mapped out-of-range indices to
/// <c>default(T)</c> instead of surfacing the malformed wire data, hiding
/// upstream corruption (or worse, a malicious reply) as legitimate-looking
/// nulls/zeros. Asserts the reader rejects bad indices loudly.
/// </summary>
public class LowCardinalityIndexBoundsTests
{
    /// <summary>
    /// Hand-builds a LowCardinality(Int32) column body with a single row whose
    /// index points past the end of the dictionary. Pre-fix this returned 0.
    /// </summary>
    [Fact]
    public void ReadTypedColumn_IndexBeyondDictionary_Throws()
    {
        var bytes = BuildLowCardinalityInt32(
            indexType: 0, // UInt8
            dictionary: new[] { 100, 200 },
            indices: new ulong[] { 5 });

        var reader = new LowCardinalityColumnReader<int>(new Int32ColumnReader());
        Assert.Throws<InvalidDataException>(() =>
        {
            var pr = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
            using var col = reader.ReadTypedColumn(ref pr, 1);
        });
    }

    [Fact]
    public void ReadTypedColumn_UInt64IndexBeyondInt32_Throws()
    {
        // UInt64 index values that exceed int.MaxValue must be rejected; the cast
        // path reaches dictColumn[(int)index] which would silently truncate.
        var bytes = BuildLowCardinalityInt32(
            indexType: 3, // UInt64
            dictionary: new[] { 42 },
            indices: new ulong[] { (ulong)int.MaxValue + 1UL });

        var reader = new LowCardinalityColumnReader<int>(new Int32ColumnReader());
        Assert.Throws<InvalidDataException>(() =>
        {
            var pr = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
            using var col = reader.ReadTypedColumn(ref pr, 1);
        });
    }

    [Fact]
    public void ReadTypedColumn_ValidIndices_ResolveDictionaryEntries()
    {
        // Sanity: the unhappy-path fix must not break the happy path.
        var bytes = BuildLowCardinalityInt32(
            indexType: 0, // UInt8
            dictionary: new[] { 10, 20, 30 },
            indices: new ulong[] { 0, 2, 1 });

        var reader = new LowCardinalityColumnReader<int>(new Int32ColumnReader());
        var pr = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        using var col = reader.ReadTypedColumn(ref pr, 3);

        Assert.Equal(10, col[0]);
        Assert.Equal(30, col[1]);
        Assert.Equal(20, col[2]);
    }

    /// <summary>
    /// Wire shape (after ReadPrefix has consumed the version):
    ///   flags        — UInt64 (low byte = index type)
    ///   dictSize     — UInt64
    ///   dict values  — Int32[dictSize]
    ///   indexCount   — UInt64
    ///   indices      — packed by indexType
    /// </summary>
    private static byte[] BuildLowCardinalityInt32(int indexType, int[] dictionary, ulong[] indices)
    {
        var ms = new MemoryStream();

        Span<byte> u64 = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(u64, (ulong)indexType);
        ms.Write(u64);

        BinaryPrimitives.WriteUInt64LittleEndian(u64, (ulong)dictionary.Length);
        ms.Write(u64);

        Span<byte> i32 = stackalloc byte[4];
        foreach (var v in dictionary)
        {
            BinaryPrimitives.WriteInt32LittleEndian(i32, v);
            ms.Write(i32);
        }

        BinaryPrimitives.WriteUInt64LittleEndian(u64, (ulong)indices.Length);
        ms.Write(u64);

        foreach (var idx in indices)
        {
            switch (indexType)
            {
                case 0:
                    ms.WriteByte((byte)idx);
                    break;
                case 1:
                    Span<byte> u16 = stackalloc byte[2];
                    BinaryPrimitives.WriteUInt16LittleEndian(u16, (ushort)idx);
                    ms.Write(u16);
                    break;
                case 2:
                    Span<byte> u32 = stackalloc byte[4];
                    BinaryPrimitives.WriteUInt32LittleEndian(u32, (uint)idx);
                    ms.Write(u32);
                    break;
                case 3:
                    BinaryPrimitives.WriteUInt64LittleEndian(u64, idx);
                    ms.Write(u64);
                    break;
            }
        }

        return ms.ToArray();
    }
}
