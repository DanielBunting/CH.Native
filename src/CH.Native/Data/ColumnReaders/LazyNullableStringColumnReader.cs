using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Lazy column reader for Nullable(String). Reads the null bitmap and raw string bytes
/// without materializing System.String objects. Returns a <see cref="NullableRawStringColumn"/>
/// that defers decoding to GetValue().
/// </summary>
internal sealed class LazyNullableStringColumnReader : IColumnReader
{
    private readonly StringColumnReader _innerReader;

    public LazyNullableStringColumnReader(StringColumnReader innerReader)
    {
        _innerReader = innerReader;
    }

    /// <inheritdoc />
    public string TypeName => "Nullable(String)";

    /// <inheritdoc />
    public Type ClrType => typeof(string);

    /// <inheritdoc />
    public ITypedColumn ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        if (rowCount == 0)
        {
            return new NullableRawStringColumn(
                ArrayPool<byte>.Shared.Rent(0),
                new RawStringColumn(
                    ArrayPool<byte>.Shared.Rent(0),
                    ArrayPool<int>.Shared.Rent(0),
                    ArrayPool<int>.Shared.Rent(0),
                    0),
                0);
        }

        // Step 1: Read null bitmap (1 byte per row)
        var nullBitmap = ArrayPool<byte>.Shared.Rent(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            nullBitmap[i] = reader.ReadByte();
        }

        // Step 2: Read ALL string values as raw bytes (ClickHouse sends placeholders for null slots)
        var inner = _innerReader.ReadRawColumn(ref reader, rowCount);

        return new NullableRawStringColumn(nullBitmap, inner, rowCount);
    }
}
