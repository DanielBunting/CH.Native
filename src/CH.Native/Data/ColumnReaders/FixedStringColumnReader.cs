using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for FixedString(N) values.
/// FixedString stores exactly N bytes per value, padded with null bytes if needed.
/// </summary>
public sealed class FixedStringColumnReader : IColumnReader<byte[]>
{
    private readonly int _length;

    /// <summary>
    /// Creates a FixedString reader for the specified length.
    /// </summary>
    /// <param name="length">The fixed string length in bytes.</param>
    public FixedStringColumnReader(int length)
    {
        if (length <= 0)
            throw new ArgumentOutOfRangeException(nameof(length), "FixedString length must be positive.");

        _length = length;
    }

    /// <inheritdoc />
    public string TypeName => $"FixedString({_length})";

    /// <inheritdoc />
    public Type ClrType => typeof(byte[]);

    /// <summary>
    /// Gets the fixed string length in bytes.
    /// </summary>
    public int Length => _length;

    /// <inheritdoc />
    public byte[] ReadValue(ref ProtocolReader reader)
    {
        return reader.ReadBytes(_length).ToArray();
    }

    /// <inheritdoc />
    public TypedColumn<byte[]> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        var pool = ArrayPool<byte[]>.Shared;
        var values = pool.Rent(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            values[i] = ReadValue(ref reader);
        }
        return new TypedColumn<byte[]>(values, rowCount, pool);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }

    /// <summary>
    /// Converts the raw bytes to a string, trimming trailing null bytes.
    /// </summary>
    public static string BytesToString(byte[] bytes)
    {
        // Find the first null byte (if any) to determine actual string length
        var length = Array.IndexOf(bytes, (byte)0);
        if (length < 0)
            length = bytes.Length;

        return System.Text.Encoding.UTF8.GetString(bytes, 0, length);
    }
}
