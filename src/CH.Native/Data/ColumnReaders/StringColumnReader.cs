using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for String values. Supports both eager (default) and lazy modes.
/// In lazy mode, the non-generic ReadTypedColumn returns a <see cref="RawStringColumn"/>
/// that defers UTF-8 decoding until GetValue() is called.
/// </summary>
public sealed class StringColumnReader : IColumnReader<string>
{
    /// <summary>
    /// Thread-local pooled dictionary for string interning.
    /// Avoids allocating a new dictionary per column read.
    /// </summary>
    [ThreadStatic]
    private static Dictionary<string, string>? s_internPool;

    private readonly bool _lazy;

    private static Dictionary<string, string> GetInternDictionary()
    {
        var dict = s_internPool;
        if (dict != null)
        {
            dict.Clear();
            return dict;
        }
        return s_internPool = new Dictionary<string, string>(1024, StringComparer.Ordinal);
    }

    /// <summary>
    /// Creates a new StringColumnReader.
    /// </summary>
    /// <param name="lazy">If true, the non-generic ReadTypedColumn returns a lazy RawStringColumn.</param>
    public StringColumnReader(bool lazy = false)
    {
        _lazy = lazy;
    }

    /// <summary>
    /// Gets whether this reader operates in lazy mode.
    /// </summary>
    internal bool IsLazy => _lazy;

    /// <inheritdoc />
    public string TypeName => "String";

    /// <inheritdoc />
    public Type ClrType => typeof(string);

    /// <inheritdoc />
    public string ReadValue(ref ProtocolReader reader)
    {
        return reader.ReadString();
    }

    /// <inheritdoc />
    public TypedColumn<string> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        var pool = ArrayPool<string>.Shared;
        var values = pool.Rent(rowCount);

        // Use interning for larger columns to deduplicate repeated values
        if (rowCount >= 100)
        {
            var intern = GetInternDictionary();
            const int maxInternedStrings = 10000;

            for (int i = 0; i < rowCount; i++)
            {
                var s = reader.ReadString();
                if (intern.TryGetValue(s, out var existing))
                {
                    values[i] = existing;
                }
                else if (intern.Count < maxInternedStrings)
                {
                    intern[s] = s;
                    values[i] = s;
                }
                else
                {
                    values[i] = s;
                }
            }
        }
        else
        {
            for (int i = 0; i < rowCount; i++)
            {
                values[i] = reader.ReadString();
            }
        }

        return new TypedColumn<string>(values, rowCount, pool);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        if (_lazy)
            return ReadRawColumn(ref reader, rowCount);

        return ReadTypedColumn(ref reader, rowCount);
    }

    /// <summary>
    /// Reads string data as raw UTF-8 bytes into a <see cref="RawStringColumn"/>.
    /// Defers string materialization until GetValue() is called.
    /// </summary>
    internal RawStringColumn ReadRawColumn(ref ProtocolReader reader, int rowCount)
    {
        if (rowCount == 0)
        {
            return new RawStringColumn(
                ArrayPool<byte>.Shared.Rent(0),
                ArrayPool<int>.Shared.Rent(0),
                ArrayPool<int>.Shared.Rent(0),
                0);
        }

        var offsets = ArrayPool<int>.Shared.Rent(rowCount);
        var lengths = ArrayPool<int>.Shared.Rent(rowCount);

        // Estimate initial buffer size: assume ~32 bytes per string on average
        var estimatedSize = rowCount * 32;
        var rawData = ArrayPool<byte>.Shared.Rent(estimatedSize);
        var position = 0;

        for (int i = 0; i < rowCount; i++)
        {
            var length = (int)reader.ReadVarInt();
            offsets[i] = position;
            lengths[i] = length;

            if (length > 0)
            {
                // Ensure buffer capacity
                var required = position + length;
                if (required > rawData.Length)
                {
                    var newSize = Math.Max(rawData.Length * 2, required);
                    var newBuffer = ArrayPool<byte>.Shared.Rent(newSize);
                    Buffer.BlockCopy(rawData, 0, newBuffer, 0, position);
                    ArrayPool<byte>.Shared.Return(rawData);
                    rawData = newBuffer;
                }

                // Read bytes directly into our buffer
                using var bytes = reader.ReadPooledBytes(length);
                bytes.Span.CopyTo(rawData.AsSpan(position, length));
                position += length;
            }
        }

        return new RawStringColumn(rawData, offsets, lengths, rowCount);
    }
}
