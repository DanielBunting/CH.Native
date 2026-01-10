using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for Float64 (double) values.
/// </summary>
public sealed class Float64ColumnReader : IColumnReader<double>
{
    /// <inheritdoc />
    public string TypeName => "Float64";

    /// <inheritdoc />
    public Type ClrType => typeof(double);

    /// <inheritdoc />
    public double ReadValue(ref ProtocolReader reader)
    {
        using var bytes = reader.ReadPooledBytes(sizeof(double));
        return BinaryPrimitives.ReadDoubleLittleEndian(bytes.Span);
    }

    /// <inheritdoc />
    public TypedColumn<double> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        var pool = ArrayPool<double>.Shared;
        var values = pool.Rent(rowCount);
        var byteCount = rowCount * sizeof(double);

        // Fast path: bulk copy if data is contiguous
        if (reader.TryGetContiguousSpan(byteCount, out var span))
        {
            MemoryMarshal.Cast<byte, double>(span).CopyTo(values);
            reader.Advance(byteCount);
        }
        else
        {
            // Fallback: per-value read
            for (int i = 0; i < rowCount; i++)
            {
                values[i] = ReadValue(ref reader);
            }
        }

        return new TypedColumn<double>(values, rowCount, pool);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}
