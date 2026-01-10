using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for String values.
/// </summary>
public sealed class StringColumnReader : IColumnReader<string>
{
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
            var intern = new Dictionary<string, string>(StringComparer.Ordinal);
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
        return ReadTypedColumn(ref reader, rowCount);
    }
}
