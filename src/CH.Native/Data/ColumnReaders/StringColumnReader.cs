using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for String values.
/// </summary>
public sealed class StringColumnReader : IColumnReader<string>
{
    /// <summary>
    /// Thread-local pooled dictionary for string interning.
    /// Avoids allocating a new dictionary per column read.
    /// </summary>
    [ThreadStatic]
    private static Dictionary<string, string>? s_internPool;

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
        return ReadTypedColumn(ref reader, rowCount);
    }
}
