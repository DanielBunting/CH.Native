using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for LowCardinality(T) values.
/// </summary>
/// <remarks>
/// LowCardinality uses dictionary encoding to efficiently store columns with few unique values.
///
/// Wire format:
/// 1. Version (UInt64) - serialization version/state
/// 2. Index type and flags (UInt64)
/// 3. Dictionary size (UInt64)
/// 4. Dictionary values (using inner writer)
/// 5. Index count (UInt64)
/// 6. Indices (using index type from flags)
///
/// The CLR type accepted is the same as the inner type (dictionary encoding is transparent).
/// </remarks>
/// <typeparam name="T">The underlying type.</typeparam>
public sealed class LowCardinalityColumnWriter<T> : IColumnWriter<T>
{
    private readonly IColumnWriter<T> _innerWriter;

    // Index type constants matching ClickHouse
    private const int IndexTypeUInt8 = 0;
    private const int IndexTypeUInt16 = 1;
    private const int IndexTypeUInt32 = 2;
    private const int IndexTypeUInt64 = 3;

    // Serialization flags
    private const ulong HasAdditionalKeysBit = 1UL << 9;
    private const ulong NeedUpdateDictionary = 1UL << 11;

    /// <summary>
    /// Creates a LowCardinality writer that wraps the specified inner writer.
    /// </summary>
    /// <param name="innerWriter">The writer for the underlying type.</param>
    public LowCardinalityColumnWriter(IColumnWriter<T> innerWriter)
    {
        _innerWriter = innerWriter ?? throw new ArgumentNullException(nameof(innerWriter));
    }

    /// <summary>
    /// Creates a LowCardinality writer from a non-generic IColumnWriter.
    /// </summary>
    /// <param name="innerWriter">The writer for the underlying type.</param>
    public LowCardinalityColumnWriter(IColumnWriter innerWriter)
    {
        if (innerWriter is IColumnWriter<T> typedWriter)
        {
            _innerWriter = typedWriter;
        }
        else
        {
            throw new ArgumentException(
                $"Inner writer must implement IColumnWriter<{typeof(T).Name}>.",
                nameof(innerWriter));
        }
    }

    /// <inheritdoc />
    public string TypeName => $"LowCardinality({_innerWriter.TypeName})";

    /// <inheritdoc />
    public Type ClrType => typeof(T);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, T[] values)
    {
        if (values.Length == 0)
        {
            // Empty column
            writer.WriteUInt64(0); // Version
            writer.WriteUInt64(HasAdditionalKeysBit | NeedUpdateDictionary); // Flags
            writer.WriteUInt64(0); // Dictionary size
            writer.WriteUInt64(0); // Index count
            return;
        }

        // Build dictionary
        var dictionary = new List<T>();
        var indexMap = new Dictionary<T, int>(EqualityComparer<T>.Default);
        var indices = new int[values.Length];

        for (int i = 0; i < values.Length; i++)
        {
            var value = values[i];
            if (!indexMap.TryGetValue(value, out var index))
            {
                index = dictionary.Count;
                dictionary.Add(value);
                indexMap[value] = index;
            }
            indices[i] = index;
        }

        // Determine index type based on dictionary size
        var indexType = dictionary.Count <= 256 ? IndexTypeUInt8 :
                        dictionary.Count <= 65536 ? IndexTypeUInt16 :
                        dictionary.Count <= int.MaxValue ? IndexTypeUInt32 : IndexTypeUInt64;

        // Write version
        writer.WriteUInt64(0);

        // Write index type and flags
        ulong flags = (ulong)indexType | HasAdditionalKeysBit | NeedUpdateDictionary;
        writer.WriteUInt64(flags);

        // Write dictionary size and values
        writer.WriteUInt64((ulong)dictionary.Count);
        for (int i = 0; i < dictionary.Count; i++)
        {
            _innerWriter.WriteValue(ref writer, dictionary[i]);
        }

        // Write index count and indices
        writer.WriteUInt64((ulong)values.Length);
        for (int i = 0; i < indices.Length; i++)
        {
            var idx = indices[i];
            switch (indexType)
            {
                case IndexTypeUInt8:
                    writer.WriteByte((byte)idx);
                    break;
                case IndexTypeUInt16:
                    writer.WriteUInt16((ushort)idx);
                    break;
                case IndexTypeUInt32:
                    writer.WriteUInt32((uint)idx);
                    break;
                case IndexTypeUInt64:
                    writer.WriteUInt64((ulong)idx);
                    break;
            }
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, T value)
    {
        // Single value - write as 1-element column
        WriteColumn(ref writer, new[] { value });
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        var typed = new T[values.Length];
        for (int i = 0; i < values.Length; i++)
        {
            typed[i] = values[i] is T v ? v : default!;
        }
        WriteColumn(ref writer, typed);
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        WriteValue(ref writer, value is T v ? v : default!);
    }
}
