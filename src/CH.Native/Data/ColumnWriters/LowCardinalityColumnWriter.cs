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
/// 4. Dictionary values (using inner writer for the base type)
/// 5. Index count (UInt64)
/// 6. Indices (using index type from flags)
///
/// For Nullable inner types, ClickHouse strips the Nullable wrapper from the dictionary.
/// Dictionary entry at index 0 represents null (written as a default value).
///
/// The CLR type accepted is the same as the inner type (dictionary encoding is transparent).
/// </remarks>
/// <typeparam name="T">The underlying type.</typeparam>
internal sealed class LowCardinalityColumnWriter<T> : IColumnWriter<T>
{
    private readonly IColumnWriter<T> _innerWriter;
    private readonly bool _isNullable;
    private readonly string _typeName;

    // Index type constants matching ClickHouse
    private const int IndexTypeUInt8 = 0;
    private const int IndexTypeUInt16 = 1;
    private const int IndexTypeUInt32 = 2;
    private const int IndexTypeUInt64 = 3;

    // KeysSerializationVersion — ClickHouse only accepts SharedDictionariesWithAdditionalKeys (1).
    private const ulong KeysSerializationVersion = 1;

    // Serialization flags — must match ClickHouse's
    // SerializationLowCardinality::IndexesSerializationType.
    private const ulong HasAdditionalKeysBit = 1UL << 9;
    private const ulong NeedUpdateDictionary = 1UL << 10;

    /// <summary>
    /// Creates a LowCardinality writer that wraps the specified inner writer.
    /// </summary>
    /// <param name="innerWriter">The writer for the underlying type (base type, not Nullable).</param>
    /// <param name="isNullable">Whether the original type was LowCardinality(Nullable(T)).</param>
    public LowCardinalityColumnWriter(IColumnWriter<T> innerWriter, bool isNullable = false)
    {
        _innerWriter = innerWriter ?? throw new ArgumentNullException(nameof(innerWriter));
        _isNullable = isNullable;
        _typeName = isNullable
            ? $"LowCardinality(Nullable({_innerWriter.TypeName}))"
            : $"LowCardinality({_innerWriter.TypeName})";
    }

    /// <summary>
    /// Creates a LowCardinality writer from a non-generic IColumnWriter.
    /// </summary>
    /// <param name="innerWriter">The writer for the underlying type.</param>
    /// <param name="isNullable">Whether the original type was LowCardinality(Nullable(T)).</param>
    public LowCardinalityColumnWriter(IColumnWriter innerWriter, bool isNullable = false)
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
        _isNullable = isNullable;
        _typeName = isNullable
            ? $"LowCardinality(Nullable({_innerWriter.TypeName}))"
            : $"LowCardinality({_innerWriter.TypeName})";
    }

    /// <inheritdoc />
    public string TypeName => _typeName;

    /// <inheritdoc />
    public Type ClrType => typeof(T);

    /// <inheritdoc />
    // The KeysSerializationVersion is a column-level state prefix that ClickHouse
    // expects BEFORE any outer composite's structural bytes (Array offsets, etc.),
    // so it lives on WritePrefix rather than inline at the top of WriteColumn.
    public void WritePrefix(ref ProtocolWriter writer)
    {
        writer.WriteUInt64(KeysSerializationVersion);
    }

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, T[] values)
    {
        if (values.Length == 0)
        {
            // Empty column — flags + dict size + index count (version was written via WritePrefix).
            writer.WriteUInt64(HasAdditionalKeysBit | NeedUpdateDictionary); // Flags
            writer.WriteUInt64(0); // Dictionary size
            writer.WriteUInt64(0); // Index count
            return;
        }

        // Build dictionary. Null keys are rejected before insertion below (either
        // mapped to slot 0 for nullable T, or thrown via NullAt for non-nullable T),
        // so the notnull-constraint warning on Dictionary<T, int> is suppressed.
        var dictionary = new List<T>();
#pragma warning disable CS8714
        var indexMap = new Dictionary<T, int>(EqualityComparer<T>.Default);
#pragma warning restore CS8714
        var indices = new int[values.Length];

        // For Nullable types, reserve dictionary slot 0 for the null/default value.
        // Don't insert `default!` into indexMap — null is handled via the explicit
        // null-check below, and for reference-type T (e.g. string) Dictionary rejects
        // null keys with ArgumentNullException.
        //
        // For reference-type T, `default(T)` is null and Phase 1's strict-null inner
        // writers (StringColumnWriter etc.) reject it. Substitute the inner writer's
        // declared NullPlaceholder so the slot-0 placeholder write produces benign
        // bytes (e.g. an empty string). For value-type T, `default(T)` is the benign
        // zero and the inner writer accepts it directly — no substitution needed.
        if (_isNullable)
        {
            var slotZero = default(T) is null
                ? _innerWriter.NullPlaceholder
                : default!;
            dictionary.Add(slotZero);
        }

        for (int i = 0; i < values.Length; i++)
        {
            var value = values[i];

            // For Nullable types, null/default maps to index 0
            if (_isNullable && EqualityComparer<T>.Default.Equals(value, default!))
            {
                indices[i] = 0;
                continue;
            }

            // For non-nullable LowCardinality(T) where T is a reference type,
            // reject null explicitly. Without this, null would land at the
            // (wrong) "first unique key" dictionary slot — silent corruption.
            if (!_isNullable && value is null)
                throw NullAt(i);

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

        // Write index type and flags (version was written via WritePrefix)
        ulong flags = (ulong)indexType | HasAdditionalKeysBit | NeedUpdateDictionary;
        writer.WriteUInt64(flags);

        // Write dictionary size and values using the base type writer
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
        // For non-nullable T, falling back to the typed WriteColumn is safe:
        // the typed path can't represent null for value-type T, and reference-type
        // T uses object identity so the existing null detection still works.
        if (!_isNullable)
        {
            var typed = new T[values.Length];
            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] is null)
                    throw NullAt(i);
                if (values[i] is T v)
                {
                    typed[i] = v;
                }
                else
                {
                    throw new InvalidOperationException(
                        $"LowCardinalityColumnWriter<{typeof(T).Name}> received unsupported value type " +
                        $"{values[i]!.GetType().Name} at row {i}. Expected {typeof(T).Name}.");
                }
            }
            WriteColumn(ref writer, typed);
            return;
        }

        // Nullable wrapper + value-type T (int/long/DateTime/etc.) hits the
        // collision: collapsing null → default(T) loses null info, and the
        // typed WriteColumn then maps every default(T) row to slot 0 (NULL).
        // Build the dictionary inline using an explicit nullness check so a
        // real default(T) row doesn't get aliased to the null sentinel.
        WriteNullableValuesFromObjects(ref writer, values);
    }

    private void WriteNullableValuesFromObjects(ref ProtocolWriter writer, object?[] values)
    {
        if (values.Length == 0)
        {
            writer.WriteUInt64(HasAdditionalKeysBit | NeedUpdateDictionary);
            writer.WriteUInt64(0);
            writer.WriteUInt64(0);
            return;
        }

        var dictionary = new List<T>();
        // Reserve dictionary slot 0 for the null sentinel — same convention
        // as WriteColumn(T[]) above so the wire format is identical.
        var slotZero = default(T) is null
            ? _innerWriter.NullPlaceholder
            : default!;
        dictionary.Add(slotZero);

#pragma warning disable CS8714
        var indexMap = new Dictionary<T, int>(EqualityComparer<T>.Default);
#pragma warning restore CS8714
        var indices = new int[values.Length];

        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is null)
            {
                indices[i] = 0;
                continue;
            }

            T value;
            if (values[i] is T v)
            {
                value = v;
            }
            else
            {
                throw new InvalidOperationException(
                    $"LowCardinalityColumnWriter<{typeof(T).Name}> received unsupported value type " +
                    $"{values[i]!.GetType().Name} at row {i}. Expected {typeof(T).Name}.");
            }

            if (!indexMap.TryGetValue(value, out var index))
            {
                index = dictionary.Count;
                dictionary.Add(value);
                indexMap[value] = index;
            }
            indices[i] = index;
        }

        var indexType = dictionary.Count <= 256 ? IndexTypeUInt8 :
                        dictionary.Count <= 65536 ? IndexTypeUInt16 :
                        dictionary.Count <= int.MaxValue ? IndexTypeUInt32 : IndexTypeUInt64;

        writer.WriteUInt64((ulong)indexType | HasAdditionalKeysBit | NeedUpdateDictionary);
        writer.WriteUInt64((ulong)dictionary.Count);
        for (int i = 0; i < dictionary.Count; i++)
        {
            _innerWriter.WriteValue(ref writer, dictionary[i]);
        }
        writer.WriteUInt64((ulong)values.Length);
        for (int i = 0; i < indices.Length; i++)
        {
            var idx = indices[i];
            switch (indexType)
            {
                case IndexTypeUInt8: writer.WriteByte((byte)idx); break;
                case IndexTypeUInt16: writer.WriteUInt16((ushort)idx); break;
                case IndexTypeUInt32: writer.WriteUInt32((uint)idx); break;
                case IndexTypeUInt64: writer.WriteUInt64((ulong)idx); break;
            }
        }
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        if (value is null)
        {
            if (!_isNullable)
                throw NullAt(rowIndex: -1);
            WriteValue(ref writer, default!);
            return;
        }
        if (value is T v)
        {
            WriteValue(ref writer, v);
        }
        else
        {
            throw new InvalidOperationException(
                $"LowCardinalityColumnWriter<{typeof(T).Name}> received unsupported value type " +
                $"{value.GetType().Name}. Expected {typeof(T).Name}.");
        }
    }

    private InvalidOperationException NullAt(int rowIndex)
    {
        var where = rowIndex >= 0 ? $" at row {rowIndex}" : string.Empty;
        return new InvalidOperationException(
            $"LowCardinalityColumnWriter<{typeof(T).Name}> received null{where}. The {_typeName} column " +
            $"is non-nullable; declare the column as LowCardinality(Nullable({_innerWriter.TypeName})) " +
            $"if null entries are valid, or ensure source values are non-null.");
    }
}
