using System.Buffers;
using CH.Native.Data.Dynamic;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for <c>Dynamic</c> values using ClickHouse's FLATTENED native
/// serialization (structure version 3, ClickHouse 25.6+).
/// </summary>
/// <remarks>
/// Wire format per block:
/// <list type="bullet">
///   <item><description>State prefix: <c>UInt64</c> structure version = 3.</description></item>
///   <item><description>Column: <c>VARINT</c> number of types, <c>N</c> length-prefixed
///     type-name strings, an indexes column sized to the smallest <c>UIntN</c> that can
///     represent <c>N+1</c> distinct values (index = N marks NULL), then each type's
///     column data concatenated in declared order.</description></item>
/// </list>
/// </remarks>
public sealed class DynamicColumnWriter : IColumnWriter<ClickHouseDynamic>
{
    private const ulong StructureVersionFlattened = 3;

    private readonly ColumnWriterFactory _factory;
    private readonly int _maxTypes;
    private readonly string _typeName;

    public DynamicColumnWriter(ColumnWriterFactory factory, int maxTypes = 32)
    {
        _factory = factory ?? throw new ArgumentNullException(nameof(factory));
        if (maxTypes < 0 || maxTypes > 254)
            throw new ArgumentOutOfRangeException(nameof(maxTypes), "max_types must be between 0 and 254.");
        _maxTypes = maxTypes;
        _typeName = maxTypes == 32 ? "Dynamic" : $"Dynamic(max_types={maxTypes})";
    }

    /// <inheritdoc />
    public string TypeName => _typeName;

    /// <inheritdoc />
    public Type ClrType => typeof(ClickHouseDynamic);

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, ClickHouseDynamic value)
    {
        throw new NotSupportedException(
            "DynamicColumnWriter does not support row-at-a-time WriteValue. Build a 1-element array.");
    }

    /// <inheritdoc />
    public void WritePrefix(ref ProtocolWriter writer) => writer.WriteUInt64(StructureVersionFlattened);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, ClickHouseDynamic[] values)
    {
        // Step 1: collect distinct declared type names in first-seen order (NULL rows skipped).
        var typeIndex = new Dictionary<string, int>(StringComparer.Ordinal);
        var typeNames = new List<string>();

        for (int i = 0; i < values.Length; i++)
        {
            if (values[i].IsNull) continue;
            var name = values[i].DeclaredTypeName
                ?? throw new ArgumentException($"Row {i} is non-NULL but DeclaredTypeName is null.", nameof(values));

            if (typeIndex.ContainsKey(name))
                continue;

            if (typeIndex.Count >= _maxTypes)
                throw new ArgumentException(
                    $"Row {i} declares type '{name}', which would exceed max_types={_maxTypes}. FLATTENED serialization cannot route overflow types through a shared variant.",
                    nameof(values));

            typeIndex.Add(name, typeNames.Count);
            typeNames.Add(name);
        }

        var numberOfTypes = typeNames.Count;
        var nullIndex = numberOfTypes;
        var totalIndexValues = numberOfTypes + 1;

        // Step 2: type-name table.
        writer.WriteVarInt((ulong)numberOfTypes);
        for (int i = 0; i < numberOfTypes; i++)
            writer.WriteString(typeNames[i]);

        if (values.Length == 0)
            return;

        // Step 3: indexes column. Pick the smallest UIntN holding N+1 distinct values.
        Span<int> counts = totalIndexValues <= 32 ? stackalloc int[totalIndexValues] : new int[totalIndexValues];
        var indexes = ArrayPool<int>.Shared.Rent(values.Length);

        try
        {
            for (int i = 0; i < values.Length; i++)
            {
                if (values[i].IsNull)
                {
                    indexes[i] = nullIndex;
                    continue;
                }
                var idx = typeIndex[values[i].DeclaredTypeName!];
                indexes[i] = idx;
                counts[idx]++;
            }

            WriteIndexes(ref writer, indexes.AsSpan(0, values.Length), totalIndexValues);

            // Step 4: per-type column data in declared order, length-equal to count of that index.
            var buckets = ArrayPool<object?[]>.Shared.Rent(numberOfTypes > 0 ? numberOfTypes : 1);
            try
            {
                for (int arm = 0; arm < numberOfTypes; arm++)
                {
                    buckets[arm] = counts[arm] == 0
                        ? Array.Empty<object?>()
                        : new object?[counts[arm]];
                }

                Span<int> cursors = numberOfTypes <= 32 ? stackalloc int[numberOfTypes] : new int[numberOfTypes];
                for (int i = 0; i < values.Length; i++)
                {
                    var idx = indexes[i];
                    if (idx == nullIndex) continue;
                    buckets[idx][cursors[idx]++] = values[i].Value;
                }

                for (int arm = 0; arm < numberOfTypes; arm++)
                {
                    var innerWriter = _factory.CreateWriter(typeNames[arm]);
                    innerWriter.WriteColumn(ref writer, buckets[arm]);
                }
            }
            finally
            {
                ArrayPool<object?[]>.Shared.Return(buckets, clearArray: true);
            }
        }
        finally
        {
            ArrayPool<int>.Shared.Return(indexes);
        }
    }

    private static void WriteIndexes(ref ProtocolWriter writer, ReadOnlySpan<int> indexes, int totalIndexValues)
    {
        if (totalIndexValues <= byte.MaxValue + 1)
        {
            var buffer = ArrayPool<byte>.Shared.Rent(indexes.Length);
            try
            {
                for (int i = 0; i < indexes.Length; i++)
                    buffer[i] = (byte)indexes[i];
                writer.WriteBytes(buffer.AsSpan(0, indexes.Length));
            }
            finally { ArrayPool<byte>.Shared.Return(buffer); }
        }
        else if (totalIndexValues <= ushort.MaxValue + 1)
        {
            for (int i = 0; i < indexes.Length; i++)
                writer.WriteUInt16((ushort)indexes[i]);
        }
        else
        {
            for (int i = 0; i < indexes.Length; i++)
                writer.WriteUInt32((uint)indexes[i]);
        }
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        var converted = new ClickHouseDynamic[values.Length];
        for (int i = 0; i < values.Length; i++)
        {
            converted[i] = values[i] switch
            {
                null => ClickHouseDynamic.Null,
                ClickHouseDynamic d => d,
                _ => throw new ArgumentException(
                    $"Row {i}: expected ClickHouseDynamic or null, got {values[i]!.GetType()}.", nameof(values)),
            };
        }
        WriteColumn(ref writer, converted);
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value) => WriteValue(ref writer, default);
}
