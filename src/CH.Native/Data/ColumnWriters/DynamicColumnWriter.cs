using System.Buffers;
using CH.Native.Data.Dynamic;
using CH.Native.Data.Variant;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for <c>Dynamic</c> values.
/// </summary>
/// <remarks>
/// Mirrors <see cref="ColumnReaders.DynamicColumnReader"/>. Distinct declared type names in
/// first-seen order populate the arm list up to <c>max_types</c>; overflow types are written
/// via the shared-variant arm.
/// </remarks>
public sealed class DynamicColumnWriter : IColumnWriter<ClickHouseDynamic>
{
    private const ulong StructureVersion1 = 1;
    private const ulong DiscriminatorVersion0 = 0;

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

            if (typeIndex.Count < _maxTypes)
            {
                typeIndex.Add(name, typeNames.Count);
                typeNames.Add(name);
            }
            else
            {
                // Overflow — routed to shared arm; no arm index allocated.
                typeIndex.Add(name, -1);
            }
        }

        var numberOfTypes = typeNames.Count;
        var sharedArm = numberOfTypes;
        var armCount = numberOfTypes + 1;

        // Step 2: header + type-name table.
        writer.WriteUInt64(StructureVersion1);
        writer.WriteUInt64((ulong)_maxTypes);
        writer.WriteUInt64((ulong)numberOfTypes);
        for (int i = 0; i < numberOfTypes; i++)
            writer.WriteString(typeNames[i]);

        // Step 3: Variant discriminator version + discriminator bytes.
        writer.WriteUInt64(DiscriminatorVersion0);

        if (values.Length == 0)
            return;

        var discriminators = ArrayPool<byte>.Shared.Rent(values.Length);
        Span<int> counts = armCount <= 32 ? stackalloc int[armCount] : new int[armCount];

        try
        {
            for (int i = 0; i < values.Length; i++)
            {
                if (values[i].IsNull)
                {
                    discriminators[i] = ClickHouseDynamic.NullDiscriminator;
                    continue;
                }

                var name = values[i].DeclaredTypeName!;
                var arm = typeIndex[name];
                if (arm < 0) arm = sharedArm;
                discriminators[i] = (byte)arm;
                counts[arm]++;
            }

            writer.WriteBytes(discriminators.AsSpan(0, values.Length));

            // Step 4: bucket declared arms in a single pass.
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
                    var disc = discriminators[i];
                    if (disc == ClickHouseDynamic.NullDiscriminator || disc >= numberOfTypes) continue;
                    buckets[disc][cursors[disc]++] = values[i].Value;
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

            // Step 5: shared arm — per row write (string type_name, binary value). We still
            // emit (type_name, binary_value) pairs in row order per the wire format, but we
            // cache inner writers by type name to avoid re-resolving on every row, and share
            // a single-row scratch array across rows.
            if (counts[sharedArm] > 0)
            {
                Dictionary<string, IColumnWriter>? writerCache = null;
                // Inner writers iterate values.Length so the scratch must be exactly length 1.
                var scratch = new object?[1];

                for (int i = 0; i < values.Length; i++)
                {
                    if (discriminators[i] != sharedArm) continue;
                    var name = values[i].DeclaredTypeName!;
                    writer.WriteString(name);

                    writerCache ??= new Dictionary<string, IColumnWriter>(StringComparer.Ordinal);
                    if (!writerCache.TryGetValue(name, out var innerWriter))
                    {
                        innerWriter = _factory.CreateWriter(name);
                        writerCache[name] = innerWriter;
                    }
                    scratch[0] = values[i].Value;
                    innerWriter.WriteColumn(ref writer, scratch);
                }
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(discriminators);
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
