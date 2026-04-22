using System.Buffers;
using CH.Native.Data.Dynamic;
using CH.Native.Data.Variant;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for the self-describing <c>Dynamic</c> / <c>Dynamic(max_types=N)</c> type.
/// </summary>
/// <remarks>
/// <para>Wire format per block:</para>
/// <list type="number">
/// <item><description><see langword="UInt64"/> structure version (currently <c>1</c>).</description></item>
/// <item><description><see langword="UInt64"/> max_types.</description></item>
/// <item><description><see langword="UInt64"/> numberOfTypes.</description></item>
/// <item><description>numberOfTypes string type-names.</description></item>
/// <item><description>Variant encoding with numberOfTypes + 1 arms; the last arm is the shared overflow arm.</description></item>
/// </list>
/// <para>Each row in the shared arm is a <c>(String type_name, binary_value)</c> pair whose
/// binary encoding is the single-value native format for that type name.</para>
/// <para>Produces a <see cref="DynamicTypedColumn"/>; <see cref="ClickHouseDynamic"/> values
/// are materialised on demand from the raw arm columns.</para>
/// </remarks>
public sealed class DynamicColumnReader : IColumnReader
{
    private const ulong StructureVersion1 = 1;
    private const ulong DiscriminatorVersion0 = 0;

    private readonly ColumnReaderFactory _factory;
    private readonly int _configuredMaxTypes;
    private readonly string _typeName;

    public DynamicColumnReader(ColumnReaderFactory factory, int configuredMaxTypes = 32)
    {
        _factory = factory ?? throw new ArgumentNullException(nameof(factory));
        _configuredMaxTypes = configuredMaxTypes;
        _typeName = configuredMaxTypes == 32 ? "Dynamic" : $"Dynamic(max_types={configuredMaxTypes})";
    }

    /// <inheritdoc />
    public string TypeName => _typeName;

    /// <inheritdoc />
    public Type ClrType => typeof(ClickHouseDynamic);

    /// <inheritdoc />
    public ITypedColumn ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        var structureVersion = reader.ReadUInt64();
        if (structureVersion != StructureVersion1)
            throw new NotSupportedException(
                $"Dynamic structure version {structureVersion} is not supported; expected {StructureVersion1}.");

        _ = reader.ReadUInt64(); // max_types — block-level limit; we accept whatever the server sent.
        var numberOfTypes = checked((int)reader.ReadUInt64());

        var typeNames = new string[numberOfTypes];
        var innerReaders = new IColumnReader[numberOfTypes];
        for (int i = 0; i < numberOfTypes; i++)
        {
            typeNames[i] = reader.ReadString();
            try
            {
                innerReaders[i] = _factory.CreateReader(typeNames[i]);
            }
            catch (Exception ex)
            {
                throw new NotSupportedException(
                    $"Unable to resolve reader for Dynamic arm type '{typeNames[i]}'.", ex);
            }
        }

        // Variant section follows.
        var variantVersion = reader.ReadUInt64();
        if (variantVersion != DiscriminatorVersion0)
            throw new NotSupportedException(
                $"Dynamic's inner Variant discriminator version {variantVersion} is not supported.");

        var armCount = numberOfTypes + 1; // +1 for the shared variant arm
        var sharedArm = numberOfTypes;

        if (rowCount == 0)
        {
            var emptyArms = new ITypedColumn[numberOfTypes];
            for (int i = 0; i < numberOfTypes; i++)
                emptyArms[i] = innerReaders[i].ReadTypedColumn(ref reader, 0);
            return new DynamicTypedColumn(
                Array.Empty<byte>(), 0, emptyArms, typeNames,
                sharedArmTypeNames: null, sharedArmValues: null,
                rowToArmOffset: Array.Empty<int>());
        }

        var discPool = ArrayPool<byte>.Shared;
        var discBuffer = discPool.Rent(rowCount);

        try
        {
            using (var pooled = reader.ReadPooledBytes(rowCount))
            {
                pooled.Span.CopyTo(discBuffer.AsSpan(0, rowCount));
            }

            var discriminators = discBuffer.AsSpan(0, rowCount);
            var counts = VariantBucketing.CountPerArm(discriminators, armCount);

            var armColumns = new ITypedColumn[numberOfTypes];
            var armColumnsBuilt = 0;
            try
            {
                for (int arm = 0; arm < numberOfTypes; arm++)
                {
                    armColumns[arm] = innerReaders[arm].ReadTypedColumn(ref reader, counts[arm]);
                    armColumnsBuilt++;
                }

                // Shared arm: per-row (type_name, binary value) pairs.
                string[]? sharedArmTypeNames = null;
                object?[]? sharedArmValues = null;
                var sharedCount = counts[sharedArm];
                if (sharedCount > 0)
                {
                    sharedArmTypeNames = new string[sharedCount];
                    sharedArmValues = new object?[sharedCount];
                    Dictionary<string, IColumnReader>? readerCache = null;
                    for (int r = 0; r < sharedCount; r++)
                    {
                        var sharedTypeName = reader.ReadString();
                        readerCache ??= new Dictionary<string, IColumnReader>(StringComparer.Ordinal);
                        if (!readerCache.TryGetValue(sharedTypeName, out var sharedReader))
                        {
                            try { sharedReader = _factory.CreateReader(sharedTypeName); }
                            catch (Exception ex)
                            {
                                throw new NotSupportedException(
                                    $"Dynamic shared-arm row {r} has unresolvable type '{sharedTypeName}'.", ex);
                            }
                            readerCache[sharedTypeName] = sharedReader;
                        }
                        using var oneCol = sharedReader.ReadTypedColumn(ref reader, 1);
                        sharedArmTypeNames[r] = sharedTypeName;
                        sharedArmValues[r] = oneCol.GetValue(0);
                    }
                }

                var offsetPool = ArrayPool<int>.Shared;
                var rowToArmOffset = offsetPool.Rent(rowCount);
                try
                {
                    Span<int> cursors = armCount <= 32 ? stackalloc int[armCount] : new int[armCount];
                    cursors.Clear();

                    for (int i = 0; i < rowCount; i++)
                    {
                        var disc = discriminators[i];
                        if (disc == ClickHouseDynamic.NullDiscriminator)
                        {
                            rowToArmOffset[i] = -1;
                            continue;
                        }
                        if (disc >= armCount)
                            throw new InvalidOperationException(
                                $"Discriminator {disc} out of range for Dynamic ({armCount}-arm including shared) at row {i}.");
                        rowToArmOffset[i] = cursors[disc]++;
                    }

                    return new DynamicTypedColumn(
                        discBuffer,
                        rowCount,
                        armColumns,
                        typeNames,
                        sharedArmTypeNames,
                        sharedArmValues,
                        rowToArmOffset,
                        discPool,
                        offsetPool);
                }
                catch
                {
                    offsetPool.Return(rowToArmOffset);
                    throw;
                }
            }
            catch
            {
                for (int i = 0; i < armColumnsBuilt; i++)
                    armColumns[i]?.Dispose();
                throw;
            }
        }
        catch
        {
            discPool.Return(discBuffer);
            throw;
        }
    }
}
