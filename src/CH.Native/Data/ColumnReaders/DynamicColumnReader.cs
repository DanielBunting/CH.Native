using System.Buffers;
using CH.Native.Data.Dynamic;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for the self-describing <c>Dynamic</c> / <c>Dynamic(max_types=N)</c> type
/// using ClickHouse's FLATTENED native serialization (structure version 3, ClickHouse 25.6+).
/// </summary>
/// <remarks>
/// <para>Wire format per block:</para>
/// <list type="number">
/// <item><description><see langword="UInt64"/> structure version (must be <c>3</c>).</description></item>
/// <item><description><c>VARINT</c> number of types.</description></item>
/// <item><description><c>N</c> length-prefixed type-name strings.</description></item>
/// <item><description>Indexes column — smallest <c>UIntN</c> holding <c>N+1</c> distinct
///     values; index <c>N</c> marks NULL.</description></item>
/// <item><description>Each type's column data concatenated in declared order, sized to
///     the count of rows whose index equals that arm.</description></item>
/// </list>
/// <para>Requires <c>SET output_format_native_use_flattened_dynamic_and_json_serialization = 1</c>
/// on the server session (CH.Native injects this by default on ClickHouse 25.6+).</para>
/// </remarks>
public sealed class DynamicColumnReader : IColumnReader
{
    private const ulong StructureVersionFlattened = 3;

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
    public void ReadPrefix(ref ProtocolReader reader)
    {
        var structureVersion = reader.ReadUInt64();
        if (structureVersion != StructureVersionFlattened)
            throw new NotSupportedException(
                $"Dynamic structure version {structureVersion} is not supported. CH.Native requires FLATTENED " +
                "(version 3); set output_format_native_use_flattened_dynamic_and_json_serialization = 1 on the " +
                "server session (ClickHouse 25.6+).");
    }

    /// <inheritdoc />
    public ITypedColumn ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        var numberOfTypes = checked((int)reader.ReadVarInt());

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

        var nullIndex = numberOfTypes;
        var totalIndexValues = numberOfTypes + 1;

        if (rowCount == 0)
        {
            var emptyArms = new ITypedColumn[numberOfTypes];
            for (int i = 0; i < numberOfTypes; i++)
                emptyArms[i] = innerReaders[i].ReadTypedColumn(ref reader, 0);
            return new DynamicTypedColumn(
                Array.Empty<int>(), 0, emptyArms, typeNames, Array.Empty<int>());
        }

        var indexPool = ArrayPool<int>.Shared;
        var indexes = indexPool.Rent(rowCount);
        var armColumns = new ITypedColumn[numberOfTypes];
        var armColumnsBuilt = 0;

        try
        {
            ReadIndexes(ref reader, indexes.AsSpan(0, rowCount), totalIndexValues);
            var counts = CountPerArm(indexes.AsSpan(0, rowCount), totalIndexValues);

            for (int arm = 0; arm < numberOfTypes; arm++)
            {
                armColumns[arm] = innerReaders[arm].ReadTypedColumn(ref reader, counts[arm]);
                armColumnsBuilt++;
            }

            var offsetPool = ArrayPool<int>.Shared;
            var rowToArmOffset = offsetPool.Rent(rowCount);
            try
            {
                Span<int> cursors = numberOfTypes <= 32 ? stackalloc int[numberOfTypes] : new int[numberOfTypes];
                cursors.Clear();

                for (int i = 0; i < rowCount; i++)
                {
                    var idx = indexes[i];
                    if (idx == nullIndex)
                    {
                        rowToArmOffset[i] = -1;
                        continue;
                    }
                    if ((uint)idx >= (uint)numberOfTypes)
                        throw new InvalidOperationException(
                            $"Index {idx} out of range for Dynamic ({numberOfTypes} types) at row {i}.");
                    rowToArmOffset[i] = cursors[idx]++;
                }

                return new DynamicTypedColumn(
                    indexes, rowCount, armColumns, typeNames, rowToArmOffset, indexPool, offsetPool);
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
            indexPool.Return(indexes);
            throw;
        }
    }

    private static void ReadIndexes(ref ProtocolReader reader, Span<int> indexes, int totalIndexValues)
    {
        if (totalIndexValues <= byte.MaxValue + 1)
        {
            using var pooled = reader.ReadPooledBytes(indexes.Length);
            var span = pooled.Span;
            for (int i = 0; i < indexes.Length; i++)
                indexes[i] = span[i];
        }
        else if (totalIndexValues <= ushort.MaxValue + 1)
        {
            for (int i = 0; i < indexes.Length; i++)
                indexes[i] = reader.ReadUInt16();
        }
        else
        {
            for (int i = 0; i < indexes.Length; i++)
                indexes[i] = checked((int)reader.ReadUInt32());
        }
    }

    private static int[] CountPerArm(ReadOnlySpan<int> indexes, int totalIndexValues)
    {
        var counts = new int[totalIndexValues];
        for (int i = 0; i < indexes.Length; i++)
            counts[indexes[i]]++;
        return counts;
    }
}
