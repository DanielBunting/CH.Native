using System.Buffers;
using CH.Native.Data.Geo;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for ClickHouse <c>Geometry</c> — a <c>Variant</c> over the six geo arms
/// (LineString, MultiLineString, MultiPolygon, Point, Polygon, Ring) in alphabetical order.
/// </summary>
/// <remarks>
/// Wire layout per block (derived from ClickHouse
/// <c>src/DataTypes/Serializations/SerializationVariant.cpp</c>):
/// <code>
/// UInt64 mode          // 0 = BASIC, 1 = COMPACT
/// If BASIC:
///   N bytes of discriminators (0..5 = arm index, 0xFF = NULL)
/// If COMPACT:
///   varint granule_limit
///   byte   granule_format  (0 = PLAIN → followed by N discriminators, 1 = COMPACT → 1 discriminator)
///   (granule discriminators as per format byte)
/// Then, in global arm order (0..5):
///   arm_i_data serialized in columnar form for the rows whose discriminator == i.
/// </code>
/// NULL rows consume a discriminator byte (0xFF) but contribute no arm data.
/// </remarks>
public sealed class GeometryColumnReader : IColumnReader<Geometry>
{
    private const int ArmCount = 6;
    private const byte NullDiscriminator = 0xFF;
    private const ulong ModeBasic = 0;
    private const ulong ModeCompact = 1;
    private const byte GranuleFormatPlain = 0;
    private const byte GranuleFormatCompact = 1;

    private readonly IColumnReader[] _arms;

    /// <summary>Creates a Geometry reader that composes the PR 1 arm readers.</summary>
    public GeometryColumnReader()
    {
        _arms = new IColumnReader[ArmCount];
        _arms[(int)GeometryKind.LineString] = new LineStringColumnReader();
        _arms[(int)GeometryKind.MultiLineString] = new MultiLineStringColumnReader();
        _arms[(int)GeometryKind.MultiPolygon] = new MultiPolygonColumnReader();
        _arms[(int)GeometryKind.Point] = new PointColumnReader();
        _arms[(int)GeometryKind.Polygon] = new PolygonColumnReader();
        _arms[(int)GeometryKind.Ring] = new RingColumnReader();
    }

    /// <inheritdoc />
    public string TypeName => "Geometry";

    /// <inheritdoc />
    public Type ClrType => typeof(Geometry);

    /// <inheritdoc />
    public Geometry ReadValue(ref ProtocolReader reader)
    {
        // Geometry has no single-value wire form in the native protocol; the block is the
        // unit of serialization. Synthesize via a 1-row ReadTypedColumn.
        using var column = ReadTypedColumn(ref reader, 1);
        return column[0];
    }

    /// <inheritdoc />
    public TypedColumn<Geometry> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        if (rowCount == 0)
            return new TypedColumn<Geometry>(Array.Empty<Geometry>());

        var mode = reader.ReadUInt64();

        var discriminatorsPool = ArrayPool<byte>.Shared;
        var discriminators = discriminatorsPool.Rent(rowCount);
        try
        {
            if (mode == ModeBasic)
            {
                ReadDiscriminatorsBasic(ref reader, discriminators, rowCount);
            }
            else if (mode == ModeCompact)
            {
                ReadDiscriminatorsCompact(ref reader, discriminators, rowCount);
            }
            else
            {
                throw new InvalidDataException(
                    $"Unknown Variant discriminators mode {mode} for Geometry column.");
            }

            var armCounts = CountPerArm(discriminators, rowCount);

            // Read each arm's sub-column in global arm order.
            var armColumns = new ITypedColumn?[ArmCount];
            try
            {
                for (int arm = 0; arm < ArmCount; arm++)
                {
                    armColumns[arm] = _arms[arm].ReadTypedColumn(ref reader, armCounts[arm]);
                }

                // Walk discriminators, pulling the next unconsumed row from each arm.
                var result = new Geometry[rowCount];
                Span<int> armIndex = stackalloc int[ArmCount];
                for (int row = 0; row < rowCount; row++)
                {
                    var disc = discriminators[row];
                    if (disc == NullDiscriminator)
                    {
                        result[row] = Geometry.Null;
                        continue;
                    }

                    if (disc >= ArmCount)
                    {
                        throw new InvalidDataException(
                            $"Invalid Geometry discriminator {disc} at row {row}.");
                    }

                    var armValue = armColumns[disc]!.GetValue(armIndex[disc]);
                    armIndex[disc]++;
                    result[row] = new Geometry((GeometryKind)disc, armValue);
                }

                return new TypedColumn<Geometry>(result);
            }
            finally
            {
                for (int arm = 0; arm < ArmCount; arm++)
                    armColumns[arm]?.Dispose();
            }
        }
        finally
        {
            discriminatorsPool.Return(discriminators);
        }
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
        => ReadTypedColumn(ref reader, rowCount);

    private static void ReadDiscriminatorsBasic(ref ProtocolReader reader, byte[] buffer, int rowCount)
    {
        for (int i = 0; i < rowCount; i++)
            buffer[i] = reader.ReadByte();
    }

    private static void ReadDiscriminatorsCompact(ref ProtocolReader reader, byte[] buffer, int rowCount)
    {
        // COMPACT mode is split into granules. For native protocol SELECT responses the server
        // typically emits a single granule covering the whole block, but be defensive and loop.
        int filled = 0;
        while (filled < rowCount)
        {
            var granuleLimit = reader.ReadVarIntAsInt32("Geometry granule limit");
            var granuleFormat = reader.ReadByte();

            if (granuleFormat == GranuleFormatCompact)
            {
                var disc = reader.ReadByte();
                for (int i = 0; i < granuleLimit; i++)
                    buffer[filled + i] = disc;
            }
            else if (granuleFormat == GranuleFormatPlain)
            {
                for (int i = 0; i < granuleLimit; i++)
                    buffer[filled + i] = reader.ReadByte();
            }
            else
            {
                throw new InvalidDataException(
                    $"Unknown Variant compact granule format {granuleFormat}.");
            }

            filled += granuleLimit;
        }

        if (filled != rowCount)
        {
            throw new InvalidDataException(
                $"Variant compact discriminators filled {filled} rows, expected {rowCount}.");
        }
    }

    private static int[] CountPerArm(byte[] discriminators, int rowCount)
    {
        var counts = new int[ArmCount];
        for (int i = 0; i < rowCount; i++)
        {
            var disc = discriminators[i];
            if (disc == NullDiscriminator)
                continue;
            if (disc >= ArmCount)
                throw new InvalidDataException(
                    $"Invalid Geometry discriminator {disc} at row {i}.");
            counts[disc]++;
        }
        return counts;
    }
}
