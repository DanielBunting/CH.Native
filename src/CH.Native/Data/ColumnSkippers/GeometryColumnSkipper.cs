using System.Buffers;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnSkippers;

/// <summary>
/// Column skipper for Geometry. Reads discriminators to determine per-arm row counts,
/// then delegates to each arm skipper in global arm order.
/// </summary>
internal sealed class GeometryColumnSkipper : IColumnSkipper
{
    private const int ArmCount = 6;
    private const byte NullDiscriminator = 0xFF;
    private const ulong ModeBasic = 0;
    private const ulong ModeCompact = 1;
    private const byte GranuleFormatPlain = 0;
    private const byte GranuleFormatCompact = 1;

    private readonly IColumnSkipper[] _arms;

    /// <summary>Creates a Geometry skipper that composes the PR 1 arm skippers.</summary>
    public GeometryColumnSkipper()
    {
        _arms = new IColumnSkipper[ArmCount];
        _arms[0] = new LineStringColumnSkipper();       // LineString
        _arms[1] = new MultiLineStringColumnSkipper();  // MultiLineString
        _arms[2] = new MultiPolygonColumnSkipper();     // MultiPolygon
        _arms[3] = new PointColumnSkipper();            // Point
        _arms[4] = new PolygonColumnSkipper();          // Polygon
        _arms[5] = new RingColumnSkipper();             // Ring
    }

    /// <inheritdoc />
    public string TypeName => "Geometry";

    /// <inheritdoc />
    public bool TrySkipColumn(ref ProtocolReader reader, int rowCount)
    {
        if (rowCount == 0)
            return true;

        if (!reader.TryReadUInt64(out var mode))
            return false;

        var pool = ArrayPool<byte>.Shared;
        var discriminators = pool.Rent(rowCount);
        try
        {
            if (mode == ModeBasic)
            {
                for (int i = 0; i < rowCount; i++)
                {
                    if (!reader.TryReadByte(out var b))
                        return false;
                    discriminators[i] = b;
                }
            }
            else if (mode == ModeCompact)
            {
                int filled = 0;
                while (filled < rowCount)
                {
                    if (!reader.TryReadVarInt(out var granuleLimitLong))
                        return false;
                    var granuleLimit = ProtocolGuards.ToInt32(granuleLimitLong, "Geometry granule limit");
                    if (!reader.TryReadByte(out var granuleFormat))
                        return false;

                    if (granuleFormat == GranuleFormatCompact)
                    {
                        if (!reader.TryReadByte(out var disc))
                            return false;
                        for (int i = 0; i < granuleLimit; i++)
                            discriminators[filled + i] = disc;
                    }
                    else if (granuleFormat == GranuleFormatPlain)
                    {
                        for (int i = 0; i < granuleLimit; i++)
                        {
                            if (!reader.TryReadByte(out var b))
                                return false;
                            discriminators[filled + i] = b;
                        }
                    }
                    else
                    {
                        return false;
                    }

                    filled += granuleLimit;
                }
            }
            else
            {
                return false;
            }

            Span<int> counts = stackalloc int[ArmCount];
            for (int i = 0; i < rowCount; i++)
            {
                var disc = discriminators[i];
                if (disc == NullDiscriminator) continue;
                if (disc >= ArmCount) return false;
                counts[disc]++;
            }

            for (int arm = 0; arm < ArmCount; arm++)
            {
                if (!_arms[arm].TrySkipColumn(ref reader, counts[arm]))
                    return false;
            }

            return true;
        }
        finally
        {
            pool.Return(discriminators);
        }
    }
}
