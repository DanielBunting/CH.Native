using System.Buffers;
using CH.Native.Data.Geo;
using CH.Native.Protocol;

namespace CH.Native.Data.ColumnWriters;

/// <summary>
/// Column writer for ClickHouse <c>Geometry</c>. Emits the BASIC Variant layout:
/// <c>UInt64 mode=0; N discriminator bytes; then per-arm column data in global arm order</c>.
/// </summary>
/// <remarks>
/// BASIC mode is always used on write — the server accepts it regardless of its internal
/// granule strategy. The reader handles both BASIC and COMPACT modes for completeness.
/// </remarks>
internal sealed class GeometryColumnWriter : IColumnWriter<Geometry>
{
    private const int ArmCount = 6;
    private const byte NullDiscriminator = 0xFF;
    private const ulong ModeBasic = 0;

    private readonly PointColumnWriter _point = new();
    private readonly RingColumnWriter _ring = new();
    private readonly LineStringColumnWriter _lineString = new();
    private readonly PolygonColumnWriter _polygon = new();
    private readonly MultiLineStringColumnWriter _multiLineString = new();
    private readonly MultiPolygonColumnWriter _multiPolygon = new();

    /// <inheritdoc />
    public string TypeName => "Geometry";

    /// <inheritdoc />
    public Type ClrType => typeof(Geometry);

    /// <inheritdoc />
    public void WriteColumn(ref ProtocolWriter writer, Geometry[] values)
    {
        writer.WriteUInt64(ModeBasic);

        // 1. Discriminator bytes — one per row.
        for (int i = 0; i < values.Length; i++)
        {
            writer.WriteByte(DiscriminatorOf(values[i]));
        }

        // 2. Partition rows into per-arm buckets (preserving input order within each bucket).
        var buckets = RentBuckets(values, out var rented);
        try
        {
            WriteArm(ref writer, _lineString, buckets.LineString, values, ExtractLineString);
            WriteArm(ref writer, _multiLineString, buckets.MultiLineString, values, ExtractMultiLineString);
            WriteArm(ref writer, _multiPolygon, buckets.MultiPolygon, values, ExtractMultiPolygon);
            WriteArm(ref writer, _point, buckets.Point, values, ExtractPoint);
            WriteArm(ref writer, _polygon, buckets.Polygon, values, ExtractPolygon);
            WriteArm(ref writer, _ring, buckets.Ring, values, ExtractRing);
        }
        finally
        {
            ReturnBuckets(rented);
        }
    }

    /// <inheritdoc />
    public void WriteValue(ref ProtocolWriter writer, Geometry value)
    {
        // Single-row convenience — wraps into a 1-row column block.
        WriteColumn(ref writer, new[] { value });
    }

    void IColumnWriter.WriteColumn(ref ProtocolWriter writer, object?[] values)
    {
        var typed = new Geometry[values.Length];
        for (int i = 0; i < values.Length; i++)
        {
            typed[i] = values[i] switch
            {
                Geometry g => g,
                null => Geometry.Null,
                _ => throw new ArgumentException(
                    $"Cannot write {values[i]!.GetType()} as Geometry at row {i}; expected Geometry struct.",
                    nameof(values)),
            };
        }
        WriteColumn(ref writer, typed);
    }

    void IColumnWriter.WriteValue(ref ProtocolWriter writer, object? value)
    {
        var geo = value switch
        {
            Geometry g => g,
            null => Geometry.Null,
            _ => throw new ArgumentException(
                $"Cannot write {value.GetType()} as Geometry; expected Geometry struct.",
                nameof(value)),
        };
        WriteValue(ref writer, geo);
    }

    private static byte DiscriminatorOf(in Geometry g) => g.Kind == GeometryKind.Null
        ? NullDiscriminator
        : (byte)g.Kind;

    private static Point ExtractPoint(in Geometry g) => g.AsPoint();
    private static Point[] ExtractLineString(in Geometry g) => g.AsLineString();
    private static Point[] ExtractRing(in Geometry g) => g.AsRing();
    private static Point[][] ExtractPolygon(in Geometry g) => g.AsPolygon();
    private static Point[][] ExtractMultiLineString(in Geometry g) => g.AsMultiLineString();
    private static Point[][][] ExtractMultiPolygon(in Geometry g) => g.AsMultiPolygon();

    private delegate T ArmExtractor<T>(in Geometry g);

    private static void WriteArm<T>(
        ref ProtocolWriter writer,
        IColumnWriter<T> armWriter,
        ArmBucket bucket,
        Geometry[] values,
        ArmExtractor<T> extract)
    {
        var arm = new T[bucket.Count];
        for (int i = 0; i < bucket.Count; i++)
        {
            arm[i] = extract(values[bucket.RowIndices[i]]);
        }
        armWriter.WriteColumn(ref writer, arm);
    }

    private readonly struct ArmBucket
    {
        public readonly int[] RowIndices;
        public readonly int Count;
        public ArmBucket(int[] rowIndices, int count) { RowIndices = rowIndices; Count = count; }
    }

    private readonly struct ArmBuckets
    {
        public readonly ArmBucket LineString;
        public readonly ArmBucket MultiLineString;
        public readonly ArmBucket MultiPolygon;
        public readonly ArmBucket Point;
        public readonly ArmBucket Polygon;
        public readonly ArmBucket Ring;

        public ArmBuckets(ArmBucket ls, ArmBucket mls, ArmBucket mp, ArmBucket pt, ArmBucket pg, ArmBucket rg)
        {
            LineString = ls; MultiLineString = mls; MultiPolygon = mp;
            Point = pt; Polygon = pg; Ring = rg;
        }
    }

    private static ArmBuckets RentBuckets(Geometry[] values, out int[][] rented)
    {
        Span<int> counts = stackalloc int[ArmCount];
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i].Kind == GeometryKind.Null) continue;
            counts[(int)values[i].Kind]++;
        }

        rented = new int[ArmCount][];
        var pool = ArrayPool<int>.Shared;
        for (int arm = 0; arm < ArmCount; arm++)
        {
            rented[arm] = counts[arm] == 0 ? Array.Empty<int>() : pool.Rent(counts[arm]);
        }

        Span<int> idx = stackalloc int[ArmCount];
        for (int row = 0; row < values.Length; row++)
        {
            var kind = values[row].Kind;
            if (kind == GeometryKind.Null) continue;
            var arm = (int)kind;
            rented[arm][idx[arm]++] = row;
        }

        return new ArmBuckets(
            new ArmBucket(rented[(int)GeometryKind.LineString], counts[(int)GeometryKind.LineString]),
            new ArmBucket(rented[(int)GeometryKind.MultiLineString], counts[(int)GeometryKind.MultiLineString]),
            new ArmBucket(rented[(int)GeometryKind.MultiPolygon], counts[(int)GeometryKind.MultiPolygon]),
            new ArmBucket(rented[(int)GeometryKind.Point], counts[(int)GeometryKind.Point]),
            new ArmBucket(rented[(int)GeometryKind.Polygon], counts[(int)GeometryKind.Polygon]),
            new ArmBucket(rented[(int)GeometryKind.Ring], counts[(int)GeometryKind.Ring]));
    }

    private static void ReturnBuckets(int[][] rented)
    {
        var pool = ArrayPool<int>.Shared;
        for (int arm = 0; arm < ArmCount; arm++)
        {
            if (rented[arm].Length > 0)
                pool.Return(rented[arm]);
        }
    }
}
