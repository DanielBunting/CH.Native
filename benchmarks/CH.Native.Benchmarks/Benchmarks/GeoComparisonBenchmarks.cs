using BenchmarkDotNet.Attributes;
using CH.Native.Benchmarks.Infrastructure;
using CH.Native.BulkInsert;
using CH.Native.Data.Geo;
using CH.Native.Mapping;
using NativeConnection = CH.Native.Connection.ClickHouseConnection;
using DriverConnection = ClickHouse.Driver.ADO.ClickHouseConnection;

namespace CH.Native.Benchmarks.Benchmarks;

/// <summary>
/// End-to-end geo round-trip benchmarks comparing CH.Native (native TCP) against
/// ClickHouse.Driver (HTTP). Both drivers see the same ClickHouse server so the
/// comparison captures protocol + serialization + CLR-mapping cost combined.
///
/// Note: ClickHouse.Driver uses HTTP (port 8123) and returns raw Tuple&lt;double,double&gt;;
/// CH.Native uses native TCP and returns the typed Point struct. Interpret the
/// numbers with that shape difference in mind.
/// </summary>
[Config(typeof(ProtocolComparisonConfig))]
public class GeoComparisonBenchmarks
{
    private const string PointTable = "bench_geo_point";
    private const string RingTable = "bench_geo_ring";
    private const string MultiPolygonTable = "bench_geo_multipolygon";
    private const string GeometryTable = "bench_geo_geometry";

    private string _nativeConnectionString = null!;
    private string _driverConnectionString = null!;

    private PointRow[] _pointRows = null!;
    private RingRow[] _ringRows = null!;
    private MultiPolygonRow[] _multiPolygonRows = null!;
    private GeometryRow[] _geometryRows = null!;

    [Params(1_000)]
    public int RowCount { get; set; }

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        await BenchmarkContainerManager.Instance.EnsureInitializedAsync();
        var manager = BenchmarkContainerManager.Instance;
        _nativeConnectionString = manager.NativeConnectionString;
        _driverConnectionString = manager.DriverConnectionString;

        await using var connection = new NativeConnection(_nativeConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {PointTable}");
        await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {RingTable}");
        await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {MultiPolygonTable}");
        await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {GeometryTable}");

        // Geometry is a ClickHouse Variant internally; server requires this flag to allow it.
        await connection.ExecuteNonQueryAsync("SET allow_suspicious_variant_types = 1");

        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {PointTable} (id Int32, geom Point) ENGINE = Memory");
        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {RingTable} (id Int32, geom Ring) ENGINE = Memory");
        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {MultiPolygonTable} (id Int32, geom MultiPolygon) ENGINE = Memory");
        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {GeometryTable} (id Int32, geom Geometry) ENGINE = Memory");

        var rng = new Random(42);
        _pointRows = Enumerable.Range(0, RowCount)
            .Select(i => new PointRow { Id = i, Geom = new Point(rng.NextDouble() * 180 - 90, rng.NextDouble() * 180 - 90) })
            .ToArray();

        _ringRows = Enumerable.Range(0, RowCount)
            .Select(i => new RingRow { Id = i, Geom = GenerateRing(rng, 16) })
            .ToArray();

        _multiPolygonRows = Enumerable.Range(0, RowCount / 10)
            .Select(i => new MultiPolygonRow
            {
                Id = i,
                Geom = new Point[][][]
                {
                    new Point[][] { GenerateRing(rng, 8) },
                    new Point[][] { GenerateRing(rng, 8) },
                }
            })
            .ToArray();

        // Mixed-arm Geometry rows: Point / Ring / LineString / Polygon / MultiLineString /
        // MultiPolygon rotated, with an occasional NULL. Represents a realistic mixed-shape column.
        _geometryRows = Enumerable.Range(0, RowCount)
            .Select(i =>
            {
                var g = (i % 10) switch
                {
                    0 => Geometry.Null,
                    1 => Geometry.FromRing(GenerateRing(rng, 4)),
                    2 => Geometry.FromLineString(GenerateRing(rng, 4)),
                    3 => Geometry.FromPolygon(new Point[][] { GenerateRing(rng, 4) }),
                    4 => Geometry.FromMultiLineString(new Point[][] { GenerateRing(rng, 4), GenerateRing(rng, 4) }),
                    5 => Geometry.FromMultiPolygon(new Point[][][] { new Point[][] { GenerateRing(rng, 4) } }),
                    _ => Geometry.From(new Point(rng.NextDouble(), rng.NextDouble())),
                };
                return new GeometryRow { Id = i, Geom = g };
            })
            .ToArray();

        // Seed the tables once for SELECT benchmarks.
        await connection.BulkInsertAsync(PointTable, _pointRows);
        await connection.BulkInsertAsync(RingTable, _ringRows);
        await connection.BulkInsertAsync(MultiPolygonTable, _multiPolygonRows);
        await connection.BulkInsertAsync(GeometryTable, _geometryRows);
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        try
        {
            await using var connection = new NativeConnection(_nativeConnectionString);
            await connection.OpenAsync();
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {PointTable}");
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {RingTable}");
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {MultiPolygonTable}");
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {GeometryTable}");
        }
        catch { /* best effort */ }
    }

    private static Point[] GenerateRing(Random rng, int count)
    {
        var ring = new Point[count];
        for (int i = 0; i < count; i++)
            ring[i] = new Point(rng.NextDouble(), rng.NextDouble());
        return ring;
    }

    // --- SELECT comparisons ---

    [Benchmark(Description = "SELECT 1K Points - Native")]
    public async Task<int> Native_SelectPoint()
    {
        await using var connection = new NativeConnection(_nativeConnectionString);
        await connection.OpenAsync();
        int count = 0;
        await foreach (var row in connection.QueryAsync($"SELECT geom FROM {PointTable}"))
        {
            _ = row.GetFieldValue<Point>("geom");
            count++;
        }
        return count;
    }

    [Benchmark(Description = "SELECT 1K Points - Driver")]
    public async Task<int> Driver_SelectPoint()
    {
        await using var connection = new DriverConnection(_driverConnectionString);
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT geom FROM {PointTable}";
        await using var reader = await cmd.ExecuteReaderAsync();
        int count = 0;
        while (await reader.ReadAsync())
        {
            _ = reader.GetValue(0);
            count++;
        }
        return count;
    }

    [Benchmark(Description = "SELECT 1K Rings - Native")]
    public async Task<int> Native_SelectRing()
    {
        await using var connection = new NativeConnection(_nativeConnectionString);
        await connection.OpenAsync();
        int count = 0;
        await foreach (var row in connection.QueryAsync($"SELECT geom FROM {RingTable}"))
        {
            _ = (Point[])row.GetFieldValue<object>("geom");
            count++;
        }
        return count;
    }

    [Benchmark(Description = "SELECT 1K Rings - Driver")]
    public async Task<int> Driver_SelectRing()
    {
        await using var connection = new DriverConnection(_driverConnectionString);
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT geom FROM {RingTable}";
        await using var reader = await cmd.ExecuteReaderAsync();
        int count = 0;
        while (await reader.ReadAsync())
        {
            _ = reader.GetValue(0);
            count++;
        }
        return count;
    }

    [Benchmark(Description = "SELECT 100 MultiPolygons - Native")]
    public async Task<int> Native_SelectMultiPolygon()
    {
        await using var connection = new NativeConnection(_nativeConnectionString);
        await connection.OpenAsync();
        int count = 0;
        await foreach (var row in connection.QueryAsync($"SELECT geom FROM {MultiPolygonTable}"))
        {
            _ = (Point[][][])row.GetFieldValue<object>("geom");
            count++;
        }
        return count;
    }

    [Benchmark(Description = "SELECT 100 MultiPolygons - Driver")]
    public async Task<int> Driver_SelectMultiPolygon()
    {
        await using var connection = new DriverConnection(_driverConnectionString);
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT geom FROM {MultiPolygonTable}";
        await using var reader = await cmd.ExecuteReaderAsync();
        int count = 0;
        while (await reader.ReadAsync())
        {
            _ = reader.GetValue(0);
            count++;
        }
        return count;
    }

    // --- Geometry (mixed-arm discriminated union) ---

    [Benchmark(Description = "SELECT 1K Geometry - Native")]
    public async Task<int> Native_SelectGeometry()
    {
        await using var connection = new NativeConnection(_nativeConnectionString);
        await connection.OpenAsync();
        int count = 0;
        await foreach (var row in connection.QueryAsync($"SELECT geom FROM {GeometryTable}"))
        {
            _ = row.GetFieldValue<Geometry>("geom");
            count++;
        }
        return count;
    }

    [Benchmark(Description = "SELECT 1K Geometry - Driver")]
    public async Task<int> Driver_SelectGeometry()
    {
        await using var connection = new DriverConnection(_driverConnectionString);
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT geom FROM {GeometryTable}";
        await using var reader = await cmd.ExecuteReaderAsync();
        int count = 0;
        while (await reader.ReadAsync())
        {
            _ = reader.GetValue(0);
            count++;
        }
        return count;
    }

    // --- INSERT comparisons ---
    // These use dedicated tables that we truncate each iteration so the timing
    // reflects only the insert path (not query-side state).

    [IterationSetup(Targets = new[] {
        nameof(Native_InsertPoint), nameof(Driver_InsertPoint),
        nameof(Native_InsertRing), nameof(Driver_InsertRing),
        nameof(Native_InsertMultiPolygon), nameof(Driver_InsertMultiPolygon),
        nameof(Native_InsertGeometry), nameof(Driver_InsertGeometry),
    })]
    public void TruncateInsertTables()
    {
        TruncateAsync().GetAwaiter().GetResult();
    }

    private async Task TruncateAsync()
    {
        await using var connection = new NativeConnection(_nativeConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync($"TRUNCATE TABLE {PointTable}");
        await connection.ExecuteNonQueryAsync($"TRUNCATE TABLE {RingTable}");
        await connection.ExecuteNonQueryAsync($"TRUNCATE TABLE {MultiPolygonTable}");
        await connection.ExecuteNonQueryAsync($"TRUNCATE TABLE {GeometryTable}");
    }

    [Benchmark(Description = "INSERT 1K Points - Native")]
    public async Task Native_InsertPoint()
    {
        await using var connection = new NativeConnection(_nativeConnectionString);
        await connection.OpenAsync();
        await connection.BulkInsertAsync(PointTable, _pointRows);
    }

    [Benchmark(Description = "INSERT 1K Points - Driver")]
    public async Task Driver_InsertPoint()
    {
        using var client = new ClickHouse.Driver.ClickHouseClient(_driverConnectionString);
        var rows = _pointRows.Select(r => new object[] { r.Id, Tuple.Create(r.Geom.X, r.Geom.Y) });
        await client.InsertBinaryAsync(PointTable, new[] { "id", "geom" }, rows);
    }

    [Benchmark(Description = "INSERT 1K Rings - Native")]
    public async Task Native_InsertRing()
    {
        await using var connection = new NativeConnection(_nativeConnectionString);
        await connection.OpenAsync();
        await connection.BulkInsertAsync(RingTable, _ringRows);
    }

    [Benchmark(Description = "INSERT 1K Rings - Driver")]
    public async Task Driver_InsertRing()
    {
        using var client = new ClickHouse.Driver.ClickHouseClient(_driverConnectionString);
        var rows = _ringRows.Select(r => new object[]
        {
            r.Id,
            r.Geom.Select(p => Tuple.Create(p.X, p.Y)).ToArray()
        });
        await client.InsertBinaryAsync(RingTable, new[] { "id", "geom" }, rows);
    }

    [Benchmark(Description = "INSERT 100 MultiPolygons - Native")]
    public async Task Native_InsertMultiPolygon()
    {
        await using var connection = new NativeConnection(_nativeConnectionString);
        await connection.OpenAsync();
        await connection.BulkInsertAsync(MultiPolygonTable, _multiPolygonRows);
    }

    [Benchmark(Description = "INSERT 100 MultiPolygons - Driver")]
    public async Task Driver_InsertMultiPolygon()
    {
        using var client = new ClickHouse.Driver.ClickHouseClient(_driverConnectionString);
        var rows = _multiPolygonRows.Select(r => new object[]
        {
            r.Id,
            r.Geom.Select(poly =>
                poly.Select(ring =>
                    ring.Select(p => Tuple.Create(p.X, p.Y)).ToArray()
                ).ToArray()
            ).ToArray()
        });
        await client.InsertBinaryAsync(MultiPolygonTable, new[] { "id", "geom" }, rows);
    }

    [Benchmark(Description = "INSERT 1K Geometry - Native")]
    public async Task Native_InsertGeometry()
    {
        await using var connection = new NativeConnection(_nativeConnectionString);
        await connection.OpenAsync();
        await connection.BulkInsertAsync(GeometryTable, _geometryRows);
    }

    [Benchmark(Description = "INSERT 1K Geometry - Driver")]
    public async Task Driver_InsertGeometry()
    {
        // clickhouse-cs's GeometryType.Write iterates UnderlyingTypes and calls CanWrite on each
        // until a match. It accepts raw Tuple/nested-array shapes per arm and null for NULL rows.
        using var client = new ClickHouse.Driver.ClickHouseClient(_driverConnectionString);
        var rows = _geometryRows.Select(r => new object?[]
        {
            r.Id,
            ToDriverShape(r.Geom),
        });
        await client.InsertBinaryAsync(GeometryTable, new[] { "id", "geom" }, rows);
    }

    private static object? ToDriverShape(Geometry g) => g.Kind switch
    {
        GeometryKind.Null => null,
        GeometryKind.Point => Tuple.Create(g.AsPoint().X, g.AsPoint().Y),
        GeometryKind.Ring => g.AsRing().Select(p => Tuple.Create(p.X, p.Y)).ToArray(),
        GeometryKind.LineString => g.AsLineString().Select(p => Tuple.Create(p.X, p.Y)).ToArray(),
        GeometryKind.Polygon => g.AsPolygon().Select(r => r.Select(p => Tuple.Create(p.X, p.Y)).ToArray()).ToArray(),
        GeometryKind.MultiLineString => g.AsMultiLineString().Select(r => r.Select(p => Tuple.Create(p.X, p.Y)).ToArray()).ToArray(),
        GeometryKind.MultiPolygon => g.AsMultiPolygon()
            .Select(poly => poly.Select(ring => ring.Select(p => Tuple.Create(p.X, p.Y)).ToArray()).ToArray())
            .ToArray(),
        _ => throw new InvalidOperationException($"Unhandled GeometryKind {g.Kind}"),
    };

    private class PointRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "geom", Order = 1)]
        public Point Geom { get; set; }
    }

    private class RingRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "geom", Order = 1)]
        public Point[] Geom { get; set; } = Array.Empty<Point>();
    }

    private class MultiPolygonRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "geom", Order = 1)]
        public Point[][][] Geom { get; set; } = Array.Empty<Point[][]>();
    }

    private class GeometryRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "geom", Order = 1)]
        public Geometry Geom { get; set; }
    }
}
