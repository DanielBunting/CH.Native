using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Data.Geo;
using CH.Native.Mapping;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

[Collection("ClickHouse")]
public class GeoTypeTests
{
    private readonly ClickHouseFixture _fixture;

    public GeoTypeTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task ToTypeName_Spike_ServerReportsBareAliasNames()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await foreach (var row in connection.QueryAsync(
            "SELECT toTypeName(CAST((1.0, 2.0) AS Point)) AS p, " +
            "toTypeName(CAST([(1.0,2.0)] AS Ring)) AS r, " +
            "toTypeName(CAST([(1.0,2.0)] AS LineString)) AS ls, " +
            "toTypeName(CAST([[(1.0,2.0)]] AS MultiLineString)) AS mls, " +
            "toTypeName(CAST([[(1.0,2.0)]] AS Polygon)) AS poly, " +
            "toTypeName(CAST([[[(1.0,2.0)]]] AS MultiPolygon)) AS mpoly"))
        {
            Assert.Equal("Point", row.GetFieldValue<string>("p"));
            Assert.Equal("Ring", row.GetFieldValue<string>("r"));
            Assert.Equal("LineString", row.GetFieldValue<string>("ls"));
            Assert.Equal("MultiLineString", row.GetFieldValue<string>("mls"));
            Assert.Equal("Polygon", row.GetFieldValue<string>("poly"));
            Assert.Equal("MultiPolygon", row.GetFieldValue<string>("mpoly"));
        }
    }

    [Fact]
    public async Task BulkInsert_Point_RoundTrips()
    {
        var tableName = $"test_geo_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (id Int32, geom Point) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<PointRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new PointRow { Id = 1, Geom = new Point(1.0, 2.0) });
            await inserter.AddAsync(new PointRow { Id = 2, Geom = new Point(-0.5, 0.5) });
            await inserter.AddAsync(new PointRow { Id = 3, Geom = Point.Zero });
            await inserter.CompleteAsync();

            var results = new List<Point>();
            await foreach (var row in connection.QueryAsync(
                $"SELECT geom FROM {tableName} ORDER BY id"))
            {
                results.Add(row.GetFieldValue<Point>("geom"));
            }

            Assert.Equal(3, results.Count);
            Assert.Equal(new Point(1.0, 2.0), results[0]);
            Assert.Equal(new Point(-0.5, 0.5), results[1]);
            Assert.Equal(Point.Zero, results[2]);
        }
        finally
        {
            await using var cleanup = new ClickHouseConnection(_fixture.ConnectionString);
            await cleanup.OpenAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_Ring_RoundTrips()
    {
        var tableName = $"test_geo_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (id Int32, geom Ring) ENGINE = Memory");

        try
        {
            var unitSquare = new Point[] { new(0, 0), new(1, 0), new(1, 1), new(0, 1) };

            await using var inserter = connection.CreateBulkInserter<RingRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new RingRow { Id = 1, Geom = unitSquare });
            await inserter.AddAsync(new RingRow { Id = 2, Geom = Array.Empty<Point>() });
            await inserter.AddAsync(new RingRow { Id = 3, Geom = new Point[] { new(5, 5) } });
            await inserter.CompleteAsync();

            var results = new List<Point[]>();
            await foreach (var row in connection.QueryAsync(
                $"SELECT geom FROM {tableName} ORDER BY id"))
            {
                results.Add((Point[])row.GetFieldValue<object>("geom"));
            }

            Assert.Equal(3, results.Count);
            Assert.Equal(unitSquare, results[0]);
            Assert.Empty(results[1]);
            Assert.Equal(new Point[] { new(5, 5) }, results[2]);
        }
        finally
        {
            await using var cleanup = new ClickHouseConnection(_fixture.ConnectionString);
            await cleanup.OpenAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_LineString_RoundTrips()
    {
        var tableName = $"test_geo_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (id Int32, geom LineString) ENGINE = Memory");

        try
        {
            var segment = new Point[] { new(0, 0), new(1, 1) };
            var path = new Point[] { new(0, 0), new(1, 2), new(3, 5), new(8, 13), new(21, 34) };

            await using var inserter = connection.CreateBulkInserter<LineStringRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new LineStringRow { Id = 1, Geom = segment });
            await inserter.AddAsync(new LineStringRow { Id = 2, Geom = path });
            await inserter.AddAsync(new LineStringRow { Id = 3, Geom = Array.Empty<Point>() });
            await inserter.CompleteAsync();

            var results = new List<Point[]>();
            await foreach (var row in connection.QueryAsync(
                $"SELECT geom FROM {tableName} ORDER BY id"))
            {
                results.Add((Point[])row.GetFieldValue<object>("geom"));
            }

            Assert.Equal(3, results.Count);
            Assert.Equal(segment, results[0]);
            Assert.Equal(path, results[1]);
            Assert.Empty(results[2]);
        }
        finally
        {
            await using var cleanup = new ClickHouseConnection(_fixture.ConnectionString);
            await cleanup.OpenAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_MultiLineString_RoundTrips()
    {
        var tableName = $"test_geo_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (id Int32, geom MultiLineString) ENGINE = Memory");

        try
        {
            var lines = new Point[][]
            {
                new Point[] { new(0, 0), new(1, 1) },
                new Point[] { new(2, 2), new(3, 3), new(4, 4) },
            };

            await using var inserter = connection.CreateBulkInserter<MultiLineStringRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new MultiLineStringRow { Id = 1, Geom = lines });
            await inserter.CompleteAsync();

            Point[][]? read = null;
            await foreach (var row in connection.QueryAsync(
                $"SELECT geom FROM {tableName} WHERE id = 1"))
            {
                read = (Point[][])row.GetFieldValue<object>("geom");
            }

            Assert.NotNull(read);
            Assert.Equal(2, read!.Length);
            Assert.Equal(lines[0], read[0]);
            Assert.Equal(lines[1], read[1]);
        }
        finally
        {
            await using var cleanup = new ClickHouseConnection(_fixture.ConnectionString);
            await cleanup.OpenAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_Polygon_RoundTrips()
    {
        var tableName = $"test_geo_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (id Int32, geom Polygon) ENGINE = Memory");

        try
        {
            var outerRing = new Point[] { new(0, 0), new(10, 0), new(10, 10), new(0, 10) };
            var innerRing = new Point[] { new(3, 3), new(4, 3), new(4, 4) };
            var polygon = new Point[][] { outerRing, innerRing };

            await using var inserter = connection.CreateBulkInserter<PolygonRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new PolygonRow { Id = 1, Geom = polygon });
            await inserter.CompleteAsync();

            Point[][]? read = null;
            await foreach (var row in connection.QueryAsync(
                $"SELECT geom FROM {tableName} WHERE id = 1"))
            {
                read = (Point[][])row.GetFieldValue<object>("geom");
            }

            Assert.NotNull(read);
            Assert.Equal(2, read!.Length);
            Assert.Equal(outerRing, read[0]);
            Assert.Equal(innerRing, read[1]);
        }
        finally
        {
            await using var cleanup = new ClickHouseConnection(_fixture.ConnectionString);
            await cleanup.OpenAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_MultiPolygon_RoundTrips()
    {
        var tableName = $"test_geo_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (id Int32, geom MultiPolygon) ENGINE = Memory");

        try
        {
            var square1 = new Point[][]
            {
                new Point[] { new(0, 0), new(1, 0), new(1, 1), new(0, 1) }
            };
            var square2 = new Point[][]
            {
                new Point[] { new(10, 10), new(11, 10), new(11, 11), new(10, 11) }
            };
            var mp = new Point[][][] { square1, square2 };

            await using var inserter = connection.CreateBulkInserter<MultiPolygonRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new MultiPolygonRow { Id = 1, Geom = mp });
            await inserter.CompleteAsync();

            Point[][][]? read = null;
            await foreach (var row in connection.QueryAsync(
                $"SELECT geom FROM {tableName} WHERE id = 1"))
            {
                read = (Point[][][])row.GetFieldValue<object>("geom");
            }

            Assert.NotNull(read);
            Assert.Equal(2, read!.Length);
            Assert.Equal(square1[0], read[0][0]);
            Assert.Equal(square2[0], read[1][0]);
        }
        finally
        {
            await using var cleanup = new ClickHouseConnection(_fixture.ConnectionString);
            await cleanup.OpenAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task NullablePoint_IsRejectedByServer_AtCreateTable()
    {
        // Regression guard: ClickHouse does not allow geo types inside Nullable(...)
        // because they lack a storable default. The driver's NullableColumnReader<Point>
        // wiring is mechanically correct but the server short-circuits this path.
        var tableName = $"test_geo_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var ex = await Assert.ThrowsAsync<CH.Native.Exceptions.ClickHouseServerException>(() =>
            connection.ExecuteNonQueryAsync(
                $"CREATE TABLE {tableName} (id Int32, geom Nullable(Point)) ENGINE = Memory"));
        Assert.Contains("cannot be inside Nullable", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    #region Test POCOs

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

    private class LineStringRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "geom", Order = 1)]
        public Point[] Geom { get; set; } = Array.Empty<Point>();
    }

    private class MultiLineStringRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "geom", Order = 1)]
        public Point[][] Geom { get; set; } = Array.Empty<Point[]>();
    }

    private class PolygonRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "geom", Order = 1)]
        public Point[][] Geom { get; set; } = Array.Empty<Point[]>();
    }

    private class MultiPolygonRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "geom", Order = 1)]
        public Point[][][] Geom { get; set; } = Array.Empty<Point[][]>();
    }

    #endregion
}
