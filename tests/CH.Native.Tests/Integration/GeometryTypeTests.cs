using CH.Native.Connection;
using CH.Native.Data.Geo;
using CH.Native.Mapping;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

[Collection("ClickHouse")]
public class GeometryTypeTests
{
    private readonly ClickHouseFixture _fixture;

    public GeometryTypeTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task ToTypeName_Spike_ReportsBareGeometry()
    {
        await using var connection = await OpenWithVariantSettingsAsync();

        await foreach (var row in connection.QueryAsync(
            "SELECT toTypeName(CAST((1.0, 2.0) AS Point)::Geometry) AS t"))
        {
            Assert.Equal("Geometry", row.GetFieldValue<string>("t"));
        }
    }

    [Fact]
    public async Task BulkInsert_EachArm_RoundTrips()
    {
        var tableName = $"test_geometry_{Guid.NewGuid():N}";
        await using var connection = await OpenWithVariantSettingsAsync();

        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (id Int32, geom Geometry) ENGINE = Memory");

        try
        {
            var rows = new List<GeometryRow>
            {
                new() { Id = 1, Geom = Geometry.From(new Point(1.5, -2.25)) },
                new() { Id = 2, Geom = Geometry.FromLineString(new Point[] { new(0, 0), new(1, 1), new(2, 0) }) },
                new() { Id = 3, Geom = Geometry.FromRing(new Point[] { new(0, 0), new(1, 0), new(1, 1), new(0, 1) }) },
                new() { Id = 4, Geom = Geometry.FromPolygon(new Point[][] {
                    new Point[] { new(0, 0), new(10, 0), new(10, 10), new(0, 10) },
                    new Point[] { new(3, 3), new(4, 3), new(4, 4) },
                }) },
                new() { Id = 5, Geom = Geometry.FromMultiLineString(new Point[][] {
                    new Point[] { new(0, 0), new(1, 1) },
                    new Point[] { new(5, 5), new(6, 6), new(7, 7) },
                }) },
                new() { Id = 6, Geom = Geometry.FromMultiPolygon(new Point[][][] {
                    new Point[][] { new Point[] { new(0, 0), new(1, 0), new(1, 1), new(0, 1) } },
                    new Point[][] { new Point[] { new(10, 10), new(11, 10), new(11, 11), new(10, 11) } },
                }) },
            };

            await using var inserter = connection.CreateBulkInserter<GeometryRow>(tableName);
            await inserter.InitAsync();
            foreach (var r in rows)
                await inserter.AddAsync(r);
            await inserter.CompleteAsync();

            var byId = new Dictionary<int, Geometry>();
            await foreach (var row in connection.QueryAsync(
                $"SELECT id, geom FROM {tableName} ORDER BY id"))
            {
                byId[row.GetFieldValue<int>("id")] = row.GetFieldValue<Geometry>("geom");
            }

            Assert.Equal(rows.Count, byId.Count);

            Assert.Equal(GeometryKind.Point, byId[1].Kind);
            Assert.Equal(new Point(1.5, -2.25), byId[1].AsPoint());

            Assert.Equal(GeometryKind.LineString, byId[2].Kind);
            Assert.Equal(rows[1].Geom.AsLineString(), byId[2].AsLineString());

            Assert.Equal(GeometryKind.Ring, byId[3].Kind);
            Assert.Equal(rows[2].Geom.AsRing(), byId[3].AsRing());

            Assert.Equal(GeometryKind.Polygon, byId[4].Kind);
            Assert.Equal(2, byId[4].AsPolygon().Length);
            Assert.Equal(rows[3].Geom.AsPolygon()[0], byId[4].AsPolygon()[0]);
            Assert.Equal(rows[3].Geom.AsPolygon()[1], byId[4].AsPolygon()[1]);

            Assert.Equal(GeometryKind.MultiLineString, byId[5].Kind);
            Assert.Equal(rows[4].Geom.AsMultiLineString()[0], byId[5].AsMultiLineString()[0]);
            Assert.Equal(rows[4].Geom.AsMultiLineString()[1], byId[5].AsMultiLineString()[1]);

            Assert.Equal(GeometryKind.MultiPolygon, byId[6].Kind);
            Assert.Equal(rows[5].Geom.AsMultiPolygon()[0][0], byId[6].AsMultiPolygon()[0][0]);
            Assert.Equal(rows[5].Geom.AsMultiPolygon()[1][0], byId[6].AsMultiPolygon()[1][0]);
        }
        finally
        {
            await using var cleanup = await OpenWithVariantSettingsAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_WithNullRows_RoundTrips()
    {
        var tableName = $"test_geometry_{Guid.NewGuid():N}";
        await using var connection = await OpenWithVariantSettingsAsync();

        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (id Int32, geom Geometry) ENGINE = Memory");

        try
        {
            var rows = new[]
            {
                new GeometryRow { Id = 1, Geom = Geometry.From(new Point(5, 5)) },
                new GeometryRow { Id = 2, Geom = Geometry.Null },
                new GeometryRow { Id = 3, Geom = Geometry.FromRing(new Point[] { new(0, 0), new(1, 1) }) },
                new GeometryRow { Id = 4, Geom = Geometry.Null },
            };

            await using var inserter = connection.CreateBulkInserter<GeometryRow>(tableName);
            await inserter.InitAsync();
            foreach (var r in rows)
                await inserter.AddAsync(r);
            await inserter.CompleteAsync();

            var results = new List<Geometry>();
            await foreach (var row in connection.QueryAsync(
                $"SELECT geom FROM {tableName} ORDER BY id"))
            {
                results.Add(row.GetFieldValue<Geometry>("geom"));
            }

            Assert.Equal(4, results.Count);
            Assert.Equal(GeometryKind.Point, results[0].Kind);
            Assert.True(results[1].IsNull);
            Assert.Equal(GeometryKind.Ring, results[2].Kind);
            Assert.True(results[3].IsNull);
        }
        finally
        {
            await using var cleanup = await OpenWithVariantSettingsAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task VariantType_ReportsArmNamesPerRow()
    {
        var tableName = $"test_geometry_{Guid.NewGuid():N}";
        await using var connection = await OpenWithVariantSettingsAsync();

        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (id Int32, geom Geometry) ENGINE = Memory");

        try
        {
            var rows = new[]
            {
                new GeometryRow { Id = 1, Geom = Geometry.From(new Point(1, 1)) },
                new GeometryRow { Id = 2, Geom = Geometry.FromPolygon(new Point[][] { new Point[] { new(0, 0), new(1, 0), new(1, 1), new(0, 1) } }) },
                new GeometryRow { Id = 3, Geom = Geometry.Null },
            };

            await using var inserter = connection.CreateBulkInserter<GeometryRow>(tableName);
            await inserter.InitAsync();
            foreach (var r in rows)
                await inserter.AddAsync(r);
            await inserter.CompleteAsync();

            // variantType() on a Geometry column returns the numeric arm index as a string
            // (CH 26.x behaviour — named-arm lookup is reserved for explicit Variant(...) types).
            // We assert the arm indices match the GeometryKind enum.
            var armIndices = new Dictionary<int, string>();
            await foreach (var row in connection.QueryAsync(
                $"SELECT id, variantType(geom) AS t FROM {tableName} ORDER BY id"))
            {
                armIndices[row.GetFieldValue<int>("id")] = row.GetFieldValue<string>("t");
            }

            Assert.Equal(((int)GeometryKind.Point).ToString(), armIndices[1]);
            Assert.Equal(((int)GeometryKind.Polygon).ToString(), armIndices[2]);
            // NULL arm surfaces as "-1", "None", or "255" depending on server version.
            Assert.Contains(armIndices[3], new[] { "-1", "None", "255" });
        }
        finally
        {
            await using var cleanup = await OpenWithVariantSettingsAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    private async Task<ClickHouseConnection> OpenWithVariantSettingsAsync()
    {
        var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        // Geometry is a suspicious Variant (server flag required on ≥25.11).
        await connection.ExecuteNonQueryAsync("SET allow_suspicious_variant_types = 1");
        return connection;
    }

    private class GeometryRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "geom", Order = 1)]
        public Geometry Geom { get; set; }
    }
}
