using CH.Native.Connection;
using CH.Native.Data;
using CH.Native.SmokeTests.Fixtures;
using Xunit;

namespace CH.Native.SmokeTests.Query;

/// <summary>
/// Smoke coverage for <see cref="ClickHouseMap{TKey, TValue}"/> end-to-end against a
/// real ClickHouse server. The wire format is identical to the legacy
/// <see cref="Dictionary{TKey, TValue}"/> path (already covered by
/// <see cref="CompositeTypeSmokeTests.MapStringInt32"/> for cross-driver parity);
/// these tests instead pin the new CLR-side opt-in path: typed POCOs whose
/// properties signal entries shape must preserve duplicate keys in wire order.
/// </summary>
[Collection("SmokeTest")]
public class MapMaterialisationSmokeTests
{
    private readonly SmokeTestFixture _fixture;

    public MapMaterialisationSmokeTests(SmokeTestFixture fixture)
    {
        _fixture = fixture;
    }

    private class MapPoco
    {
        public int Id { get; set; }
        public ClickHouseMap<string, int> Tags { get; set; } = null!;
    }

    [Fact]
    public async Task ScalarClickHouseMap_PreservesDuplicates()
    {
        // Layer-1 selection via scalar T: the entries reader fires for the whole call.
        await using var connection = new ClickHouseConnection(_fixture.NativeConnectionString);
        await connection.OpenAsync();

        var map = await connection.ExecuteScalarAsync<ClickHouseMap<string, int>>(
            "SELECT cast(map('a', 1, 'a', 2, 'b', 3) as Map(String, Int32))");

        Assert.NotNull(map);
        Assert.Equal(3, map!.Count);
        Assert.True(map.HasDuplicateKeys);
        Assert.Equal(new KeyValuePair<string, int>("a", 1), map[0]);
        Assert.Equal(new KeyValuePair<string, int>("a", 2), map[1]);
        Assert.Equal(new KeyValuePair<string, int>("b", 3), map[2]);
    }

    [Fact]
    public async Task TypedPocoBulkInsert_RoundTripsWithDuplicates()
    {
        // Per-column Layer-1 selection from T's property type, plus a full bulk-insert
        // round-trip through a real server table. Catches integration issues that the
        // unit-level byte assertions can't (e.g. block framing under the real server).
        var table = $"smoke_map_chmap_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.NativeConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (Id Int32, Tags Map(String, Int32)) ENGINE = Memory");

        try
        {
            await using (var inserter = connection.CreateBulkInserter<MapPoco>(table))
            {
                await inserter.InitAsync();
                await inserter.AddAsync(new MapPoco
                {
                    Id = 1,
                    Tags = new ClickHouseMap<string, int>(new[]
                    {
                        new KeyValuePair<string, int>("k", 10),
                        new KeyValuePair<string, int>("k", 20),
                        new KeyValuePair<string, int>("z", 99),
                    }),
                });
                await inserter.CompleteAsync();
            }

            await foreach (var row in connection.StreamAsync<MapPoco>(
                $"SELECT Id, Tags FROM {table}"))
            {
                Assert.Equal(1, row.Id);
                Assert.Equal(3, row.Tags.Count);
                Assert.True(row.Tags.HasDuplicateKeys);
                Assert.Equal(new KeyValuePair<string, int>("k", 10), row.Tags[0]);
                Assert.Equal(new KeyValuePair<string, int>("k", 20), row.Tags[1]);
                Assert.Equal(new KeyValuePair<string, int>("z", 99), row.Tags[2]);
            }
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task UntypedQueryAsync_DefaultsToDictionary()
    {
        // Regression lock: the untyped path must continue to surface Map as
        // Dictionary<,>, byte-for-byte identical to pre-feature behaviour.
        await using var connection = new ClickHouseConnection(_fixture.NativeConnectionString);
        await connection.OpenAsync();

        await foreach (var row in connection.StreamAsync(
            "SELECT cast(map('a', 1, 'b', 2) as Map(String, Int32)) AS m"))
        {
            var value = row[0];
            Assert.IsType<Dictionary<string, int>>(value);
        }
    }
}
