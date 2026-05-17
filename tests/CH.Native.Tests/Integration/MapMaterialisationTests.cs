using CH.Native.Connection;
using CH.Native.Data;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// End-to-end tests for the <see cref="ClickHouseMap{TKey, TValue}"/> lossless
/// materialisation. The default <see cref="Dictionary{TKey, TValue}"/> path is
/// byte-for-byte compatible with prior releases; opting in via property type
/// (or via a scalar <c>T = ClickHouseMap&lt;,&gt;</c>) preserves duplicate keys
/// and entry order without changing any connection-level setting.
/// </summary>
[Collection("ClickHouse")]
public class MapMaterialisationTests
{
    private readonly ClickHouseFixture _fixture;

    public MapMaterialisationTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task DefaultConnection_DuplicateKeys_AreLastWinsViaDictionary()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<object>(
            "SELECT cast(map('a', 1, 'a', 2) as Map(String, Int32))");

        var dict = Assert.IsType<Dictionary<string, int>>(result);
        Assert.Single(dict);
        Assert.Equal(2, dict["a"]); // last-wins
    }

    [Fact]
    public async Task ScalarClickHouseMap_DuplicateKeys_ArePreservedInOrder()
    {
        // Layer 1 (per-call): T = ClickHouseMap<string, int> opts every Map column
        // in this call into entries shape, so duplicates survive.
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var map = await connection.ExecuteScalarAsync<ClickHouseMap<string, int>>(
            "SELECT cast(map('a', 1, 'a', 2, 'b', 3) as Map(String, Int32))");

        Assert.NotNull(map);
        Assert.Equal(3, map!.Count);
        Assert.Equal(new KeyValuePair<string, int>("a", 1), map[0]);
        Assert.Equal(new KeyValuePair<string, int>("a", 2), map[1]);
        Assert.Equal(new KeyValuePair<string, int>("b", 3), map[2]);
        Assert.True(map.HasDuplicateKeys);
    }

    [Fact]
    public async Task ScalarClickHouseMap_NoDuplicates_StillReturnsClickHouseMap()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var map = await connection.ExecuteScalarAsync<ClickHouseMap<string, int>>(
            "SELECT cast(map('a', 1, 'b', 2) as Map(String, Int32))");

        Assert.NotNull(map);
        Assert.Equal(2, map!.Count);
        Assert.False(map.HasDuplicateKeys);
        Assert.Equal(1, map["a"]);
        Assert.Equal(2, map["b"]);
    }

    [Fact]
    public async Task DefaultConnection_ExistingDictionaryAssertion_IsUnchanged()
    {
        // Regression lock-in: duplicating an existing ExtendedTypeTests assertion to
        // prove the default behaviour did not change.
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<object>(
            "SELECT map('a', toInt32(1), 'b', toInt32(2), 'c', toInt32(3))");

        var dict = Assert.IsType<Dictionary<string, int>>(result);
        Assert.Equal(3, dict.Count);
        Assert.Equal(1, dict["a"]);
        Assert.Equal(2, dict["b"]);
        Assert.Equal(3, dict["c"]);
    }

    [Fact]
    public async Task ScalarClickHouseMap_EmptyMap_RoundTrips()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var map = await connection.ExecuteScalarAsync<ClickHouseMap<string, int>>(
            "SELECT map()::Map(String, Int32)");

        Assert.NotNull(map);
        Assert.Empty(map!);
    }

    private class DuplicateMapPoco
    {
        public int Id { get; set; }
        public ClickHouseMap<string, int> Data { get; set; } = null!;
    }

    private class DuplicateMapDictionaryPoco
    {
        public int Id { get; set; }
        public Dictionary<string, int> Data { get; set; } = new();
    }

    private class DuplicateMapKvpArrayPoco
    {
        public int Id { get; set; }
        public KeyValuePair<string, int>[] Data { get; set; } = null!;
    }

    [Fact]
    public async Task TypedQuery_ClickHouseMapProperty_PreservesDuplicates()
    {
        // Per-column L1 hint from T's properties — works without any connection-level setting.
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await foreach (var row in connection.QueryAsync<DuplicateMapPoco>(
            "SELECT 1 AS Id, cast(map('a', 1, 'a', 2) as Map(String, Int32)) AS Data"))
        {
            Assert.Equal(1, row.Id);
            Assert.Equal(2, row.Data.Count);
            Assert.True(row.Data.HasDuplicateKeys);
            Assert.Equal(new KeyValuePair<string, int>("a", 1), row.Data[0]);
            Assert.Equal(new KeyValuePair<string, int>("a", 2), row.Data[1]);
        }
    }

    [Fact]
    public async Task TypedQuery_DictionaryProperty_StaysLastWins()
    {
        // Regression: existing Dictionary-typed POCOs continue to lose duplicates
        // (the documented backward-compatible behaviour).
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await foreach (var row in connection.QueryAsync<DuplicateMapDictionaryPoco>(
            "SELECT 1 AS Id, cast(map('a', 1, 'a', 2) as Map(String, Int32)) AS Data"))
        {
            Assert.Equal(1, row.Id);
            Assert.Single(row.Data);
            Assert.Equal(2, row.Data["a"]); // last-wins
        }
    }

    [Fact]
    public async Task TypedQuery_KeyValuePairArrayProperty_PreservesDuplicates()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await foreach (var row in connection.QueryAsync<DuplicateMapKvpArrayPoco>(
            "SELECT 1 AS Id, cast(map('a', 1, 'a', 2) as Map(String, Int32)) AS Data"))
        {
            Assert.Equal(1, row.Id);
            Assert.Equal(2, row.Data.Length);
            Assert.Equal(new KeyValuePair<string, int>("a", 1), row.Data[0]);
            Assert.Equal(new KeyValuePair<string, int>("a", 2), row.Data[1]);
        }
    }

    [Fact]
    public async Task BulkInsert_ClickHouseMapColumn_RoundTrips()
    {
        // End-to-end: insert a ClickHouseMap with duplicate keys, read it back through
        // a typed POCO, assert wire fidelity. Catches header/offset issues that the
        // byte-level writer unit tests can miss.
        var tableName = $"test_map_chmap_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (Id Int32, Data Map(String, Int32)) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<DuplicateMapPoco>(tableName);
            await inserter.InitAsync();
            await inserter.AddAsync(new DuplicateMapPoco
            {
                Id = 1,
                Data = new ClickHouseMap<string, int>(new[]
                {
                    new KeyValuePair<string, int>("a", 1),
                    new KeyValuePair<string, int>("a", 2),
                    new KeyValuePair<string, int>("b", 3),
                }),
            });
            await inserter.CompleteAsync();

            await foreach (var row in connection.QueryAsync<DuplicateMapPoco>(
                $"SELECT Id, Data FROM {tableName}"))
            {
                Assert.Equal(1, row.Id);
                Assert.Equal(3, row.Data.Count);
                Assert.True(row.Data.HasDuplicateKeys);
                Assert.Equal(new KeyValuePair<string, int>("a", 1), row.Data[0]);
                Assert.Equal(new KeyValuePair<string, int>("a", 2), row.Data[1]);
                Assert.Equal(new KeyValuePair<string, int>("b", 3), row.Data[2]);
            }
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task BulkInsert_KeyValuePairArrayColumn_RoundTrips()
    {
        var tableName = $"test_map_kvparr_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (Id Int32, Data Map(String, Int32)) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<DuplicateMapKvpArrayPoco>(tableName);
            await inserter.InitAsync();
            await inserter.AddAsync(new DuplicateMapKvpArrayPoco
            {
                Id = 1,
                Data = new[]
                {
                    new KeyValuePair<string, int>("x", 10),
                    new KeyValuePair<string, int>("x", 20),
                },
            });
            await inserter.CompleteAsync();

            await foreach (var row in connection.QueryAsync<DuplicateMapKvpArrayPoco>(
                $"SELECT Id, Data FROM {tableName}"))
            {
                Assert.Equal(1, row.Id);
                Assert.Equal(2, row.Data.Length);
                Assert.Equal(new KeyValuePair<string, int>("x", 10), row.Data[0]);
                Assert.Equal(new KeyValuePair<string, int>("x", 20), row.Data[1]);
            }
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    private class IReadOnlyListMapPoco
    {
        public int Id { get; set; }
        public IReadOnlyList<KeyValuePair<string, int>> Data { get; set; } = null!;
    }

    [Fact]
    public async Task BulkInsert_IReadOnlyListOfKvpColumn_RoundTrips()
    {
        var tableName = $"test_map_rolist_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (Id Int32, Data Map(String, Int32)) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<IReadOnlyListMapPoco>(tableName);
            await inserter.InitAsync();
            await inserter.AddAsync(new IReadOnlyListMapPoco
            {
                Id = 1,
                Data = new List<KeyValuePair<string, int>>
                {
                    new("k", 1),
                    new("k", 2),
                    new("k", 3),
                },
            });
            await inserter.CompleteAsync();

            await foreach (var row in connection.QueryAsync<IReadOnlyListMapPoco>(
                $"SELECT Id, Data FROM {tableName}"))
            {
                Assert.Equal(1, row.Id);
                Assert.Equal(3, row.Data.Count);
                Assert.Equal(1, row.Data[0].Value);
                Assert.Equal(2, row.Data[1].Value);
                Assert.Equal(3, row.Data[2].Value);
            }
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    // Note: Nullable(Map(K,V)) is rejected by ClickHouse at every level — both
    // CREATE TABLE and in-expression CAST surface "Nested type Map(...) cannot be
    // inside Nullable type" (error 43). The schema-level guard is locked in by
    // BulkInsertCompositeTypeTests.Schema_NullableComposite_IsRejectedByServer;
    // the writer's NullPlaceholder substitution is unit-tested by
    // MapColumnWriterNullTests. No live Nullable(Map) round-trip test exists
    // because no live Nullable(Map) value can exist.

    private class MixedShapePoco
    {
        public int Id { get; set; }
        public Dictionary<string, int> Stats { get; set; } = new();
        public ClickHouseMap<string, int> Audit { get; set; } = null!;
    }

    [Fact]
    public async Task ScalarClickHouseMap_NestedMap_InnerFallbackProducesClickHouseMap()
    {
        // Nested Map(String, Map(String, Int32)) materialised into
        // ClickHouseMap<string, ClickHouseMap<string, int>>. The outer reader
        // resolves via columnName; the inner Map reader is built recursively
        // with columnName=null, exercising the ColumnReaderFactory fallback path
        // that routes nested Maps to the hint's Fallback shape (Entries for the
        // AllEntries hint pushed by the scalar T).
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var map = await connection.ExecuteScalarAsync<ClickHouseMap<string, ClickHouseMap<string, int>>>(
            "SELECT cast(map(" +
            "  'outer1', cast(map('a', 1, 'a', 2) as Map(String, Int32))," +
            "  'outer2', cast(map('b', 3) as Map(String, Int32))" +
            ") as Map(String, Map(String, Int32)))");

        Assert.NotNull(map);
        Assert.Equal(2, map!.Count);

        // Outer entry order preserved.
        Assert.Equal("outer1", map[0].Key);
        Assert.Equal("outer2", map[1].Key);

        // Inner Maps materialised as ClickHouseMap with duplicates preserved.
        var inner1 = map[0].Value;
        Assert.Equal(2, inner1.Count);
        Assert.True(inner1.HasDuplicateKeys);
        Assert.Equal(new KeyValuePair<string, int>("a", 1), inner1[0]);
        Assert.Equal(new KeyValuePair<string, int>("a", 2), inner1[1]);

        var inner2 = map[1].Value;
        Assert.Equal(1, inner2.Count);
        Assert.Equal(new KeyValuePair<string, int>("b", 3), inner2[0]);
    }

    [Fact]
    public async Task Smoke_PocoWithDictionaryAndClickHouseMap_BothColumnsRoundTrip()
    {
        // Smoke test: a single POCO with one Dictionary property and one ClickHouseMap
        // property in the same row. Exercises per-column hint dispatch end-to-end and
        // proves the two reader paths coexist on the same query.
        var tableName = $"test_map_mixed_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (Id Int32, Stats Map(String, Int32), Audit Map(String, Int32)) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<MixedShapePoco>(tableName);
            await inserter.InitAsync();
            await inserter.AddAsync(new MixedShapePoco
            {
                Id = 7,
                Stats = new Dictionary<string, int> { ["hits"] = 10, ["misses"] = 2 },
                Audit = new ClickHouseMap<string, int>(new[]
                {
                    new KeyValuePair<string, int>("op", 1),
                    new KeyValuePair<string, int>("op", 2),
                }),
            });
            await inserter.CompleteAsync();

            await foreach (var row in connection.QueryAsync<MixedShapePoco>(
                $"SELECT Id, Stats, Audit FROM {tableName}"))
            {
                Assert.Equal(7, row.Id);
                // Dictionary path: last-wins / unordered key view.
                Assert.Equal(2, row.Stats.Count);
                Assert.Equal(10, row.Stats["hits"]);
                Assert.Equal(2, row.Stats["misses"]);
                // ClickHouseMap path: entries preserved in wire order with duplicates.
                Assert.Equal(2, row.Audit.Count);
                Assert.True(row.Audit.HasDuplicateKeys);
                Assert.Equal(new KeyValuePair<string, int>("op", 1), row.Audit[0]);
                Assert.Equal(new KeyValuePair<string, int>("op", 2), row.Audit[1]);
            }
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }
}
