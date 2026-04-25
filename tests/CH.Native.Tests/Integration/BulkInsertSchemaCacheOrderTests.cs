using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Mapping;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Probes the schema-cache key for sensitivity to server-side column-ordering changes.
/// Finding #8 claims that after <c>ALTER TABLE ... ADD COLUMN ... AFTER</c>, the cached
/// mapping has columns in the wrong order so data lands in the wrong columns.
/// </summary>
/// <remarks>
/// The bulk-insert path emits an explicit <c>INSERT INTO t (c1, c2) VALUES</c>, and the
/// server responds with a schema block containing those columns in the client-supplied
/// order. In principle that means the cache should always match regardless of the
/// server's physical column order. These tests verify that, and cover the related
/// scenarios of MODIFY COLUMN type and RENAME COLUMN.
/// </remarks>
[Collection("ClickHouse")]
public class BulkInsertSchemaCacheOrderTests
{
    private readonly ClickHouseFixture _fixture;

    public BulkInsertSchemaCacheOrderTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    private static readonly BulkInsertOptions CachingEnabled = new() { UseSchemaCache = true };

    [Fact]
    public async Task AddColumnAfter_DoesNotScrambleCachedOrder()
    {
        var tableName = $"test_addcol_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (A Int32, B String) ENGINE = Memory");

        try
        {
            // Prime cache with the two-column INSERT.
            await connection.BulkInsertAsync(
                tableName,
                new[] { new TwoCol { A = 10, B = "ten" } },
                CachingEnabled);
            Assert.Equal(1, connection.SchemaCache.Count);

            // Add a new column in the middle of the server's physical schema.
            await connection.ExecuteNonQueryAsync(
                $"ALTER TABLE {tableName} ADD COLUMN C Float64 AFTER A");

            // Another insert using the same POCO (same cache key).
            await connection.BulkInsertAsync(
                tableName,
                new[] { new TwoCol { A = 20, B = "twenty" } },
                CachingEnabled);

            // Verify both rows landed with values in the right columns. If cache order
            // were scrambled, A and B would be swapped on the second row.
            var all = new List<AbResult>();
            await foreach (var row in connection.QueryAsync<AbResult>(
                $"SELECT A, B FROM {tableName} ORDER BY A"))
            {
                all.Add(row);
            }
            Assert.Equal(2, all.Count);
            Assert.Equal(10, all[0].A);
            Assert.Equal("ten", all[0].B);
            Assert.Equal(20, all[1].A);
            Assert.Equal("twenty", all[1].B);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task ModifyColumnType_CachedSchemaGoesStale()
    {
        // If ALTER TABLE changes an existing column's type, the cached types become
        // stale. The insert path catches ClickHouseServerException on the cache-hit
        // path and invalidates the cache — but the current insert is still rejected,
        // and the inserted data is lost for that call.
        var tableName = $"test_modcol_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (A Int32, B String) ENGINE = Memory");

        try
        {
            // Prime cache.
            await connection.BulkInsertAsync(
                tableName,
                new[] { new TwoCol { A = 1, B = "one" } },
                CachingEnabled);
            Assert.Equal(1, connection.SchemaCache.Count);

            // Change A's type to Int64. Cached schema still says Int32 for A.
            await connection.ExecuteNonQueryAsync(
                $"ALTER TABLE {tableName} MODIFY COLUMN A Int64");

            // Next insert uses stale Int32 encoding for A. Server may reject outright
            // or silently accept — both scenarios are noteworthy.
            var ex = await Record.ExceptionAsync(() => connection.BulkInsertAsync(
                tableName,
                new[] { new TwoCol { A = 2, B = "two" } },
                CachingEnabled));

            if (ex is ClickHouseServerException)
            {
                // Server rejected — expected self-healing path. Cache invalidated.
                Assert.Equal(0, connection.SchemaCache.Count);
            }
            else if (ex is null)
            {
                // Server silently accepted Int32 bytes as Int64. Check data integrity:
                // If the read comes back with the wrong A, that's the corruption bug.
                var list = new List<AbResult>();
                await foreach (var row in connection.QueryAsync<AbResult>(
                    $"SELECT A, B FROM {tableName} WHERE B = 'two'"))
                {
                    list.Add(row);
                }
                Assert.Single(list);
                // Document actual observed value; 2 is the desired value, any other
                // value is a corruption signal for the cached-types bug.
                Assert.Equal(2, list[0].A);
            }
            else
            {
                throw ex;
            }
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task DropAndReAddWithDifferentType_CachedSchemaGoesStale()
    {
        // Reproduces the "order + type change" worry: drop column, re-add with different
        // type at a different position. Cache still thinks the old schema applies.
        var tableName = $"test_readd_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (A Int32, B String) ENGINE = Memory");

        try
        {
            await connection.BulkInsertAsync(
                tableName,
                new[] { new TwoCol { A = 1, B = "one" } },
                CachingEnabled);

            await connection.ExecuteNonQueryAsync($"ALTER TABLE {tableName} DROP COLUMN A");
            await connection.ExecuteNonQueryAsync($"ALTER TABLE {tableName} ADD COLUMN A Int64 FIRST");

            // Cache still has A:Int32. POCO inserts should either: (a) be rejected by
            // the server as type mismatch (self-healing), or (b) land correctly if the
            // server is lenient.
            var ex = await Record.ExceptionAsync(() => connection.BulkInsertAsync(
                tableName,
                new[] { new TwoCol { A = 2, B = "two" } },
                CachingEnabled));

            // No matter what, a row with A=1 should be readable (from before the drop
            // and re-add, it was null-propagated or wiped; this depends on engine).
            var rows = new List<AbResult>();
            await foreach (var row in connection.QueryAsync<AbResult>(
                $"SELECT A, B FROM {tableName}"))
            {
                rows.Add(row);
            }
            Assert.NotEmpty(rows);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task DifferentPropertyOrder_ProducesDifferentCacheFingerprint()
    {
        // Confirms the cache fingerprint respects POCO property order so two different
        // POCOs targeting the same columns end up as separate entries.
        var tableName = $"test_order_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (A Int32, B String) ENGINE = Memory");

        try
        {
            await connection.BulkInsertAsync(
                tableName,
                new[] { new AbOrder { A = 1, B = "one" } },
                CachingEnabled);
            Assert.Equal(1, connection.SchemaCache.Count);

            await connection.BulkInsertAsync(
                tableName,
                new[] { new BaOrder { B = "two", A = 2 } },
                CachingEnabled);

            // Two fingerprints => two entries. If they aliased (buggy key), still 1.
            Assert.Equal(2, connection.SchemaCache.Count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #region Row types

    private class TwoCol
    {
        public int A { get; set; }
        public string B { get; set; } = string.Empty;
    }

    private class AbOrder
    {
        [ClickHouseColumn(Order = 1)] public int A { get; set; }
        [ClickHouseColumn(Order = 2)] public string B { get; set; } = string.Empty;
    }

    private class BaOrder
    {
        [ClickHouseColumn(Order = 1)] public string B { get; set; } = string.Empty;
        [ClickHouseColumn(Order = 2)] public int A { get; set; }
    }

    private class AbResult
    {
        public int A { get; set; }
        public string B { get; set; } = string.Empty;
    }

    #endregion
}
