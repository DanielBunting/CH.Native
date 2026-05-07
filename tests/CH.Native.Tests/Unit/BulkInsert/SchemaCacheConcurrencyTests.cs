using CH.Native.BulkInsert;
using Xunit;

namespace CH.Native.Tests.Unit.BulkInsert;

/// <summary>
/// <see cref="SchemaCacheTests"/> covers basic Set/TryGet/InvalidateTable/Clear and
/// a many-distinct-keys concurrent insert. This file pins the contention paths
/// that matter most for a per-connection cache: many threads racing to populate
/// the same key (last-write-wins semantics; ConcurrentDictionary indexer
/// guarantees no exceptions), and Invalidate fires while reads/writes are still
/// streaming through the cache (no enumerator faults).
/// </summary>
public class SchemaCacheConcurrencyTests
{
    [Fact]
    public void ConcurrentSet_SameKey_LastWriteWins_NoExceptions()
    {
        var cache = new SchemaCache();
        var key = new SchemaKey("default", "events", "id");

        Parallel.For(0, 200, i =>
        {
            var schema = new BulkInsertSchema(
                new[] { "id" },
                new[] { i % 2 == 0 ? "Int32" : "Int64" });
            cache.Set(key, schema);
        });

        Assert.True(cache.TryGet(key, out var final));
        Assert.NotNull(final);
        Assert.Single(final!.ColumnNames);
        // Either Int32 or Int64 won — both are legal outcomes; pin only that
        // the entry is intact and consistent.
        Assert.Contains(final.ColumnTypes[0], new[] { "Int32", "Int64" });
        Assert.Equal(1, cache.Count);
    }

    [Fact]
    public async Task ConcurrentSetAndTryGet_NeverThrowsAndAlwaysSeesAValueOnceSet()
    {
        var cache = new SchemaCache();
        var key = new SchemaKey("default", "hot", "id");
        var schema = new BulkInsertSchema(new[] { "id" }, new[] { "Int32" });
        cache.Set(key, schema);  // pre-populate so all readers should observe

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

        var writer = Task.Run(() =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                cache.Set(key, schema);
            }
        });
        var readers = Enumerable.Range(0, 4).Select(_ => Task.Run(() =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                Assert.True(cache.TryGet(key, out var got));
                Assert.NotNull(got);
            }
        })).ToArray();

        try { await Task.WhenAll(readers.Append(writer)); }
        catch (OperationCanceledException) { /* expected */ }
    }

    [Fact]
    public async Task InvalidateTable_DuringConcurrentTraffic_DoesNotThrow()
    {
        // The InvalidateTable implementation enumerates _entries.Keys while
        // mutating; ConcurrentDictionary's enumerator is documented as
        // weakly-consistent (no exception on concurrent mutation). Pin that
        // by hammering Set + TryGet from one task while Invalidate fires from
        // another — neither side should ever throw.
        var cache = new SchemaCache();
        var schema = new BulkInsertSchema(new[] { "id" }, new[] { "Int32" });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

        var setter = Task.Run(() =>
        {
            int i = 0;
            while (!cts.Token.IsCancellationRequested)
            {
                cache.Set(new SchemaKey("default", "hot", "k" + (i++ % 50)), schema);
            }
        });
        var getter = Task.Run(() =>
        {
            int i = 0;
            while (!cts.Token.IsCancellationRequested)
            {
                cache.TryGet(new SchemaKey("default", "hot", "k" + (i++ % 50)), out _);
            }
        });
        var invalidator = Task.Run(() =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                cache.InvalidateTable("default", "hot");
                Thread.Sleep(1);
            }
        });

        try { await Task.WhenAll(setter, getter, invalidator); }
        catch (OperationCanceledException) { /* expected */ }
    }

    [Fact]
    public void InvalidateTable_LeavesUnrelatedTableEntriesIntact_UnderConcurrentLoad()
    {
        var cache = new SchemaCache();
        var schema = new BulkInsertSchema(new[] { "id" }, new[] { "Int32" });

        // Populate two tables in parallel.
        Parallel.For(0, 100, i =>
        {
            cache.Set(new SchemaKey("default", "hot", "k" + i), schema);
            cache.Set(new SchemaKey("default", "cold", "k" + i), schema);
        });

        Assert.Equal(200, cache.Count);

        cache.InvalidateTable("default", "hot");

        // Cold table should still have all 100 entries; hot has zero.
        Assert.Equal(100, cache.Count);
        for (int i = 0; i < 100; i++)
        {
            Assert.False(cache.TryGet(new SchemaKey("default", "hot", "k" + i), out _));
            Assert.True(cache.TryGet(new SchemaKey("default", "cold", "k" + i), out _));
        }
    }
}
