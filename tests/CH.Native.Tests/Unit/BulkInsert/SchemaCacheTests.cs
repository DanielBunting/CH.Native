using CH.Native.BulkInsert;
using Xunit;

namespace CH.Native.Tests.Unit.BulkInsert;

public class SchemaCacheTests
{
    [Fact]
    public void TryGet_MissingKey_ReturnsFalse()
    {
        var cache = new SchemaCache();

        Assert.False(cache.TryGet(new SchemaKey("t", "a,b"), out _));
    }

    [Fact]
    public void Set_ThenTryGet_RoundTripsSchema()
    {
        var cache = new SchemaCache();
        var key = new SchemaKey("events", "id, name");
        var schema = new BulkInsertSchema(new[] { "id", "name" }, new[] { "Int32", "String" });

        cache.Set(key, schema);

        Assert.True(cache.TryGet(key, out var roundTripped));
        Assert.Same(schema, roundTripped);
    }

    [Fact]
    public void DifferentFingerprints_AreIndependentEntries()
    {
        var cache = new SchemaCache();
        var keyA = new SchemaKey("events", "id, name");
        var keyB = new SchemaKey("events", "id");
        var schemaA = new BulkInsertSchema(new[] { "id", "name" }, new[] { "Int32", "String" });
        var schemaB = new BulkInsertSchema(new[] { "id" }, new[] { "Int32" });

        cache.Set(keyA, schemaA);
        cache.Set(keyB, schemaB);

        Assert.Equal(2, cache.Count);
        Assert.True(cache.TryGet(keyA, out var gotA));
        Assert.True(cache.TryGet(keyB, out var gotB));
        Assert.Same(schemaA, gotA);
        Assert.Same(schemaB, gotB);
    }

    [Fact]
    public void InvalidateTable_RemovesAllEntriesForTable_LeavesOthers()
    {
        var cache = new SchemaCache();
        var eventsA = new SchemaKey("events", "id, name");
        var eventsB = new SchemaKey("events", "id");
        var users = new SchemaKey("users", "id");
        var schema = new BulkInsertSchema(new[] { "id" }, new[] { "Int32" });

        cache.Set(eventsA, schema);
        cache.Set(eventsB, schema);
        cache.Set(users, schema);

        cache.InvalidateTable("events");

        Assert.False(cache.TryGet(eventsA, out _));
        Assert.False(cache.TryGet(eventsB, out _));
        Assert.True(cache.TryGet(users, out _));
    }

    [Fact]
    public void InvalidateTable_UsesOrdinalComparison()
    {
        var cache = new SchemaCache();
        var lower = new SchemaKey("events", "id");
        var upper = new SchemaKey("EVENTS", "id");
        var schema = new BulkInsertSchema(new[] { "id" }, new[] { "Int32" });

        cache.Set(lower, schema);
        cache.Set(upper, schema);

        cache.InvalidateTable("events");

        Assert.False(cache.TryGet(lower, out _));
        Assert.True(cache.TryGet(upper, out _));
    }

    [Fact]
    public void Clear_RemovesAllEntries()
    {
        var cache = new SchemaCache();
        var schema = new BulkInsertSchema(new[] { "id" }, new[] { "Int32" });
        cache.Set(new SchemaKey("a", "x"), schema);
        cache.Set(new SchemaKey("b", "y"), schema);

        cache.Clear();

        Assert.Equal(0, cache.Count);
    }

    [Fact]
    public void ConcurrentSet_AllEntriesSurvive()
    {
        var cache = new SchemaCache();
        var schema = new BulkInsertSchema(new[] { "id" }, new[] { "Int32" });

        Parallel.For(0, 1000, i =>
        {
            cache.Set(new SchemaKey("t" + i, "id"), schema);
        });

        Assert.Equal(1000, cache.Count);
    }

    [Fact]
    public void SchemaKey_ValueEquality()
    {
        var a = new SchemaKey("events", "id, name");
        var b = new SchemaKey("events", "id, name");
        var c = new SchemaKey("events", "id");

        Assert.Equal(a, b);
        Assert.NotEqual(a, c);
    }
}
