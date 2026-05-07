using CH.Native.BulkInsert;
using Xunit;

namespace CH.Native.Tests.Unit.BulkInsert;

/// <summary>
/// Pins the new (Database, Table, ColumnListFingerprint) shape of <see cref="SchemaKey"/>:
/// cross-database isolation, fingerprint independence, and per-database invalidation
/// scoping. Drives the cache exercises that <c>BulkInserter</c>'s probe path relies on.
/// </summary>
public class BulkInserterSchemaKeyTests
{
    private static BulkInsertSchema MakeSchema() =>
        new(new[] { "id" }, new[] { "Int32" });

    [Fact]
    public void DifferentDatabases_SameTable_AreDistinctKeys()
    {
        var cache = new SchemaCache();
        var keyA = new SchemaKey("db1", "events", "fp");
        var keyB = new SchemaKey("db2", "events", "fp");
        var schemaA = MakeSchema();
        var schemaB = MakeSchema();

        cache.Set(keyA, schemaA);
        cache.Set(keyB, schemaB);

        Assert.Equal(2, cache.Count);
        Assert.True(cache.TryGet(keyA, out var gotA));
        Assert.True(cache.TryGet(keyB, out var gotB));
        Assert.Same(schemaA, gotA);
        Assert.Same(schemaB, gotB);
    }

    [Fact]
    public void InvalidateTable_ScopedByDatabase_DoesNotEvictOtherDb()
    {
        var cache = new SchemaCache();
        var schema = MakeSchema();
        var db1Events = new SchemaKey("db1", "events", "fp");
        var db2Events = new SchemaKey("db2", "events", "fp");
        cache.Set(db1Events, schema);
        cache.Set(db2Events, schema);

        cache.InvalidateTable("db1", "events");

        Assert.False(cache.TryGet(db1Events, out _));
        Assert.True(cache.TryGet(db2Events, out _));
    }

    [Fact]
    public void SchemaKey_HashCodeAndEquality_RespectAllThreeFields()
    {
        var a = new SchemaKey("default", "events", "fp");
        var b = new SchemaKey("default", "events", "fp");
        var diffDb = new SchemaKey("other", "events", "fp");
        var diffTable = new SchemaKey("default", "users", "fp");
        var diffFp = new SchemaKey("default", "events", "different_fp");

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.NotEqual(a, diffDb);
        Assert.NotEqual(a, diffTable);
        Assert.NotEqual(a, diffFp);
    }
}
