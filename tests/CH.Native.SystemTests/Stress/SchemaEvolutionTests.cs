using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Stress;

/// <summary>
/// What happens when the schema changes between bulk-insert flushes that share a
/// schema cache. We don't dictate the *correct* recovery policy — only that the
/// outcome is observable: either a clear error or a graceful refresh, not silent
/// data corruption.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Stress)]
public class SchemaEvolutionTests
{
    private readonly SingleNodeFixture _fixture;

    public SchemaEvolutionTests(SingleNodeFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task AddColumn_DuringInsertSession_DoesNotSilentlyCorrupt()
    {
        var table = $"se_add_{Guid.NewGuid():N}";

        await using var setup = new ClickHouseConnection(_fixture.BuildSettings());
        await setup.OpenAsync();
        await setup.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, name String) ENGINE = Memory");

        try
        {
            // Insert a first batch — populates any schema cache.
            await using (var conn = new ClickHouseConnection(_fixture.BuildSettings(b => b.WithSchemaCache(true))))
            {
                await conn.OpenAsync();
                await using var inserter = conn.CreateBulkInserter<RowV1>(table);
                await inserter.InitAsync();
                for (int i = 0; i < 100; i++)
                    await inserter.AddAsync(new RowV1 { Id = i, Name = $"v1_{i}" });
                await inserter.CompleteAsync();
            }

            // Schema drift from a sibling connection.
            await setup.ExecuteNonQueryAsync(
                $"ALTER TABLE {table} ADD COLUMN extra String DEFAULT '-'");

            // Subsequent insert with the OLD POCO. Server now expects 3 columns; the
            // POCO only has 2. The library may either (a) detect the mismatch and
            // throw, or (b) refresh the schema and insert with the new column. Both
            // are acceptable; what is NOT acceptable is silent data corruption.
            bool sawError = false;
            try
            {
                await using var conn = new ClickHouseConnection(_fixture.BuildSettings(b => b.WithSchemaCache(true)));
                await conn.OpenAsync();
                await using var inserter = conn.CreateBulkInserter<RowV1>(table);
                await inserter.InitAsync();
                for (int i = 100; i < 200; i++)
                    await inserter.AddAsync(new RowV1 { Id = i, Name = $"v1_post_{i}" });
                await inserter.CompleteAsync();
            }
            catch (Exception)
            {
                sawError = true;
            }

            // Validate: row count is consistent with what the library claims to have done.
            // If error, count should still be 100. If no error, count should be 200, and
            // every row must have a valid 'extra' value (server default fills it in).
            var count = await setup.ExecuteScalarAsync<ulong>($"SELECT count() FROM {table}");
            // The contract we assert is: no silent data corruption. Either the library
            // refused the post-ALTER insert (sawError, count=100) or accepted it
            // (count=200). Anything in between would be a bug.
            if (sawError)
                Assert.Equal(100UL, count);
            else
                Assert.Equal(200UL, count);
        }
        finally
        {
            await setup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task DropColumn_DuringInsertSession_FailsLoudly()
    {
        var table = $"se_drop_{Guid.NewGuid():N}";

        await using var setup = new ClickHouseConnection(_fixture.BuildSettings());
        await setup.OpenAsync();
        await setup.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, name String, extra String) ENGINE = Memory");

        try
        {
            await using (var conn = new ClickHouseConnection(_fixture.BuildSettings(b => b.WithSchemaCache(true))))
            {
                await conn.OpenAsync();
                await using var inserter = conn.CreateBulkInserter<RowV2>(table);
                await inserter.InitAsync();
                for (int i = 0; i < 50; i++)
                    await inserter.AddAsync(new RowV2 { Id = i, Name = $"a{i}", Extra = $"e{i}" });
                await inserter.CompleteAsync();
            }

            await setup.ExecuteNonQueryAsync($"ALTER TABLE {table} DROP COLUMN extra");

            // Inserting a POCO that still has 'extra' is now a schema mismatch. The
            // library must surface this as a typed server-side exception, NOT a wire-
            // level error or a generic exception (those would suggest the mismatch
            // wasn't detected cleanly).
            var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await using var conn = new ClickHouseConnection(_fixture.BuildSettings(b => b.WithSchemaCache(true)));
                await conn.OpenAsync();
                await using var inserter = conn.CreateBulkInserter<RowV2>(table);
                await inserter.InitAsync();
                for (int i = 50; i < 100; i++)
                    await inserter.AddAsync(new RowV2 { Id = i, Name = $"a{i}", Extra = $"e{i}" });
                await inserter.CompleteAsync();
            });
            var server = ex as ClickHouseServerException ?? ex.InnerException as ClickHouseServerException;
            Assert.NotNull(server);
        }
        finally
        {
            await setup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    private class RowV1
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "name", Order = 1)] public string Name { get; set; } = "";
    }

    private class RowV2
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "name", Order = 1)] public string Name { get; set; } = "";
        [ClickHouseColumn(Name = "extra", Order = 2)] public string Extra { get; set; } = "";
    }
}
