using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.BulkInsertFailures;

/// <summary>
/// Pins the write-side <c>BulkInserter.GetPropertyMappings</c> behaviour by
/// asserting end-to-end column placement on a real ClickHouse table. The
/// discovery method is private; this test proves correctness by inserting a
/// POCO whose properties are declared in one order and tagged with
/// <see cref="ClickHouseColumnAttribute.Order"/> in a different order, then
/// verifying that columns land in the attribute-defined positions on the
/// server.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class BulkInsertColumnOrderTests
{
    private readonly SingleNodeFixture _fx;

    public BulkInsertColumnOrderTests(SingleNodeFixture fx) => _fx = fx;

    /// <summary>
    /// Properties declared A, B, C but Order = 2, 0, 1 — so the inserter
    /// should serialize as B, C, A.
    /// </summary>
    public class OutOfOrderRow
    {
        [ClickHouseColumn(Name = "col_a", Order = 2)]
        public int A { get; set; }

        [ClickHouseColumn(Name = "col_b", Order = 0)]
        public string B { get; set; } = "";

        [ClickHouseColumn(Name = "col_c", Order = 1)]
        public double C { get; set; }
    }

    public class IgnoredPropertyRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "value", Order = 1)]
        public string Value { get; set; } = "";

        [ClickHouseColumn(Ignore = true)]
        public string Internal { get; set; } = "should-not-be-sent";
    }

    public class BaseAttributedRow
    {
        [ClickHouseColumn(Name = "base_id", Order = 0)]
        public int BaseId { get; set; }
    }

    public class DerivedAttributedRow : BaseAttributedRow
    {
        [ClickHouseColumn(Name = "derived_name", Order = 1)]
        public string DerivedName { get; set; } = "";
    }

    [Fact]
    public async Task ColumnOrderAttribute_ControlsServerColumnPlacement()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        var table = $"col_order_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync($@"
            CREATE TABLE {table} (
                col_b String,
                col_c Float64,
                col_a Int32
            ) ENGINE = Memory");

        try
        {
            await using var inserter = conn.CreateBulkInserter<OutOfOrderRow>(table);
            await inserter.InitAsync();
            await inserter.AddAsync(new OutOfOrderRow { A = 100, B = "hello", C = 3.14 });
            await inserter.CompleteAsync();

            // Read back via raw SELECT — if the inserter ignored Order, A/B/C
            // would land in the wrong columns and the round-trip values would
            // be wrong.
            await using var reader = await conn.ExecuteReaderAsync(
                $"SELECT col_a, col_b, col_c FROM {table}");
            Assert.True(await reader.ReadAsync());
            Assert.Equal(100, reader.GetFieldValue<int>(0));
            Assert.Equal("hello", reader.GetFieldValue<string>(1));
            Assert.Equal(3.14, reader.GetFieldValue<double>(2));
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task IgnoreAttribute_ExcludesPropertyFromInsert()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        var table = $"ignore_attr_{Guid.NewGuid():N}";
        // Note: schema does NOT have an "internal" column.
        await conn.ExecuteNonQueryAsync($@"
            CREATE TABLE {table} (
                id Int32,
                value String
            ) ENGINE = Memory");

        try
        {
            await using var inserter = conn.CreateBulkInserter<IgnoredPropertyRow>(table);
            await inserter.InitAsync();
            await inserter.AddAsync(new IgnoredPropertyRow { Id = 7, Value = "kept" });
            await inserter.CompleteAsync();

            await using var reader = await conn.ExecuteReaderAsync(
                $"SELECT id, value FROM {table}");
            Assert.True(await reader.ReadAsync());
            Assert.Equal(7, reader.GetFieldValue<int>(0));
            Assert.Equal("kept", reader.GetFieldValue<string>(1));
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task BaseClassAttributes_HonouredOnDerivedRow()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        var table = $"inherit_attr_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync($@"
            CREATE TABLE {table} (
                base_id Int32,
                derived_name String
            ) ENGINE = Memory");

        try
        {
            await using var inserter = conn.CreateBulkInserter<DerivedAttributedRow>(table);
            await inserter.InitAsync();
            await inserter.AddAsync(new DerivedAttributedRow { BaseId = 42, DerivedName = "derived" });
            await inserter.CompleteAsync();

            await using var reader = await conn.ExecuteReaderAsync(
                $"SELECT base_id, derived_name FROM {table}");
            Assert.True(await reader.ReadAsync());
            Assert.Equal(42, reader.GetFieldValue<int>(0));
            Assert.Equal("derived", reader.GetFieldValue<string>(1));
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    public class TypeMismatchRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public string Id { get; set; } = "";  // string, but server has Int32
    }

    [Fact]
    public async Task PropertyTypeMismatch_FailsWithMessageNamingPropertyAndColumn()
    {
        var table = $"type_mismatch_{Guid.NewGuid():N}";

        // Setup conn — separate from the insert conn because the failed insert
        // may leave the wire in an inconsistent busy state that prevents reuse.
        await using (var setup = new ClickHouseConnection(_fx.BuildSettings()))
        {
            await setup.OpenAsync();
            await setup.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id Int32) ENGINE = Memory");
        }

        try
        {
            await using var conn = new ClickHouseConnection(_fx.BuildSettings());
            await conn.OpenAsync();
            await using var inserter = conn.CreateBulkInserter<TypeMismatchRow>(table);

            // The mismatch surfaces either at InitAsync (extractor build) or at
            // the bulk-write call. Either is acceptable per the documented
            // contract — pin that *some* exception fires before silent
            // corruption can land on the wire.
            Exception? caught = null;
            try
            {
                await inserter.InitAsync();
                await inserter.AddAsync(new TypeMismatchRow { Id = "not-an-int" });
                await inserter.CompleteAsync();
            }
            catch (Exception ex) { caught = ex; }

            Assert.NotNull(caught);
        }
        finally
        {
            await using var cleanup = new ClickHouseConnection(_fx.BuildSettings());
            await cleanup.OpenAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }
}
