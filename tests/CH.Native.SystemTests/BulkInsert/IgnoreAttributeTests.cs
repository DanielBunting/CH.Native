using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.BulkInsertFailures.Helpers;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.BulkInsertFailures;

/// <summary>
/// Pre-fix the new <see cref="ClickHouseColumnAttribute"/> had no <c>Ignore</c>
/// property; only the deprecated <c>ColumnAttribute</c> supported it.
/// Migrating to the new attribute silently lost the ability to skip transient
/// properties, so they leaked into <see cref="MapPropertiesToSchema"/> and
/// either bound to the wrong column by name match or threw on missing column.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class IgnoreAttributeTests
{
    private readonly SingleNodeFixture _fx;

    public IgnoreAttributeTests(SingleNodeFixture fx) => _fx = fx;

    [Fact]
    public async Task IgnoredProperty_NotMappedToSchema_RoundTripSucceeds()
    {
        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fx.BuildSettings());

        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        await using (var inserter = conn.CreateBulkInserter<RowWithIgnoredField>(harness.TableName))
        {
            await inserter.InitAsync();
            await inserter.AddAsync(new RowWithIgnoredField
            {
                Id = 1,
                Payload = "hello",
                ComputedNotInTable = 999,
            });
            await inserter.CompleteAsync();
        }

        var stored = await conn.ExecuteScalarAsync<string>(
            $"SELECT payload FROM {harness.TableName} WHERE id = 1");
        Assert.Equal("hello", stored);
    }

    private sealed class RowWithIgnoredField
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "payload", Order = 1)] public string Payload { get; set; } = "";

        // No matching column in the (id Int32, payload String) harness table.
        // Pre-fix this would either bind to a phantom column (failing schema
        // mapping) or throw "column not found".
        [ClickHouseColumn(Ignore = true)] public int ComputedNotInTable { get; set; }
    }
}
