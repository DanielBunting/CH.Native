using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SmokeTests.Fixtures;
using CH.Native.SmokeTests.Helpers;
using Xunit;

namespace CH.Native.SmokeTests.Query;

[Collection("SmokeTest")]
public class BulkInsertSmokeTests
{
    private readonly SmokeTestFixture _fixture;

    public BulkInsertSmokeTests(SmokeTestFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task BulkInsert_ArrayOfInt32_RoundTrips()
    {
        var table = $"smoke_bulk_{Guid.NewGuid():N}";
        try
        {
            await using var connection = new ClickHouseConnection(_fixture.NativeConnectionString);
            await connection.OpenAsync();

            await connection.ExecuteNonQueryAsync($@"
                CREATE TABLE {table} (
                    id Int32,
                    values Array(Int32)
                ) ENGINE = Memory");

            await using var inserter = connection.CreateBulkInserter<ArrayRow>(table);
            await inserter.InitAsync();

            await inserter.AddAsync(new ArrayRow { Id = 1, Values = new[] { 10, 20, 30 } });
            await inserter.AddAsync(new ArrayRow { Id = 2, Values = Array.Empty<int>() });
            await inserter.AddAsync(new ArrayRow { Id = 3, Values = new[] { 42 } });

            await inserter.CompleteAsync();

            // Cross-validate: read via CH.Native and ClickHouse.Driver
            var native = await NativeQueryHelper.QueryAsync(
                _fixture.NativeConnectionString,
                $"SELECT id, values FROM {table} ORDER BY id");

            var driver = await DriverQueryHelper.QueryAsync(
                _fixture.DriverConnectionString,
                $"SELECT id, values FROM {table} ORDER BY id");

            Assert.Equal(3, native.Count);
            ResultComparer.AssertResultsEqual(native, driver, "BulkInsert Array(Int32)");
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    #region Test POCOs

    private class ArrayRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "values", Order = 1)]
        public int[] Values { get; set; } = Array.Empty<int>();
    }

    #endregion
}
