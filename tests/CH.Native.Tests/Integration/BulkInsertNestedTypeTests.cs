using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// End-to-end round-trip for the ClickHouse <c>Nested(...)</c> type as a single wire
/// column — release-prep item 2 (03-nested-writer.md). Exercises the new
/// <c>NestedColumnWriter</c> against a real server, complementing the server-independent
/// <c>NestedColumnWriterTests</c>.
/// </summary>
/// <remarks>
/// ClickHouse flattens a <c>Nested</c> column into parallel <c>n.field Array(T)</c>
/// columns by default (<c>flatten_nested=1</c>), in which case the insert wire never
/// presents a single <c>Nested(...)</c> column and the writer is never selected. Setting
/// <c>flatten_nested=0</c> on the session keeps <c>n</c> a single Nested column, so the
/// insert header types it as <c>Nested(...)</c> and routes through the new writer. The
/// sub-columns <c>n.field</c> remain readable regardless of the setting.
/// </remarks>
[Collection("ClickHouse")]
public class BulkInsertNestedTypeTests
{
    private readonly ClickHouseFixture _fixture;

    public BulkInsertNestedTypeTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    private class NestedColumnRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        // A Nested column value is an object[] of per-field arrays, in field order:
        // here [ string[] keys, int[] values ] for Nested(key String, value Int32).
        [ClickHouseColumn(Name = "n", Order = 1)]
        public object[] N { get; set; } = System.Array.Empty<object>();
    }

    [Fact]
    public async Task BulkInsert_NestedColumn_RoundTrips()
    {
        var tableName = $"test_nested_col_{System.Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Keep Nested as a single column rather than flattening to n.key / n.value,
        // so the insert wire types the column as Nested(...) and selects NestedColumnWriter.
        await connection.ExecuteNonQueryAsync("SET flatten_nested = 0");

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                n Nested(key String, value Int32)
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<NestedColumnRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new NestedColumnRow
            {
                Id = 1,
                N = new object[] { new[] { "a", "b" }, new[] { 10, 20 } },
            });
            await inserter.AddAsync(new NestedColumnRow
            {
                Id = 2,
                N = new object[] { System.Array.Empty<string>(), System.Array.Empty<int>() },
            });
            await inserter.AddAsync(new NestedColumnRow
            {
                Id = 3,
                N = new object[] { new[] { "only" }, new[] { 99 } },
            });

            await inserter.CompleteAsync();

            // Read back on a FRESH connection so a poisoned insert connection can't mask
            // the real cause.
            await using var readConn = new ClickHouseConnection(_fixture.ConnectionString);
            await readConn.OpenAsync();

            var count = await readConn.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(3, count);

            // Read back via the flattened sub-columns (always accessible).
            var results = new List<(int id, string[] keys, int[] values)>();
            await foreach (var row in readConn.QueryStreamAsync(
                $"SELECT id, n.key, n.value FROM {tableName} ORDER BY id"))
            {
                var id = row.GetFieldValue<int>("id");
                var keys = (string[])row.GetFieldValue<object>("n.key");
                var values = (int[])row.GetFieldValue<object>("n.value");
                results.Add((id, keys, values));
            }

            Assert.Equal(3, results.Count);

            Assert.Equal(new[] { "a", "b" }, results[0].keys);
            Assert.Equal(new[] { 10, 20 }, results[0].values);

            Assert.Empty(results[1].keys);
            Assert.Empty(results[1].values);

            Assert.Equal(new[] { "only" }, results[2].keys);
            Assert.Equal(new[] { 99 }, results[2].values);
        }
        finally
        {
            await using var cleanup = new ClickHouseConnection(_fixture.ConnectionString);
            await cleanup.OpenAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task SelectWholeNestedColumn_RoundTrips_AfterBulkInsert()
    {
        // Reading the WHOLE Nested column (SELECT n, not the n.field sub-columns)
        // exercises NestedColumnReader. Before the shared-offsets fix this threw a
        // ClickHouseProtocolException and poisoned the connection.
        var tableName = $"test_nested_whole_{System.Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync("SET flatten_nested = 0");
        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                n Nested(key String, value Int32)
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<NestedColumnRow>(tableName);
            await inserter.InitAsync();
            await inserter.AddAsync(new NestedColumnRow { Id = 1, N = new object[] { new[] { "a", "b" }, new[] { 10, 20 } } });
            await inserter.AddAsync(new NestedColumnRow { Id = 2, N = new object[] { System.Array.Empty<string>(), System.Array.Empty<int>() } });
            await inserter.AddAsync(new NestedColumnRow { Id = 3, N = new object[] { new[] { "only" }, new[] { 99 } } });
            await inserter.CompleteAsync();

            await using var readConn = new ClickHouseConnection(_fixture.ConnectionString);
            await readConn.OpenAsync();
            await readConn.ExecuteNonQueryAsync("SET flatten_nested = 0");

            var rows = new List<(int id, object[] nested)>();
            await foreach (var row in readConn.QueryStreamAsync($"SELECT id, n FROM {tableName} ORDER BY id"))
            {
                rows.Add((row.GetFieldValue<int>("id"), (object[])row.GetFieldValue<object>("n")));
            }

            Assert.Equal(3, rows.Count);

            // n is [ keysArray, valuesArray ] per row, with the correct element CLR types.
            Assert.Equal(new[] { "a", "b" }, (string[])rows[0].nested[0]);
            Assert.Equal(new[] { 10, 20 }, (int[])rows[0].nested[1]);

            Assert.Empty((string[])rows[1].nested[0]);
            Assert.Empty((int[])rows[1].nested[1]);

            Assert.Equal(new[] { "only" }, (string[])rows[2].nested[0]);
            Assert.Equal(new[] { 99 }, (int[])rows[2].nested[1]);

            // The read connection must remain usable (the pre-fix failure poisoned it).
            var stillAlive = await readConn.ExecuteScalarAsync<int>("SELECT 1");
            Assert.Equal(1, stillAlive);
        }
        finally
        {
            await using var cleanup = new ClickHouseConnection(_fixture.ConnectionString);
            await cleanup.OpenAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task SelectWholeNestedColumn_ThreeFieldsMixedTypes_RoundTrips()
    {
        var tableName = $"test_nested_three_{System.Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync("SET flatten_nested = 0");
        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                n Nested(name String, qty Int64, price Float64)
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<ThreeFieldNestedRow>(tableName);
            await inserter.InitAsync();
            await inserter.AddAsync(new ThreeFieldNestedRow
            {
                Id = 1,
                N = new object[] { new[] { "x", "y" }, new[] { 1L, 2L }, new[] { 1.5, 2.5 } },
            });
            await inserter.AddAsync(new ThreeFieldNestedRow
            {
                Id = 2,
                N = new object[] { System.Array.Empty<string>(), System.Array.Empty<long>(), System.Array.Empty<double>() },
            });
            await inserter.CompleteAsync();

            await using var readConn = new ClickHouseConnection(_fixture.ConnectionString);
            await readConn.OpenAsync();
            await readConn.ExecuteNonQueryAsync("SET flatten_nested = 0");

            var rows = new List<object[]>();
            await foreach (var row in readConn.QueryStreamAsync($"SELECT n FROM {tableName} ORDER BY id"))
            {
                rows.Add((object[])row.GetFieldValue<object>("n"));
            }

            Assert.Equal(2, rows.Count);
            Assert.Equal(new[] { "x", "y" }, (string[])rows[0][0]);
            Assert.Equal(new[] { 1L, 2L }, (long[])rows[0][1]);
            Assert.Equal(new[] { 1.5, 2.5 }, (double[])rows[0][2]);
            Assert.Empty((string[])rows[1][0]);
        }
        finally
        {
            await using var cleanup = new ClickHouseConnection(_fixture.ConnectionString);
            await cleanup.OpenAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    private class ThreeFieldNestedRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "n", Order = 1)]
        public object[] N { get; set; } = System.Array.Empty<object>();
    }
}
