using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.Mapping;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// End-to-end coverage for <see cref="ClickHouseQueryableExtensions.InsertAsync{T}"/>
/// — the LINQ-side ergonomic that lets callers add records via the same
/// <c>connection.Table&lt;T&gt;()</c> handle they read from. Tables use
/// <c>id</c>/<c>name</c> snake-case column names because the LINQ provider
/// resolves property names to snake_case at translation time; bulk insert is
/// case-insensitive when matching properties to schema, so both paths agree
/// on the same physical column names.
/// </summary>
[Collection("ClickHouse")]
public class QueryableInsertTests
{
    private readonly ClickHouseFixture _fixture;

    public QueryableInsertTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    private const string CreateTableSql = @"
        CREATE TABLE {0} (
            id Int32,
            name String
        ) ENGINE = Memory";

    [Fact]
    public async Task InsertAsync_SingleRow_RoundTrips()
    {
        var tableName = $"test_qinsert_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync(string.Format(CreateTableSql, tableName));

        try
        {
            var table = connection.Table<TestRow>(tableName);
            await table.InsertAsync(new TestRow { Id = 42, Name = "Alice" });

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(1, count);

            var name = await connection.ExecuteScalarAsync<string>($"SELECT name FROM {tableName} WHERE id = 42");
            Assert.Equal("Alice", name);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task InsertAsync_Collection_RoundTrips()
    {
        var tableName = $"test_qinsert_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync(string.Format(CreateTableSql, tableName));

        try
        {
            var rows = new[]
            {
                new TestRow { Id = 1, Name = "Alice" },
                new TestRow { Id = 2, Name = "Bob" },
                new TestRow { Id = 3, Name = "Charlie" },
            };

            await connection.Table<TestRow>(tableName).InsertAsync(rows);

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(3, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task InsertAsync_AsyncEnumerable_RoundTrips()
    {
        var tableName = $"test_qinsert_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync(string.Format(CreateTableSql, tableName));

        try
        {
            await connection.Table<TestRow>(tableName).InsertAsync(StreamRowsAsync());

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(5, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }

        static async IAsyncEnumerable<TestRow> StreamRowsAsync()
        {
            for (var i = 0; i < 5; i++)
            {
                await Task.Yield();
                yield return new TestRow { Id = i, Name = $"row-{i}" };
            }
        }
    }

    [Fact]
    public async Task InsertAsync_ThenLinqRead_OnSameTableHandle()
    {
        // Pin: a queryable obtained from connection.Table<T>(name) should be
        // usable for both writes (InsertAsync) and reads (LINQ + ToListAsync)
        // — the "given a table connection, add a record AND query it"
        // ergonomic the API was designed for.
        var tableName = $"test_qinsert_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync(string.Format(CreateTableSql, tableName));

        try
        {
            var table = connection.Table<TestRow>(tableName);

            await table.InsertAsync(new[]
            {
                new TestRow { Id = 1, Name = "Alice" },
                new TestRow { Id = 2, Name = "Bob" },
            });

            var alice = await table.Where(r => r.Id == 1).ToListAsync();
            Assert.Single(alice);
            Assert.Equal("Alice", alice[0].Name);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task InsertAsync_DataSourceBound_PersistsRowsAndReleasesConnection()
    {
        var tableName = $"test_qinsert_{Guid.NewGuid():N}";

        // Pre-create the table on a separate (non-pooled) connection so the
        // pool's own activity is purely the InsertAsync rent we're measuring.
        await using (var setupConn = new ClickHouseConnection(_fixture.ConnectionString))
        {
            await setupConn.OpenAsync();
            await setupConn.ExecuteNonQueryAsync(string.Format(CreateTableSql, tableName));
        }

        await using var dataSource = new ClickHouseDataSource(_fixture.ConnectionString);

        try
        {
            await dataSource.Table<TestRow>(tableName)
                .InsertAsync(new TestRow { Id = 7, Name = "Pooled" });

            // The connection rented for the insert must not still be busy after
            // disposal — the rent-and-release lifecycle is what makes the
            // data-source-backed table usable from concurrent service code.
            var stats = dataSource.GetStatistics();
            Assert.Equal(0, stats.Busy);

            // Read back through a fresh rent — confirms the row landed.
            await using var verifyConn = await dataSource.OpenConnectionAsync();
            var name = await verifyConn.ExecuteScalarAsync<string>($"SELECT name FROM {tableName} WHERE id = 7");
            Assert.Equal("Pooled", name);
        }
        finally
        {
            await using var teardownConn = new ClickHouseConnection(_fixture.ConnectionString);
            await teardownConn.OpenAsync();
            await teardownConn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task DataSourceTable_LinqRead_StreamsRowsAcrossPoolRent()
    {
        // Sanity-check the data-source-backed read path: a queryable obtained
        // from dataSource.Table<T>() should rent a pooled connection for the
        // lifetime of an enumeration and return it on completion. The pool's
        // Busy counter going back to zero is the externally-observable signal
        // that the rent-per-enumeration lifecycle held together.
        var tableName = $"test_qinsert_{Guid.NewGuid():N}";

        await using (var setupConn = new ClickHouseConnection(_fixture.ConnectionString))
        {
            await setupConn.OpenAsync();
            await setupConn.ExecuteNonQueryAsync(string.Format(CreateTableSql, tableName));
            await setupConn.ExecuteNonQueryAsync(
                $"INSERT INTO {tableName} VALUES (1, 'Alice'), (2, 'Bob')");
        }

        await using var dataSource = new ClickHouseDataSource(_fixture.ConnectionString);

        try
        {
            var rows = await dataSource.Table<TestRow>(tableName)
                .OrderBy(r => r.Id)
                .ToListAsync();

            Assert.Equal(2, rows.Count);
            Assert.Equal("Alice", rows[0].Name);
            Assert.Equal("Bob", rows[1].Name);

            var stats = dataSource.GetStatistics();
            Assert.Equal(0, stats.Busy);
        }
        finally
        {
            await using var teardownConn = new ClickHouseConnection(_fixture.ConnectionString);
            await teardownConn.OpenAsync();
            await teardownConn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    private class TestRow
    {
        // BulkInserter uses the property name verbatim for the INSERT column
        // list, while the LINQ visitor snake-cases property names when emitting
        // SELECT/WHERE. Pin both onto the same physical column via an explicit
        // attribute so a single POCO works for both write and read paths.
        [ClickHouseColumn(Name = "id")] public int Id { get; set; }
        [ClickHouseColumn(Name = "name")] public string Name { get; set; } = string.Empty;
    }
}
