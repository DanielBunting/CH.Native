using System.Linq;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Bulk-insert coverage ported from the driver: wide tables, SimpleAggregateFunction columns, and POCO
/// shapes that only need a public getter for insert (records, init-only, private-setter, abstract base,
/// no parameterless constructor). CH.Native never constructs the row type on insert, so all shapes work.
/// </summary>
[Collection("ClickHouse")]
public class BulkInsertShapeAndTypeTests
{
    private readonly ClickHouseFixture _fixture;

    public BulkInsertShapeAndTypeTests(ClickHouseFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task WideTable_ManyColumns_Inserts()
    {
        const int columnCount = 1000; // driver uses 3900; 1000 keeps CI fast while exercising the wide path.
        var table = $"wide_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var cols = Enumerable.Range(0, columnCount).Select(i => $"c{i}").ToArray();
        var ddl = string.Join(", ", cols.Select(c => $"{c} Int32"));
        await connection.ExecuteNonQueryAsync($"CREATE TABLE {table} ({ddl}) ENGINE = Memory");
        try
        {
            var row = Enumerable.Range(0, columnCount).Select(i => (object?)i).ToArray();
            await connection.BulkInsertAsync(table, cols, new[] { row });

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {table}");
            Assert.Equal(1, count);
            var last = await connection.ExecuteScalarAsync<int>($"SELECT c{columnCount - 1} FROM {table}");
            Assert.Equal(columnCount - 1, last);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    private sealed class AggRow
    {
        [ClickHouseColumn(Name = "value")]
        public double? Value { get; set; }
    }

    [Fact]
    public async Task SimpleAggregateFunctionColumn_Inserts()
    {
        var table = $"saf_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (value SimpleAggregateFunction(anyLast, Nullable(Float64))) ENGINE = Memory");
        try
        {
            await using var inserter = connection.CreateBulkInserter<AggRow>(table);
            await inserter.InitAsync();
            await inserter.AddAsync(new AggRow { Value = 1.5 });
            await inserter.CompleteAsync();

            var value = await connection.ExecuteScalarAsync<double>($"SELECT value FROM {table}");
            Assert.Equal(1.5, value);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    // --- POCO shapes: insert needs only a public getter -----------------------------------------

    public record PositionalRecord(int Id, string Name);

    private sealed class InitOnlyRow
    {
        public int Id { get; init; }
        public string Name { get; init; } = "";
    }

    private sealed class PrivateSetterRow
    {
        public int Id { get; private set; }
        public string Name { get; private set; } = "";

        public PrivateSetterRow(int id, string name)
        {
            Id = id;
            Name = name;
        }
    }

    [Fact]
    public async Task PositionalRecord_Inserts() =>
        await AssertShapeRoundTripsAsync(new[] { new PositionalRecord(1, "a"), new PositionalRecord(2, "b") });

    [Fact]
    public async Task InitOnlyProperties_Insert() =>
        await AssertShapeRoundTripsAsync(new[]
        {
            new InitOnlyRow { Id = 1, Name = "a" },
            new InitOnlyRow { Id = 2, Name = "b" },
        });

    [Fact]
    public async Task PrivateSetters_Insert() =>
        await AssertShapeRoundTripsAsync(new[] { new PrivateSetterRow(1, "a"), new PrivateSetterRow(2, "b") });

    private async Task AssertShapeRoundTripsAsync<T>(T[] rows)
        where T : class
    {
        var table = $"shape_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync($"CREATE TABLE {table} (Id Int32, Name String) ENGINE = Memory");
        try
        {
            await using var inserter = connection.CreateBulkInserter<T>(table);
            await inserter.InitAsync();
            foreach (var row in rows)
                await inserter.AddAsync(row);
            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {table}");
            Assert.Equal(rows.Length, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }
}
