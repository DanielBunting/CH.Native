using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Linq;

/// <summary>
/// Pins the documented cursor-pagination good-pattern: <c>WHERE (ts, id) >
/// (@cursor_ts, @cursor_id) ORDER BY ts, id LIMIT N</c>. Use-cases §8.8 calls
/// out the <c>OFFSET 1_000_000</c> anti-pattern — this test does not lock
/// that out, but pins that the recommended pattern produces no duplicates,
/// no skips, and stable ordering across N pages.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Linq)]
public class CursorPaginationContractTests : IAsyncLifetime
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;
    private readonly string _table = $"events_cursor_{Guid.NewGuid():N}";

    public CursorPaginationContractTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    public async Task InitializeAsync()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {_table} (ts DateTime, id Int32, body String) " +
            "ENGINE = MergeTree ORDER BY (ts, id)");

        // 250 rows. Bunch some with intentionally-equal ts so the (ts,id)
        // tuple cursor must use id as tiebreaker.
        var values = new List<string>(250);
        var t0 = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        for (int i = 0; i < 250; i++)
        {
            // Round timestamps to chunks of 5 so multiple ids share each ts.
            var ts = t0.AddSeconds(i / 5);
            values.Add($"('{ts:yyyy-MM-dd HH:mm:ss}', {i}, 'b{i}')");
        }
        await conn.ExecuteNonQueryAsync($"INSERT INTO {_table} VALUES {string.Join(",", values)}");
    }

    public async Task DisposeAsync()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {_table}");
    }

    [Fact]
    public async Task RawSqlCursorPagination_NoDuplicates_NoSkips_AcrossThreePages()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        const int pageSize = 100;
        var seen = new HashSet<int>();
        DateTime? cursorTs = null;
        int? cursorId = null;

        for (int page = 0; page < 3; page++)
        {
            string whereClause = cursorTs is null
                ? string.Empty
                : $"WHERE (ts, id) > (toDateTime('{cursorTs:yyyy-MM-dd HH:mm:ss}'), {cursorId})";

            var sql = $"SELECT ts, id, body FROM {_table} {whereClause} ORDER BY ts, id LIMIT {pageSize}";

            var rows = new List<EventRow>();
            await foreach (var row in conn.QueryAsync<EventRow>(sql))
                rows.Add(row);

            if (rows.Count == 0) break;
            _output.WriteLine($"Page {page}: {rows.Count} rows, ids {rows.First().Id}..{rows.Last().Id}");

            foreach (var r in rows)
            {
                Assert.True(seen.Add(r.Id), $"Duplicate id {r.Id} on page {page}");
            }

            cursorTs = rows[^1].Ts;
            cursorId = rows[^1].Id;
        }

        // 250 rows over three 100-row pages: pages 0..1 fill, page 2 has 50.
        Assert.Equal(250, seen.Count);
        Assert.Equal(Enumerable.Range(0, 250), seen.OrderBy(x => x));
    }

    [Fact]
    public async Task LinqCompoundCursor_DuplicateTimestampsHandledByIdTiebreaker()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        // Pick a cursor right at a duplicate-ts boundary: id=4 has the same
        // ts as id=0..4 (because ts = i/5). Cursor at (ts0, 2) should
        // return id 3, 4, then advance.
        var t0 = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var cursorTs = t0;
        var cursorId = 2;

        var rows = await conn.Table<EventRow>(_table)
            .Where(r => r.Ts > cursorTs || (r.Ts == cursorTs && r.Id > cursorId))
            .OrderBy(r => r.Ts).ThenBy(r => r.Id)
            .Take(10)
            .ToListAsync();

        Assert.Equal(10, rows.Count);
        Assert.Equal(3, rows.First().Id);
        Assert.True(rows.Last().Id > 3);
        // Strict ascending after the cursor.
        Assert.Equal(rows.OrderBy(r => r.Ts).ThenBy(r => r.Id).ToList(), rows);
    }

    internal sealed class EventRow
    {
        [ClickHouseColumn(Name = "ts")] public DateTime Ts { get; set; }
        [ClickHouseColumn(Name = "id")] public int Id { get; set; }
        [ClickHouseColumn(Name = "body")] public string Body { get; set; } = "";
    }
}
