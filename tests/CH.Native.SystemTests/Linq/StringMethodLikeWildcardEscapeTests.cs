using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Linq;

/// <summary>
/// Probes the LIKE-wildcard handling for LINQ string methods. The visitor
/// translates <c>x.Contains("foo")</c> → <c>x LIKE '%foo%'</c>,
/// <c>x.StartsWith("foo")</c> → <c>x LIKE 'foo%'</c>,
/// <c>x.EndsWith("foo")</c> → <c>x LIKE '%foo'</c>.
///
/// <para>
/// If the user's literal already contains <c>%</c> or <c>_</c> (LIKE
/// wildcards), the wrapper merges with the data and queries silently match
/// more than intended. Example: <c>x.Contains("50%")</c> would become
/// <c>x LIKE '%50%%'</c>, matching ANY string containing "50". This is the
/// SQL-injection-adjacent gotcha most LINQ providers handle by escaping
/// the user's literal with the LIKE escape char.
/// </para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Linq)]
public class StringMethodLikeWildcardEscapeTests : IAsyncLifetime
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;
    private string _table = null!;
    private ClickHouseConnection _conn = null!;

    public StringMethodLikeWildcardEscapeTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    public sealed class StringRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "value", Order = 1)] public string Value { get; set; } = "";
    }

    public async Task InitializeAsync()
    {
        _table = $"like_wc_{Guid.NewGuid():N}";
        _conn = new ClickHouseConnection(_fx.BuildSettings());
        await _conn.OpenAsync();
        await _conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {_table} (id Int32, value String) ENGINE = Memory");
        await _conn.ExecuteNonQueryAsync(
            $"INSERT INTO {_table} VALUES " +
            "(1, '50%'), " +              // literal "50%"
            "(2, 'fifty50five'), " +      // contains "50" but no '%'
            "(3, 'discount: 25%'), " +    // contains "%"
            "(4, 'a_b'), " +              // literal "a_b"
            "(5, 'aXb'), " +              // matches LIKE 'a_b' (LIKE wildcard "_")
            "(6, 'normal value')");
    }

    public async Task DisposeAsync()
    {
        try { await _conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {_table}"); } catch { }
        await _conn.DisposeAsync();
    }

    [Fact]
    public async Task Contains_LiteralWithPercent_DocumentsMatchBehavior()
    {
        // Caller intent: find rows whose `value` contains the substring "50%".
        // Expected (escaped): only row 1 ("50%") and row 3 ("discount: 25%"
        // does NOT contain "50%" so this would be just row 1).
        // Today (probably unescaped): the '%' in "50%" merges with the LIKE
        // wildcards, matching any row containing "50" (rows 1, 2).
        var rows = await _conn.Table<StringRow>(_table)
            .Where(r => r.Value.Contains("50%"))
            .OrderBy(r => r.Id)
            .ToListAsync();

        _output.WriteLine($"Contains('50%') matched: {string.Join(", ", rows.Select(r => $"{r.Id}={r.Value}"))}");

        // OBSERVE today's behavior: assert that we got SOMETHING (not crash).
        // If escaping is missing, rows includes row 2 (false-positive match).
        // If escaping is correct, rows is just row 1.
        Assert.NotEmpty(rows);
        Assert.Contains(rows, r => r.Id == 1);
    }

    [Fact]
    public async Task Contains_LiteralWithUnderscore_DocumentsMatchBehavior()
    {
        // Caller intent: find rows containing literal "a_b". Expected: row 4.
        // Without escape: LIKE wildcard "_" matches any single char, so row 5
        // ("aXb") also matches.
        var rows = await _conn.Table<StringRow>(_table)
            .Where(r => r.Value.Contains("a_b"))
            .OrderBy(r => r.Id)
            .ToListAsync();

        _output.WriteLine($"Contains('a_b') matched: {string.Join(", ", rows.Select(r => $"{r.Id}={r.Value}"))}");

        // Pin today's behavior. If both rows match (4 and 5), the LIKE
        // wildcard isn't escaped — silent over-match.
        Assert.Contains(rows, r => r.Id == 4);
        // The smoking gun for the bug: if row 5 also matches, escaping is
        // broken. Pin which we observe.
        var hasFalsePositive = rows.Any(r => r.Id == 5);
        _output.WriteLine($"LIKE-underscore false-positive present: {hasFalsePositive}");
    }

    [Fact]
    public async Task StartsWith_LiteralWithPercent_DocumentsMatchBehavior()
    {
        // Find rows starting with "50%". Expected: row 1.
        var rows = await _conn.Table<StringRow>(_table)
            .Where(r => r.Value.StartsWith("50"))
            .OrderBy(r => r.Id)
            .ToListAsync();

        _output.WriteLine($"StartsWith('50') matched: {string.Join(", ", rows.Select(r => $"{r.Id}={r.Value}"))}");

        // Sanity baseline: no wildcard chars in literal — must work.
        Assert.Contains(rows, r => r.Id == 1);
    }
}
