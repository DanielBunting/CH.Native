using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.SystemTests.Fixtures;
using CH.Native.SystemTests.Linq.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Linq;

[Collection("LinqFacts")]
[Trait(Categories.Name, Categories.Linq)]
public class StringTranslationTests : IAsyncLifetime
{
    private readonly SingleNodeFixture _node;
    private readonly LinqFactTableFixture _facts;
    private ClickHouseConnection _conn = null!;

    public StringTranslationTests(SingleNodeFixture node, LinqFactTableFixture facts)
    {
        _node = node;
        _facts = facts;
    }

    public async Task InitializeAsync()
    {
        await _facts.EnsureSeededAsync(_node);
        _conn = new ClickHouseConnection(_node.BuildSettings());
        await _conn.OpenAsync();
    }

    public Task DisposeAsync() => _conn.DisposeAsync().AsTask();

    [Fact]
    public async Task String_Contains_TranslatesToLike_OrPositionGreaterZero()
    {
        // The fixture seeds "foobar", "afoo", "barfoo" -- all contain "foo".
        // The implementation may translate to LIKE '%foo%' or position(name, 'foo') > 0;
        // assert by *result rows*, not SQL string.
        var matches = await _conn.Table<LinqFactRow>(_facts.TableName)
            .Where(x => x.Name.Contains("foo"))
            .Select(x => x.Name)
            .ToListAsync();

        var oracle = _facts.Rows
            .Where(r => r.Name.Contains("foo"))
            .Select(r => r.Name)
            .OrderBy(n => n)
            .ToList();

        Assert.Equal(oracle, matches.OrderBy(n => n).ToList());
    }

    [Fact]
    public async Task String_StartsWith_AnchorRespected()
    {
        // "foobar" starts with "foo"; "afoo" does not.
        var matches = await _conn.Table<LinqFactRow>(_facts.TableName)
            .Where(x => x.Name.StartsWith("foo"))
            .Select(x => x.Name)
            .ToListAsync();

        Assert.Contains("foobar", matches);
        Assert.DoesNotContain("afoo", matches);
        Assert.DoesNotContain("barfoo", matches);
    }

    [Fact]
    public async Task String_EndsWith_AnchorRespected()
    {
        var matches = await _conn.Table<LinqFactRow>(_facts.TableName)
            .Where(x => x.Name.EndsWith("bar"))
            .Select(x => x.Name)
            .ToListAsync();

        Assert.Contains("foobar", matches);
        Assert.DoesNotContain("afoo", matches);
    }

    [Fact]
    public async Task String_Compare_OrdinalVsCurrentCulture()
    {
        // ClickHouse compares strings byte-wise. "Café" (UTF-8) round-trips
        // through the LINQ pipeline and matches by ordinal equality. The fixture
        // seeds the literal at multiple rows; pin the round-trip rather than the count.
        var matches = await _conn.Table<LinqFactRow>(_facts.TableName)
            .Where(x => x.Name == "Café")
            .Select(x => x.Name)
            .ToListAsync();

        var oracle = _facts.Rows.Count(r => r.Name == "Café");
        Assert.Equal(oracle, matches.Count);
        Assert.NotEmpty(matches);
        Assert.All(matches, m => Assert.Equal("Café", m));
    }

    [Fact]
    public async Task String_ToLower_Translation()
    {
        // Compare against ToLower("FOOBAR") -- expects server-side lower() to match "foobar".
        var matches = await _conn.Table<LinqFactRow>(_facts.TableName)
            .Where(x => x.Name.ToLower() == "foobar")
            .Select(x => x.Name)
            .ToListAsync();

        Assert.Contains("foobar", matches);
    }
}
