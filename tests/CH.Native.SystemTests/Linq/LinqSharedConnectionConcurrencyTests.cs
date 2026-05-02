using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Linq;
using CH.Native.SystemTests.Fixtures;
using CH.Native.SystemTests.Linq.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Linq;

/// <summary>
/// Concurrent LINQ queries on a shared <see cref="ClickHouseConnection"/>. The library's
/// documented contract is fast-fail-with-<see cref="ClickHouseConnectionBusyException"/>
/// for concurrent operations on one connection — not transparent serialisation. This
/// test pins that: under contention, callers see deterministic busy errors (never a
/// torn result, never a deadlock), and the connection remains usable for follow-up
/// work after the contention clears.
/// </summary>
[Collection("LinqFacts")]
[Trait(Categories.Name, Categories.Linq)]
public class LinqSharedConnectionConcurrencyTests : IAsyncLifetime
{
    private readonly SingleNodeFixture _node;
    private readonly LinqFactTableFixture _facts;
    private readonly ITestOutputHelper _output;
    private ClickHouseConnection _conn = null!;

    public LinqSharedConnectionConcurrencyTests(SingleNodeFixture node, LinqFactTableFixture facts, ITestOutputHelper output)
    {
        _node = node;
        _facts = facts;
        _output = output;
    }

    public async Task InitializeAsync()
    {
        await _facts.EnsureSeededAsync(_node);
        _conn = new ClickHouseConnection(_node.BuildSettings());
        await _conn.OpenAsync();
    }

    public Task DisposeAsync() => _conn.DisposeAsync().AsTask();

    [Fact]
    public async Task ConcurrentLinqOnSharedConnection_BusyExceptionIsContractAndConnectionRecovers()
    {
        long expectedRaw = await LinqAssertions.ExecuteScalarAsync<long>(
            _conn, $"SELECT count() FROM {_facts.TableName} WHERE amount > 100");
        int expected = (int)expectedRaw;

        const int concurrency = 16;
        var tasks = Enumerable.Range(0, concurrency).Select(_ => Task.Run(async () =>
        {
            try
            {
                var v = await _conn.Table<LinqFactRow>(_facts.TableName)
                    .Where(x => x.Amount > 100).CountAsync();
                return ("ok", v);
            }
            catch (ClickHouseConnectionBusyException)
            {
                return ("busy", -1);
            }
        })).ToArray();

        var results = await Task.WhenAll(tasks);

        int succeeded = results.Count(r => r.Item1 == "ok");
        int busy = results.Count(r => r.Item1 == "busy");
        _output.WriteLine($"succeeded={succeeded}, busy={busy}");

        // Every successful call must return the correct count — busy-exception is
        // the only allowed failure mode (no torn data, no other exception types).
        Assert.Equal(concurrency, succeeded + busy);
        foreach (var (status, value) in results.Where(r => r.Item1 == "ok"))
            Assert.Equal(expected, value);

        // At least one call succeeded — i.e. the queue isn't deadlocked.
        Assert.True(succeeded >= 1, "Expected at least one concurrent call to succeed.");

        // After contention resolves, the connection must accept fresh work.
        var follow = await _conn.ExecuteScalarAsync<int>("SELECT 1");
        Assert.Equal(1, follow);
    }
}
