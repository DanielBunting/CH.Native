using System.Reflection;
using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Stress;

/// <summary>
/// Tier 3 stress coverage for the connection-busy gate. Each test exercises a
/// single <see cref="ClickHouseConnection"/> under heavy concurrent contention
/// — the contract is that exactly one operation succeeds at any instant and
/// the other callers see <see cref="ClickHouseConnectionBusyException"/>. The
/// connection must remain poolable at the end of the run; never silently
/// poison the wire.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Stress)]
public class SingleConnectionContentionTests
{
    private readonly SingleNodeFixture _fixture;
    private readonly ITestOutputHelper _output;

    public SingleConnectionContentionTests(SingleNodeFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public async Task SingleConnection_32ParallelQueries_60sSoak()
    {
        await using var connection = new ClickHouseConnection(_fixture.BuildSettings());
        await connection.OpenAsync();

        var duration = TimeSpan.FromSeconds(60);
        var deadline = DateTime.UtcNow + duration;

        long successes = 0;
        long busyThrows = 0;
        var failures = new System.Collections.Concurrent.ConcurrentBag<Exception>();

        const int producers = 32;
        var tasks = Enumerable.Range(0, producers).Select(_ => Task.Run(async () =>
        {
            while (DateTime.UtcNow < deadline)
            {
                try
                {
                    var v = await connection.ExecuteScalarAsync<long>("SELECT count() FROM system.one");
                    if (v == 1) Interlocked.Increment(ref successes);
                }
                catch (ClickHouseConnectionBusyException)
                {
                    Interlocked.Increment(ref busyThrows);
                }
                catch (Exception ex)
                {
                    failures.Add(ex);
                }
            }
        })).ToArray();

        await Task.WhenAll(tasks);

        _output.WriteLine($"successes={successes}, busy-throws={busyThrows}, other-failures={failures.Count}");

        // Contract: at least one producer succeeded; at least one was rejected
        // by the gate (with 32 producers and a 60s soak this is overwhelmingly
        // likely); no protocol corruption; connection stays poolable.
        Assert.True(successes > 0, "No queries completed.");
        Assert.True(busyThrows > 0, "No producer was rejected by the busy gate — gate may be a no-op.");
        Assert.Empty(failures.Where(f => f is ClickHouseProtocolException));
        Assert.False(GetPrivate<bool>(connection, "_protocolFatal"));
        Assert.True((bool)connection.GetType()
            .GetProperty("CanBePooled", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(connection)!);
    }

    [Fact]
    public async Task SingleConnection_MixedQueryAndCancel_NoCorruption()
    {
        await using var connection = new ClickHouseConnection(_fixture.BuildSettings());
        await connection.OpenAsync();

        var duration = TimeSpan.FromSeconds(60);
        var deadline = DateTime.UtcNow + duration;

        long completed = 0;
        long busyThrows = 0;
        long cancelled = 0;
        var failures = new System.Collections.Concurrent.ConcurrentBag<Exception>();

        // 16 producers issuing queries, each with its own cancellation token
        // that may be tripped by a separate canceller pool. Producers and
        // cancellers race on the same single connection.
        var producerTokens = Enumerable.Range(0, 16)
            .Select(_ => new CancellationTokenSource())
            .ToArray();

        var producers = producerTokens.Select(cts => Task.Run(async () =>
        {
            while (DateTime.UtcNow < deadline)
            {
                using var perCallCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token);
                try
                {
                    await connection.ExecuteScalarAsync<long>(
                        "SELECT count() FROM numbers(100000)", cancellationToken: perCallCts.Token);
                    Interlocked.Increment(ref completed);
                }
                catch (ClickHouseConnectionBusyException)
                {
                    Interlocked.Increment(ref busyThrows);
                }
                catch (OperationCanceledException)
                {
                    Interlocked.Increment(ref cancelled);
                }
                catch (Exception ex)
                {
                    failures.Add(ex);
                }
            }
        })).ToArray();

        var cancellers = Enumerable.Range(0, 4).Select(_ => Task.Run(async () =>
        {
            var rng = new Random(Environment.TickCount);
            while (DateTime.UtcNow < deadline)
            {
                await Task.Delay(rng.Next(50, 200));
                var idx = rng.Next(producerTokens.Length);
                try { producerTokens[idx].Cancel(); } catch { /* may be disposed */ }
                // Replace with fresh token so the producer can resume.
                producerTokens[idx] = new CancellationTokenSource();
            }
        })).ToArray();

        await Task.WhenAll(producers.Concat(cancellers));

        foreach (var cts in producerTokens) cts.Dispose();

        _output.WriteLine($"completed={completed}, busy-throws={busyThrows}, cancelled={cancelled}, failures={failures.Count}");

        // Cancellation may legitimately put the wire into a broken state on
        // some races (DrainAfterCancellation timing edge cases) — we accept
        // that the connection might end up unpoolable, but we MUST NOT see
        // ClickHouseProtocolException, which only fires when the read path
        // observes corrupt bytes.
        Assert.Empty(failures.Where(f => f is ClickHouseProtocolException));
        Assert.True(completed > 0, "No queries completed despite 60s of producers.");
    }

    [Fact]
    public async Task SingleConnection_BulkInsertContention()
    {
        await using var connection = new ClickHouseConnection(_fixture.BuildSettings());
        await connection.OpenAsync();

        var table = $"single_conn_bulk_{Guid.NewGuid():N}";
        await connection.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int64, payload String) ENGINE = Memory");

        try
        {
            var duration = TimeSpan.FromSeconds(30);
            var deadline = DateTime.UtcNow + duration;

            long busyThrows = 0;
            long producerSuccesses = 0;
            var failures = new System.Collections.Concurrent.ConcurrentBag<Exception>();

            // Background bulk inserter holds the wire for the duration. Between
            // cycles a producer may win the slot, in which case InitAsync
            // throws busy — that's a feature, not a bug, so retry briefly.
            var inserterTask = Task.Run(async () =>
            {
                long rowsInserted = 0;
                while (DateTime.UtcNow < deadline)
                {
                    BulkInserter<Row>? inserter = null;
                    try
                    {
                        inserter = connection.CreateBulkInserter<Row>(table);
                        try
                        {
                            await inserter.InitAsync();
                        }
                        catch (ClickHouseConnectionBusyException)
                        {
                            // Producer holds the slot; retry next iteration.
                            await Task.Delay(1);
                            continue;
                        }

                        for (int i = 0; i < 50_000 && DateTime.UtcNow < deadline; i++)
                        {
                            await inserter.AddAsync(new Row { Id = rowsInserted++, Payload = "x" });
                        }
                        await inserter.CompleteAsync();
                    }
                    finally
                    {
                        if (inserter is not null) await inserter.DisposeAsync();
                    }
                }
                return rowsInserted;
            });

            // 8 producers attempting queries against the same connection. They
            // should be rejected with ClickHouseConnectionBusyException for the
            // overwhelming majority of attempts (the inserter holds the slot).
            var producers = Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
            {
                while (DateTime.UtcNow < deadline)
                {
                    try
                    {
                        await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {table}");
                        Interlocked.Increment(ref producerSuccesses);
                    }
                    catch (ClickHouseConnectionBusyException)
                    {
                        Interlocked.Increment(ref busyThrows);
                    }
                    catch (Exception ex)
                    {
                        failures.Add(ex);
                    }
                    await Task.Delay(5);
                }
            })).ToArray();

            await Task.WhenAll(producers);
            var totalInserted = await inserterTask;

            _output.WriteLine($"inserted={totalInserted}, producer-successes={producerSuccesses}, busy-throws={busyThrows}, failures={failures.Count}");

            Assert.Empty(failures.Where(f => f is ClickHouseProtocolException));
            Assert.True(busyThrows > 0, "Inserter never blocked any producer — gate is broken.");
            Assert.False(GetPrivate<bool>(connection, "_protocolFatal"));

            // Verify the inserts actually landed in the order of magnitude expected.
            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {table}");
            Assert.True(count > 0, "No rows persisted.");
            Assert.Equal(totalInserted, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    private static T? GetPrivate<T>(object target, string fieldName)
    {
        var field = target.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Field not found: {fieldName}");
        return (T?)field.GetValue(target);
    }

    private class Row
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public long Id { get; set; }
        [ClickHouseColumn(Name = "payload", Order = 1)] public string Payload { get; set; } = string.Empty;
    }
}
