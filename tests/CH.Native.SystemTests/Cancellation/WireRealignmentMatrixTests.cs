using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using CH.Native.SystemTests.Helpers;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Cancellation;

/// <summary>
/// The failure-mode × entry-point matrix. Every cell applies one failure mode to
/// one query entry point and asserts the ONE universal postcondition
/// (<see cref="WireAssertions.AssertReusableOrCleanlyBrokenAsync"/>): the
/// connection is either reusable with a CORRECT distinctive result, or cleanly
/// broken — never silently wrong, never hung.
///
/// <para>Historically each of these cells was covered (or not) by a bespoke test,
/// and every uncovered cell hid a real bug: RST×anything re-pooled dead sockets,
/// cancel×typed corrupted the next query, timeout×reader returned the previous
/// query's values. The matrix makes the coverage systematic — a new entry point
/// or failure mode gets a row/column, not a hand-written scenario.</para>
/// </summary>
[Collection("Toxiproxy")]
[Trait(Categories.Name, Categories.Cancellation)]
[Trait(Categories.Name, Categories.RaceSensitive)]
public sealed class WireRealignmentMatrixTests : IAsyncLifetime
{
    private readonly ToxiproxyFixture _proxy;
    private readonly ITestOutputHelper _output;

    public WireRealignmentMatrixTests(ToxiproxyFixture proxy, ITestOutputHelper output)
    {
        _proxy = proxy;
        _output = output;
    }

    public Task InitializeAsync() => _proxy.ResetProxyAsync();
    public Task DisposeAsync() => _proxy.ResetProxyAsync();

    public enum Failure
    {
        /// <summary>TCP RST mid-response (server kill, LB reset). reset_peer toxic.</summary>
        ResetPeer,
        /// <summary>Connection closed after N bytes of response (truncation). limit_data toxic.</summary>
        TruncatedClose,
        /// <summary>Caller cancels mid-stream while responses are delayed. latency toxic + 300ms token.</summary>
        CancelMidStream,
    }

    public enum EntryPoint
    {
        Scalar,
        NonQuery,
        Reader,
        TypedStream,
        BulkInsert,
    }

    public static TheoryData<Failure, EntryPoint> Cells()
    {
        var data = new TheoryData<Failure, EntryPoint>();
        foreach (var f in Enum.GetValues<Failure>())
            foreach (var e in Enum.GetValues<EntryPoint>())
                data.Add(f, e);
        return data;
    }

    [Theory]
    [MemberData(nameof(Cells))]
    public async Task Cell_FailureThenProbe_ReusableWithCorrectResultOrCleanlyBroken(Failure failure, EntryPoint entry)
    {
        // Distinctive per-cell sentinel so a stale-bytes read can never
        // accidentally satisfy the probe.
        var sentinel = 1000 + (int)failure * 100 + (int)entry;

        await using var conn = new ClickHouseConnection(_proxy.BuildSettings());
        await conn.OpenAsync();

        // Bulk needs a table; create it BEFORE the toxic so setup is clean.
        string? bulkTable = null;
        if (entry == EntryPoint.BulkInsert)
        {
            bulkTable = $"matrix_bulk_{Guid.NewGuid():N}";
            await conn.ExecuteNonQueryAsync(
                $"CREATE TABLE {bulkTable} (id Int32, payload String) ENGINE = Memory");
        }

        using var cts = new CancellationTokenSource();
        switch (failure)
        {
            case Failure.ResetPeer:
                // RST all connections 150ms after the query starts flowing.
                await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "reset_peer", "downstream",
                    new() { ["timeout"] = 150 });
                break;
            case Failure.TruncatedClose:
                // Close after 4KB of response — mid-stream truncation.
                await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "limit_data", "downstream",
                    new() { ["bytes"] = 4096 });
                break;
            case Failure.CancelMidStream:
                // Slow the response so the 300ms token reliably fires mid-stream.
                await _proxy.Client.AddToxicAsync(ToxiproxyFixture.ProxyName, "latency", "downstream",
                    new() { ["latency"] = 800, ["jitter"] = 0 });
                cts.CancelAfter(TimeSpan.FromMilliseconds(300));
                break;
        }

        Exception? observed = null;
        try
        {
            // Each entry point streams enough data that the failure lands mid-response.
            const string bigQuery = "SELECT number FROM system.numbers LIMIT 3000000";
            switch (entry)
            {
                case EntryPoint.Scalar:
                    await conn.ExecuteScalarAsync<ulong>(
                        "SELECT count() FROM (SELECT number FROM system.numbers LIMIT 3000000)",
                        cancellationToken: cts.Token);
                    break;
                case EntryPoint.NonQuery:
                    await conn.ExecuteNonQueryAsync(bigQuery, cancellationToken: cts.Token);
                    break;
                case EntryPoint.Reader:
                    await using (var reader = await conn.ExecuteReaderAsync(bigQuery, cts.Token))
                    {
                        while (await reader.ReadAsync(cts.Token)) { }
                    }
                    break;
                case EntryPoint.TypedStream:
                    await foreach (var _ in conn.QueryTypedAsync<ulong>(bigQuery, cts.Token)) { }
                    break;
                case EntryPoint.BulkInsert:
                    await using (var inserter = conn.CreateBulkInserter<MatrixRow>(bulkTable!,
                        new BulkInsertOptions { BatchSize = 500 }))
                    {
                        await inserter.InitAsync(cts.Token);
                        for (int i = 0; i < 100_000; i++)
                            await inserter.AddAsync(new MatrixRow { Id = i, Payload = "x" }, cts.Token);
                        await inserter.CompleteAsync(cts.Token);
                    }
                    break;
            }
        }
        catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
        {
            observed = ex;
        }

        // Remove the fault before probing: the postcondition is about the
        // CONNECTION's state, not the network's.
        await _proxy.Client.RemoveAllToxicsAsync(ToxiproxyFixture.ProxyName);

        _output.WriteLine(
            $"[{failure}×{entry}] op outcome: {(observed is null ? "completed" : observed.GetType().Name)}");

        // A completed op must leave the wire idle; a failed op must satisfy the
        // universal contract. (Some cells CAN legitimately complete — e.g. a
        // truncation cap larger than the response — the postcondition holds
        // either way, which is the point of a matrix over bespoke scenarios.)
        var reusable = await WireAssertions.AssertReusableOrCleanlyBrokenAsync(conn, sentinel);
        _output.WriteLine($"[{failure}×{entry}] postcondition: {(reusable ? "reusable+correct" : "cleanly broken")}");
    }

    private sealed class MatrixRow
    {
        [Mapping.ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [Mapping.ClickHouseColumn(Name = "payload", Order = 1)] public string Payload { get; set; } = "";
    }
}
