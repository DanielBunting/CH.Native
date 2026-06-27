using System.Runtime.ExceptionServices;
using System.Threading.Channels;
using CH.Native.Connection;

namespace CH.Native.BulkInsert;

/// <summary>
/// Streams a bulk insert across multiple pooled connections ("pipes") in
/// parallel. Rows pushed via <see cref="AddAsync"/> are buffered into a bounded
/// channel and drained by <see cref="DegreeOfParallelism"/> worker tasks, each
/// owning one pooled connection running an independent single-connection
/// <see cref="BulkInserter{T}"/>.
/// </summary>
/// <remarks>
/// <para>
/// Created via <see cref="ClickHouseDataSource.CreateParallelBulkInserterAsync{T}(string,ParallelBulkInsertOptions?,System.Threading.CancellationToken)"/>;
/// the workers are started (connections opened and INSERTs initialised) before
/// the inserter is handed back, so a bad table or schema surfaces immediately.
/// </para>
/// <para>
/// <b>Semantics.</b> Rows are inserted out of input order across workers (fine for
/// MergeTree). A single parallel insert is <b>not atomic</b>: each block commits
/// independently, so a mid-stream failure leaves already-flushed blocks persisted
/// and buffered rows lost. It is <b>not idempotent on retry</b> and supports no
/// deduplication token — see <see cref="ParallelBulkInsertOptions"/>. Call
/// <see cref="CompleteAsync"/> to persist; disposing without completing abandons
/// any un-flushed rows.
/// </para>
/// <para>
/// A single instance is not safe to use from multiple threads concurrently — push
/// rows from one producer.
/// </para>
/// </remarks>
/// <typeparam name="T">The POCO row type. Must be a reference type.</typeparam>
public sealed class ParallelBulkInserter<T> : IAsyncDisposable where T : class
{
    private readonly ClickHouseDataSource _dataSource;
    private readonly string? _database;
    private readonly string _tableName;
    private readonly ParallelBulkInsertOptions _options;
    private readonly Channel<T> _channel;
    private readonly CancellationTokenSource _cts = new();

    // First-in-wins record of worker failures, used to surface the root cause
    // rather than the cascade of cancellations that follows it.
    private readonly object _faultLock = new();
    private readonly List<Exception> _faults = new();

    private readonly long[] _workerCounts;
    private Task[]? _workers;
    private long _rowsWritten;
    private bool _completed;
    private bool _disposed;

    internal ParallelBulkInserter(
        ClickHouseDataSource dataSource,
        string? database,
        string tableName,
        ParallelBulkInsertOptions options)
    {
        _dataSource = dataSource;
        _database = database;
        _tableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        _options = options;
        _workerCounts = new long[options.DegreeOfParallelism];
        _channel = Channel.CreateBounded<T>(new BoundedChannelOptions(options.ResolveChannelCapacity())
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = false,
        });
    }

    /// <summary>
    /// Gets the number of connections the insert is fanned out across.
    /// </summary>
    public int DegreeOfParallelism => _options.DegreeOfParallelism;

    /// <summary>
    /// Gets the total number of rows committed across all workers. Populated once
    /// <see cref="CompleteAsync"/> has completed successfully; 0 before then.
    /// </summary>
    public long RowsWritten => Interlocked.Read(ref _rowsWritten);

    /// <summary>
    /// Opens <see cref="ParallelBulkInsertOptions.DegreeOfParallelism"/> pooled
    /// connections, initialises an INSERT on each, and launches the worker drain
    /// loops. On any failure, every connection opened so far is returned to the
    /// pool before the exception propagates.
    /// </summary>
    internal async Task StartAsync(CancellationToken cancellationToken)
    {
        int n = _options.DegreeOfParallelism;
        var inserters = new BulkInserter<T>?[n];
        var conns = new ClickHouseConnection?[n];

        try
        {
            for (int i = 0; i < n; i++)
            {
                var conn = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
                conns[i] = conn;
                var workerOptions = _options.BuildWorkerOptions(i);
                var inserter = _database is null
                    ? new BulkInserter<T>(conn, _tableName, workerOptions)
                    : new BulkInserter<T>(conn, _database, _tableName, workerOptions);
                inserters[i] = inserter;
                await inserter.InitAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch
        {
            for (int i = 0; i < n; i++)
            {
                if (inserters[i] is { } ins)
                {
                    try { await ins.DisposeAsync().ConfigureAwait(false); }
                    catch { /* finalisation best-effort; the open failure is the real error */ }
                }

                if (conns[i] is { } c)
                    await c.DisposeAsync().ConfigureAwait(false);
            }

            _cts.Dispose();
            throw;
        }

        _workers = new Task[n];
        for (int i = 0; i < n; i++)
        {
            int index = i;
            BulkInserter<T> inserter = inserters[index]!;
            ClickHouseConnection conn = conns[index]!;
            _workers[index] = Task.Run(() => RunWorkerAsync(index, inserter, conn));
        }
    }

    /// <summary>
    /// Pushes a single row. Awaits when the internal channel is full (backpressure).
    /// </summary>
    public async ValueTask AddAsync(T item, CancellationToken cancellationToken = default)
    {
        EnsureUsable();
        try
        {
            await _channel.Writer.WriteAsync(item, cancellationToken).ConfigureAwait(false);
        }
        catch (ChannelClosedException)
        {
            // The workers tore the channel down (a worker faulted, or the inserter
            // was disposed). Surface the worker root cause if there is one.
            cancellationToken.ThrowIfCancellationRequested();
            ThrowRecordedFaultIfAny();
            throw;
        }
    }

    /// <summary>
    /// Pushes every row from a synchronous source.
    /// </summary>
    public async ValueTask AddRangeAsync(IEnumerable<T> items, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(items);
        foreach (var item in items)
            await AddAsync(item, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Pushes every row from an asynchronous source.
    /// </summary>
    public async ValueTask AddRangeStreamingAsync(IAsyncEnumerable<T> items, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(items);
        await foreach (var item in items.WithCancellation(cancellationToken).ConfigureAwait(false))
            await AddAsync(item, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Signals end-of-input, waits for every worker to flush and commit, and
    /// publishes <see cref="RowsWritten"/>. Throws the worker root cause (or an
    /// <see cref="AggregateException"/> when several workers failed) on failure.
    /// </summary>
    public async Task CompleteAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_completed)
            return;
        _completed = true;

        _channel.Writer.TryComplete();

        using var registration = cancellationToken.CanBeCanceled
            ? cancellationToken.Register(static s => ((CancellationTokenSource)s!).Cancel(), _cts)
            : default;

        try
        {
            await Task.WhenAll(_workers!).ConfigureAwait(false);
        }
        catch
        {
            ThrowAggregated(cancellationToken);
            throw; // unreachable; ThrowAggregated always throws
        }

        Interlocked.Exchange(ref _rowsWritten, _workerCounts.Sum());
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;
        _disposed = true;

        if (!_completed)
        {
            // Abandon: stop the workers and let their finally blocks return the
            // connections to the pool. Un-flushed rows are lost (documented).
            _cts.Cancel();
            _channel.Writer.TryComplete();
            if (_workers is not null)
            {
                try { await Task.WhenAll(_workers).ConfigureAwait(false); }
                catch { /* abandonment path: workers fault with cancellation, ignore */ }
            }
        }

        _cts.Dispose();
    }

    private async Task RunWorkerAsync(int index, BulkInserter<T> inserter, ClickHouseConnection conn)
    {
        var token = _cts.Token;
        long count = 0;
        try
        {
            var reader = _channel.Reader;
            while (await reader.WaitToReadAsync(token).ConfigureAwait(false))
            {
                while (reader.TryRead(out var item))
                {
                    await inserter.AddAsync(item, token).ConfigureAwait(false);
                    count++;
                }
            }

            await inserter.CompleteAsync(token).ConfigureAwait(false);
            _workerCounts[index] = count;
        }
        catch (Exception ex)
        {
            RecordFault(ex);
            throw;
        }
        finally
        {
            // The inserter does not own the connection, so dispose both. Disposing a
            // mid-insert inserter finalises the wire and may throw to report lost
            // buffered rows — that's a secondary signal during teardown, so swallow.
            try { await inserter.DisposeAsync().ConfigureAwait(false); }
            catch { /* secondary */ }
            await conn.DisposeAsync().ConfigureAwait(false);
        }
    }

    private void RecordFault(Exception ex)
    {
        lock (_faultLock)
            _faults.Add(ex);

        // Tear the operation down: cancel siblings and unblock the producer. The
        // channel is completed without an error so readers stop via cancellation
        // rather than re-observing this fault as their own.
        _cts.Cancel();
        _channel.Writer.TryComplete();
    }

    private void ThrowRecordedFaultIfAny()
    {
        Exception[] roots;
        lock (_faultLock)
            roots = _faults.Where(static e => e is not OperationCanceledException).ToArray();

        if (roots.Length == 1)
            ExceptionDispatchInfo.Throw(roots[0]);
        if (roots.Length > 1)
            throw new AggregateException(roots);
    }

    [System.Diagnostics.CodeAnalysis.DoesNotReturn]
    private void ThrowAggregated(CancellationToken userToken)
    {
        Exception[] all;
        lock (_faultLock)
            all = _faults.ToArray();

        var roots = all.Where(static e => e is not OperationCanceledException).ToArray();
        if (roots.Length == 1)
            ExceptionDispatchInfo.Throw(roots[0]);
        if (roots.Length > 1)
            throw new AggregateException(roots);

        // Only cancellations were recorded. If the caller cancelled, surface that;
        // otherwise surface the first cancellation we saw.
        userToken.ThrowIfCancellationRequested();
        if (all.Length > 0)
            ExceptionDispatchInfo.Throw(all[0]);
        throw new InvalidOperationException("Parallel bulk insert failed without a recorded cause.");
    }

    private void EnsureUsable()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_completed)
            throw new InvalidOperationException("Cannot add rows after CompleteAsync has been called.");
    }
}
