using System.Diagnostics.CodeAnalysis;
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
/// <see cref="CompleteAsync"/> to persist and to observe failures; disposing
/// without completing abandons any un-flushed rows and does not report worker
/// errors.
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
    private readonly int _degreeOfParallelism;
    private readonly Channel<T> _channel;
    private readonly CancellationTokenSource _cts = new();

    // First-in record of worker failures, used to surface the root cause rather
    // than the cascade of cancellations that follows it.
    private readonly object _faultLock = new();
    private readonly List<Exception> _faults = new();

    private Task[]? _workers;
    private long _rowsWritten;
    private bool _completeStarted;
    private bool _completed;
    private bool _disposed;

    internal ParallelBulkInserter(
        ClickHouseDataSource dataSource,
        string? database,
        string tableName,
        ParallelBulkInsertOptions options,
        int degreeOfParallelism)
    {
        _dataSource = dataSource;
        _database = database;
        _tableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        _options = options;
        _degreeOfParallelism = degreeOfParallelism;
        _channel = Channel.CreateBounded<T>(new BoundedChannelOptions(options.ResolveChannelCapacity(degreeOfParallelism))
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = false,
        });
    }

    /// <summary>
    /// Gets the number of connections the insert is fanned out across.
    /// </summary>
    public int DegreeOfParallelism => _degreeOfParallelism;

    /// <summary>
    /// Gets the number of rows committed across all workers. After a successful
    /// <see cref="CompleteAsync"/> this is the total inserted. After a partial
    /// failure it is a best-effort lower bound — the sum of the workers that
    /// committed before the failure (the insert is not atomic).
    /// </summary>
    public long RowsWritten => Interlocked.Read(ref _rowsWritten);

    /// <summary>
    /// Opens <see cref="DegreeOfParallelism"/> pooled connections concurrently,
    /// initialises an INSERT on each, and launches the worker drain loops. On any
    /// failure, every connection opened so far is returned to the pool before the
    /// exception propagates.
    /// </summary>
    internal async Task StartAsync(CancellationToken cancellationToken)
    {
        int n = _degreeOfParallelism;

        // Open + initialise all workers concurrently so startup costs ~1 round-trip
        // rather than N sequential ones.
        var setupTasks = new Task<WorkerHandle>[n];
        for (int i = 0; i < n; i++)
        {
            int index = i;
            setupTasks[index] = OpenWorkerAsync(index, cancellationToken);
        }

        WorkerHandle[] handles;
        try
        {
            handles = await Task.WhenAll(setupTasks).ConfigureAwait(false);
        }
        catch
        {
            // Task.WhenAll has awaited every task; dispose the ones that opened
            // successfully (the failed ones already disposed their own connection).
            // Disposal is best-effort and must not mask the startup root cause that
            // we re-throw below, so it runs through the exception-safe helper.
            var opened = new List<IAsyncDisposable>();
            foreach (var task in setupTasks)
            {
                if (task.IsCompletedSuccessfully)
                    opened.Add(task.Result);
            }
            await DisposeOpenedWorkersAsync(opened).ConfigureAwait(false);

            _cts.Dispose();
            throw;
        }

        _workers = new Task[n];
        for (int i = 0; i < n; i++)
        {
            WorkerHandle handle = handles[i];
            _workers[i] = Task.Run(() => RunWorkerAsync(handle));
        }
    }

    // Disposes the workers that opened before a sibling's setup faulted. Extracted
    // from the StartAsync failure path so its exception-safety contract can be
    // exercised directly: a throwing DisposeAsync on one worker must neither mask
    // the startup root cause (re-thrown by the caller) nor abort disposal of the
    // remaining workers.
    internal static async ValueTask DisposeOpenedWorkersAsync(IEnumerable<IAsyncDisposable> opened)
    {
        foreach (var worker in opened)
        {
            // Swallow per-worker teardown faults: the startup root cause is the error
            // worth surfacing, and one worker throwing must not strand the rest.
            try { await worker.DisposeAsync().ConfigureAwait(false); }
            catch { /* secondary teardown signal during a failed startup */ }
        }
    }

    private async Task<WorkerHandle> OpenWorkerAsync(int index, CancellationToken cancellationToken)
    {
        var conn = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var workerOptions = _options.BuildWorkerOptions(index);
            var inserter = _database is null
                ? new BulkInserter<T>(conn, _tableName, workerOptions)
                : new BulkInserter<T>(conn, _database, _tableName, workerOptions);
            await inserter.InitAsync(cancellationToken).ConfigureAwait(false);
            return new WorkerHandle(index, inserter, conn);
        }
        catch
        {
            // Return the connection before propagating; the inserter never claimed
            // a busy slot it didn't release (InitAsync releases on failure).
            await conn.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Pushes a single row. Awaits when the internal channel is full (backpressure).
    /// </summary>
    public async ValueTask AddAsync(T item, CancellationToken cancellationToken = default)
    {
        EnsureUsable();

        // Fast path: room in the channel, no awaiting and no per-row async-state
        // allocation. The slow path is only taken under backpressure.
        if (_channel.Writer.TryWrite(item))
            return;

        await AddSlowAsync(item, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask AddSlowAsync(T item, CancellationToken cancellationToken)
    {
        var writer = _channel.Writer;
        try
        {
            // One awaitable amortizes across the many rows that drain before the
            // next stall, instead of allocating per row on WriteAsync.
            while (await writer.WaitToWriteAsync(cancellationToken).ConfigureAwait(false))
            {
                if (writer.TryWrite(item))
                    return;
            }
        }
        catch (ChannelClosedException)
        {
            // fall through to the shared surfacing below
        }

        // The writer was completed without accepting the row: the workers tore the
        // channel down (a worker faulted, or the inserter was disposed). Surface
        // the worker root cause if there is one.
        cancellationToken.ThrowIfCancellationRequested();
        ThrowRootFaultIfAny();
        throw new ChannelClosedException();
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
    /// A second call after a failed completion throws rather than reporting success.
    /// </summary>
    public async Task CompleteAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_completed)
            return;
        if (_completeStarted)
            throw new InvalidOperationException(
                "CompleteAsync has already been called; if it failed, create a new inserter to retry.");
        _completeStarted = true;

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
            ThrowRootFaultIfAny();                       // a real worker fault, if any
            cancellationToken.ThrowIfCancellationRequested(); // else the caller cancelled
            ThrowCancellationFallback();                 // [DoesNotReturn]
        }

        // Only reached when every worker committed.
        _completed = true;
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
            // connections to the pool. Un-flushed rows are lost and worker errors
            // are NOT reported here — callers observe failures via CompleteAsync.
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

    private async Task RunWorkerAsync(WorkerHandle handle)
    {
        var token = _cts.Token;
        var inserter = handle.Inserter;
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
            // Publish best-effort so RowsWritten reflects committed workers even
            // when a sibling later faults.
            Interlocked.Add(ref _rowsWritten, count);
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
            await handle.Connection.DisposeAsync().ConfigureAwait(false);
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

    /// <summary>
    /// Throws the recorded root cause (a single fault, or an
    /// <see cref="AggregateException"/> for several), ignoring cancellations that
    /// are this inserter's own teardown cascade. Returns without throwing when the
    /// only recorded faults are that cascade.
    /// </summary>
    private void ThrowRootFaultIfAny()
    {
        Exception? single = null;
        List<Exception>? many = null;
        lock (_faultLock)
        {
            foreach (var ex in _faults)
            {
                if (IsCascadeCancellation(ex))
                    continue;

                if (single is null)
                {
                    single = ex;
                }
                else
                {
                    many ??= new List<Exception> { single };
                    many.Add(ex);
                }
            }
        }

        if (many is not null)
            throw new AggregateException(many);
        if (single is not null)
            ExceptionDispatchInfo.Throw(single);
    }

    [DoesNotReturn]
    private void ThrowCancellationFallback()
    {
        // Only this inserter's teardown cancellations were recorded and the caller
        // did not cancel: surface the first one rather than swallowing the failure.
        Exception? first = null;
        lock (_faultLock)
        {
            if (_faults.Count > 0)
                first = _faults[0];
        }

        if (first is not null)
            ExceptionDispatchInfo.Throw(first);
        throw new InvalidOperationException("Parallel bulk insert failed without a recorded cause.");
    }

    // A cancellation is "cascade" (a teardown side effect, not the root cause) only
    // when it carries THIS inserter's token. A server-origin OperationCanceledException
    // / TaskCanceledException carries a different token and is treated as a real fault.
    private bool IsCascadeCancellation(Exception ex) =>
        ex is OperationCanceledException oce && oce.CancellationToken == _cts.Token;

    private void EnsureUsable()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_completeStarted)
            throw new InvalidOperationException("Cannot add rows after CompleteAsync has been called.");
    }

    // Bundles a worker's connection + inserter so the startup/teardown paths can
    // pass them around together.
    private readonly record struct WorkerHandle(int Index, BulkInserter<T> Inserter, ClickHouseConnection Connection)
        : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            try { await Inserter.DisposeAsync().ConfigureAwait(false); }
            catch { /* finalisation best-effort; the open failure is the real error */ }
            await Connection.DisposeAsync().ConfigureAwait(false);
        }
    }
}
