using Apache.Arrow;
using Apache.Arrow.Ipc;
using CH.Native.Connection;
using CH.Native.Data;

namespace CH.Native.Adbc;

/// <summary>
/// An <see cref="IArrowArrayStream"/> that lazily pulls ClickHouse <see cref="TypedBlock"/>s from a
/// query and converts each non-empty block into one Arrow <see cref="RecordBatch"/>. The full result
/// set is never buffered — at most one block is materialised at a time.
/// </summary>
internal sealed class BlockArrowArrayStream : IArrowArrayStream
{
    private readonly ClickHouseConnection _connection;
    private readonly IAsyncEnumerator<TypedBlock> _blocks;
    private TypedBlock? _pending;
    private bool _exhausted;
    private bool _disposed;

    public BlockArrowArrayStream(
        Schema schema,
        ClickHouseConnection connection,
        TypedBlock? pending,
        IAsyncEnumerator<TypedBlock> blocks)
    {
        Schema = schema ?? throw new ArgumentNullException(nameof(schema));
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _blocks = blocks ?? throw new ArgumentNullException(nameof(blocks));
        _pending = pending;
    }

    /// <inheritdoc />
    public Schema Schema { get; }

    /// <inheritdoc />
    public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        // If the caller cancels mid-read, tell the server to stop producing so the in-flight read
        // unblocks; we then surface cancellation as OperationCanceledException below.
        await using var registration = cancellationToken.Register(
            static state => _ = ((ClickHouseConnection)state!).CancelCurrentQueryAsync(CancellationToken.None),
            _connection);

        // Emit a pending (non-empty) first block captured during schema discovery.
        if (_pending is not null)
        {
            var block = _pending;
            _pending = null;
            using (block)
            {
                return BlockRecordBatchConverter.ToRecordBatch(block, Schema);
            }
        }

        // Pull subsequent blocks, skipping any empty ones the server may interleave.
        while (await _blocks.MoveNextAsync().ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var block = _blocks.Current;
            using (block)
            {
                if (block.RowCount == 0)
                    continue;

                return BlockRecordBatchConverter.ToRecordBatch(block, Schema);
            }
        }

        _exhausted = true;
        return null;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;

        TearDownAsync().AsTask().GetAwaiter().GetResult();
    }

    private async ValueTask TearDownAsync()
    {
        try
        {
            _pending?.Dispose();
            _pending = null;

            // If the result was not fully consumed, ask the server to stop sending rather than
            // reading the entire (potentially huge or unbounded) remaining result. After the cancel
            // packet the server closes the stream, so draining to completion is now bounded — and
            // it keeps the underlying connection clean (un-drained bytes would poison its next use).
            if (!_exhausted)
            {
                try
                {
                    await _connection.CancelCurrentQueryAsync(CancellationToken.None).ConfigureAwait(false);
                }
                catch
                {
                    // Best-effort: a closed/faulted connection has nothing to cancel.
                }

                while (await _blocks.MoveNextAsync().ConfigureAwait(false))
                {
                    _blocks.Current.Dispose();
                }
            }
        }
        catch
        {
            // Best-effort drain: any failure here leaves the connection un-poolable, which the
            // pool already handles. Don't mask it as a Dispose throw.
        }
        finally
        {
            await _blocks.DisposeAsync().ConfigureAwait(false);
        }
    }
}
