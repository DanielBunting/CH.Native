namespace CH.Native.Telemetry;

/// <summary>
/// Stream wrapper that increments <see cref="ClickHouseMeter.BytesSentTotal"/>
/// and <see cref="ClickHouseMeter.BytesReceivedTotal"/> for each successful
/// read/write. Sits between the (optionally TLS-upgraded) network stream and
/// the <see cref="System.IO.Pipelines.PipeReader"/>/<see cref="System.IO.Pipelines.PipeWriter"/>
/// so the counters reflect plaintext wire bytes — compressed when ClickHouse
/// compression is enabled, but pre-TLS-encryption either way.
/// </summary>
internal sealed class MeteredNetworkStream : Stream
{
    private readonly Stream _inner;
    private readonly bool _leaveOpen;

    public MeteredNetworkStream(Stream inner, bool leaveOpen = false)
    {
        _inner = inner;
        _leaveOpen = leaveOpen;
    }

    public override bool CanRead => _inner.CanRead;
    public override bool CanSeek => _inner.CanSeek;
    public override bool CanWrite => _inner.CanWrite;
    public override bool CanTimeout => _inner.CanTimeout;
    public override long Length => _inner.Length;

    public override long Position
    {
        get => _inner.Position;
        set => _inner.Position = value;
    }

    public override int ReadTimeout
    {
        get => _inner.ReadTimeout;
        set => _inner.ReadTimeout = value;
    }

    public override int WriteTimeout
    {
        get => _inner.WriteTimeout;
        set => _inner.WriteTimeout = value;
    }

    public override void Flush() => _inner.Flush();

    public override Task FlushAsync(CancellationToken cancellationToken)
        => _inner.FlushAsync(cancellationToken);

    public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);

    public override void SetLength(long value) => _inner.SetLength(value);

    public override int Read(byte[] buffer, int offset, int count)
    {
        var n = _inner.Read(buffer, offset, count);
        if (n > 0) ClickHouseMeter.BytesReceivedTotal.Add(n);
        return n;
    }

    public override int Read(Span<byte> buffer)
    {
        var n = _inner.Read(buffer);
        if (n > 0) ClickHouseMeter.BytesReceivedTotal.Add(n);
        return n;
    }

    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        var n = await _inner.ReadAsync(buffer, offset, count, cancellationToken).ConfigureAwait(false);
        if (n > 0) ClickHouseMeter.BytesReceivedTotal.Add(n);
        return n;
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        var n = await _inner.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
        if (n > 0) ClickHouseMeter.BytesReceivedTotal.Add(n);
        return n;
    }

    public override int ReadByte()
    {
        var b = _inner.ReadByte();
        if (b >= 0) ClickHouseMeter.BytesReceivedTotal.Add(1);
        return b;
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        _inner.Write(buffer, offset, count);
        if (count > 0) ClickHouseMeter.BytesSentTotal.Add(count);
    }

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        _inner.Write(buffer);
        if (buffer.Length > 0) ClickHouseMeter.BytesSentTotal.Add(buffer.Length);
    }

    public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        await _inner.WriteAsync(buffer, offset, count, cancellationToken).ConfigureAwait(false);
        if (count > 0) ClickHouseMeter.BytesSentTotal.Add(count);
    }

    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        await _inner.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
        if (buffer.Length > 0) ClickHouseMeter.BytesSentTotal.Add(buffer.Length);
    }

    public override void WriteByte(byte value)
    {
        _inner.WriteByte(value);
        ClickHouseMeter.BytesSentTotal.Add(1);
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing && !_leaveOpen)
            _inner.Dispose();
        base.Dispose(disposing);
    }

    public override async ValueTask DisposeAsync()
    {
        if (!_leaveOpen)
            await _inner.DisposeAsync().ConfigureAwait(false);
        await base.DisposeAsync().ConfigureAwait(false);
    }
}
