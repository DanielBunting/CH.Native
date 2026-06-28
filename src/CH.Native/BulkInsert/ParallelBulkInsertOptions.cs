namespace CH.Native.BulkInsert;

/// <summary>
/// Options for a parallel (multi-connection) bulk insert driven by
/// <see cref="ParallelBulkInserter{T}"/>. Rows are fanned out across
/// <see cref="DegreeOfParallelism"/> pooled connections, each running an
/// independent single-connection <see cref="BulkInserter{T}"/>.
/// </summary>
/// <remarks>
/// <para>
/// Deliberately exposes <b>no <c>DeduplicationToken</c></b>. ClickHouse derives a
/// per-block dedup identity from the token plus the block's sequence number, not
/// from the block contents. The work-stealing fan-out used here composes blocks
/// non-deterministically across workers and across retries, so a token cannot be
/// made sound: sharing one token silently discards every worker's first block as a
/// duplicate, and suffixing it per worker still gives no retry safety. A single
/// parallel insert is therefore <b>not atomic</b> and <b>not idempotent on retry</b>.
/// Callers that need idempotency handle it above the inserter — load into a staging
/// table and atomic-swap on success, use an idempotent engine such as
/// <c>ReplacingMergeTree</c>, or fall back to a single-connection
/// <see cref="BulkInserter{T}"/> with a <see cref="BulkInsertOptions.DeduplicationToken"/>
/// (which stays sound on one ordered stream).
/// </para>
/// <para>
/// Several other <see cref="BulkInsertOptions"/> members are also intentionally
/// not surfaced here — the parallel path is a deliberately minimal, throughput-first
/// surface, and each worker runs with the high-throughput defaults:
/// <list type="bullet">
/// <item><description>
/// <c>ColumnTypes</c> — per-column type overrides aren't exposed because the column
/// schema is resolved independently per worker from the target table; the fan-out has
/// no single place to apply a caller-supplied type map soundly.
/// </description></item>
/// <item><description>
/// <c>IncludeNullColumns</c>, <c>PreferDirectStreaming</c>, <c>UsePooledArrays</c> —
/// these are left fixed at their <see cref="BulkInsertOptions"/> defaults
/// (<c>true</c> for all three), the fastest, lowest-allocation settings. They exist
/// on the single-connection path as escape hatches; the parallel path keeps them
/// pinned so every worker behaves identically and the surface stays small.
/// </description></item>
/// </list>
/// Callers needing any of these knobs use a single-connection
/// <see cref="BulkInserter{T}"/> with a full <see cref="BulkInsertOptions"/>.
/// </para>
/// </remarks>
public sealed class ParallelBulkInsertOptions
{
    /// <summary>The nominal default degree of parallelism when none is requested.</summary>
    private const int DefaultDegreeOfParallelism = 4;

    /// <summary>
    /// Number of pooled connections to fan out across. Each one runs an
    /// independent INSERT. When <c>null</c> (the default) the effective value is
    /// <c>min(4, MaxPoolSize)</c>, so the default never exceeds a small pool. When
    /// set explicitly it must be at least 1 and must not exceed the data source's
    /// <c>MaxPoolSize</c> (otherwise the inserter cannot rent enough connections).
    /// </summary>
    public int? DegreeOfParallelism { get; set; }

    /// <summary>
    /// Per-worker batch size — the number of rows each worker buffers before
    /// flushing a block to the server. Maps to
    /// <see cref="BulkInsertOptions.BatchSize"/>. Default 10,000.
    /// </summary>
    public int BatchSize { get; set; } = 10_000;

    /// <summary>
    /// Capacity of the bounded channel that feeds the workers. When the channel
    /// is full, <c>AddAsync</c> awaits — bounding memory and applying backpressure
    /// to the producer. When <c>null</c> (default), the capacity is the effective
    /// <see cref="DegreeOfParallelism"/> × <see cref="BatchSize"/> (roughly one
    /// in-flight batch per worker), clamped to <see cref="int.MaxValue"/>.
    /// </summary>
    public int? ChannelCapacity { get; set; }

    /// <summary>
    /// Roles to activate for every worker INSERT. Maps to
    /// <see cref="BulkInsertOptions.Roles"/>. <c>null</c> inherits the connection
    /// default; an empty list strips all roles.
    /// </summary>
    public IList<string>? Roles { get; set; }

    /// <summary>
    /// Per-worker schema-cache override. Maps to
    /// <see cref="BulkInsertOptions.UseSchemaCache"/>. <c>null</c> inherits the
    /// connection default.
    /// </summary>
    public bool? UseSchemaCache { get; set; }

    /// <summary>
    /// Base query ID. When non-empty, each worker's INSERT is tagged with
    /// <c>{QueryId}-p{workerIndex}</c> so the server sees a distinct query ID per
    /// connection. When <c>null</c> or empty, the driver generates a GUID per worker.
    /// </summary>
    public string? QueryId { get; set; }

    /// <summary>
    /// Gets the default options instance.
    /// </summary>
    public static ParallelBulkInsertOptions Default { get; } = new();

    /// <summary>
    /// Validates these options against the owning data source's pool size and
    /// returns the effective degree of parallelism. A <c>null</c>
    /// <see cref="DegreeOfParallelism"/> resolves to <c>min(4, maxPoolSize)</c> so
    /// the default is usable on a small pool; an explicit value that exceeds
    /// <paramref name="maxPoolSize"/> throws, since the inserter could never rent
    /// that many connections.
    /// </summary>
    internal int Resolve(int maxPoolSize)
    {
        if (BatchSize < 1)
            throw new ArgumentOutOfRangeException(
                nameof(BatchSize),
                BatchSize,
                $"{nameof(ParallelBulkInsertOptions)}.{nameof(BatchSize)} must be at least 1.");

        if (ChannelCapacity is < 1)
            throw new ArgumentOutOfRangeException(
                nameof(ChannelCapacity),
                ChannelCapacity,
                $"{nameof(ParallelBulkInsertOptions)}.{nameof(ChannelCapacity)} must be at least 1 when set.");

        if (DegreeOfParallelism is { } degree)
        {
            if (degree < 1)
                throw new ArgumentOutOfRangeException(
                    nameof(DegreeOfParallelism),
                    degree,
                    $"{nameof(ParallelBulkInsertOptions)}.{nameof(DegreeOfParallelism)} must be at least 1.");

            if (degree > maxPoolSize)
                throw new ArgumentException(
                    $"{nameof(ParallelBulkInsertOptions)}.{nameof(DegreeOfParallelism)} ({degree}) " +
                    $"exceeds the data source MaxPoolSize ({maxPoolSize}); the inserter cannot rent enough " +
                    $"connections. Lower {nameof(DegreeOfParallelism)} or raise MaxPoolSize.",
                    nameof(DegreeOfParallelism));

            return degree;
        }

        return Math.Min(DefaultDegreeOfParallelism, maxPoolSize);
    }

    /// <summary>
    /// Resolves the effective channel capacity for the given (already-resolved)
    /// degree of parallelism, defaulting to <c>degree × BatchSize</c> clamped to
    /// <see cref="int.MaxValue"/> so a large-but-individually-valid pair cannot
    /// overflow.
    /// </summary>
    internal int ResolveChannelCapacity(int degreeOfParallelism) =>
        ChannelCapacity ?? (int)Math.Min((long)degreeOfParallelism * BatchSize, int.MaxValue);

    /// <summary>
    /// Builds the per-worker <see cref="BulkInsertOptions"/>. Note that
    /// <see cref="BulkInsertOptions.DeduplicationToken"/> is deliberately never
    /// set — see the type remarks.
    /// </summary>
    internal BulkInsertOptions BuildWorkerOptions(int workerIndex) => new()
    {
        BatchSize = BatchSize,
        Roles = Roles,
        UseSchemaCache = UseSchemaCache,
        QueryId = string.IsNullOrEmpty(QueryId) ? null : $"{QueryId}-p{workerIndex}",
    };
}
