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
/// </remarks>
public sealed class ParallelBulkInsertOptions
{
    /// <summary>
    /// Number of pooled connections to fan out across. Each one runs an
    /// independent INSERT. Must be at least 1 and must not exceed the
    /// data source's <c>MaxPoolSize</c> (otherwise the inserter cannot rent
    /// enough connections and would deadlock). Default 4.
    /// </summary>
    public int DegreeOfParallelism { get; set; } = 4;

    /// <summary>
    /// Per-worker batch size — the number of rows each worker buffers before
    /// flushing a block to the server. Maps to
    /// <see cref="BulkInsertOptions.BatchSize"/>. Default 10,000.
    /// </summary>
    public int BatchSize { get; set; } = 10_000;

    /// <summary>
    /// Capacity of the bounded channel that feeds the workers. When the channel
    /// is full, <c>AddAsync</c> awaits — bounding memory and applying backpressure
    /// to the producer. When <c>null</c> (default), the capacity is
    /// <see cref="DegreeOfParallelism"/> × <see cref="BatchSize"/> (roughly one
    /// in-flight batch per worker).
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
    /// Resolves the effective channel capacity, defaulting to
    /// <see cref="DegreeOfParallelism"/> × <see cref="BatchSize"/>.
    /// </summary>
    internal int ResolveChannelCapacity() =>
        ChannelCapacity ?? checked(DegreeOfParallelism * BatchSize);

    /// <summary>
    /// Validates these options against the owning data source's pool size.
    /// </summary>
    internal void Validate(int maxPoolSize)
    {
        if (DegreeOfParallelism < 1)
            throw new ArgumentOutOfRangeException(
                nameof(DegreeOfParallelism),
                DegreeOfParallelism,
                $"{nameof(ParallelBulkInsertOptions)}.{nameof(DegreeOfParallelism)} must be at least 1.");

        if (DegreeOfParallelism > maxPoolSize)
            throw new ArgumentException(
                $"{nameof(ParallelBulkInsertOptions)}.{nameof(DegreeOfParallelism)} ({DegreeOfParallelism}) " +
                $"exceeds the data source MaxPoolSize ({maxPoolSize}); the inserter cannot rent enough " +
                $"connections. Lower {nameof(DegreeOfParallelism)} or raise MaxPoolSize.",
                nameof(DegreeOfParallelism));

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
    }

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
