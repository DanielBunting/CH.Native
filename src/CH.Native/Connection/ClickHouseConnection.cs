using System.Buffers;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using CH.Native.Ado;
using CH.Native.BulkInsert;
using CH.Native.Commands;
using CH.Native.Compression;
using CH.Native.Data;
using CH.Native.Exceptions;
using CH.Native.Parameters;
using CH.Native.Protocol;
using CH.Native.Protocol.Messages;
using CH.Native.Resilience;
using CH.Native.Results;
using CH.Native.Sql;
using CH.Native.Telemetry;

namespace CH.Native.Connection;

/// <summary>
/// Represents a connection to a ClickHouse server using the native protocol.
/// Also implements <see cref="DbConnection"/> so the same instance flows through
/// ADO.NET-facing consumers (Dapper, EF Core's <see cref="IDbConnection"/>
/// extension points, OpenTelemetry SqlClient instrumentation) without an
/// intermediate wrapper. The native API (<see cref="ExecuteScalarAsync{T}(string, IProgress{QueryProgress}?, CancellationToken, string?)"/>,
/// <see cref="QueryStreamAsync{T}(string,CancellationToken,string?)"/>, etc.) is unchanged.
/// </summary>
public sealed class ClickHouseConnection : DbConnection
{
    // Nullable to allow the parameterless ctor + property-set ConnectionString
    // pattern that DbProviderFactory / EF Core's IDbConnectionFactory expect.
    // Reads outside the explicit pre-open accessors (Settings/Database/DataSource/
    // ConnectionString.get) happen after OpenAsync has thrown if settings is
    // still null — see OpenAsync's RequireSettings() guard.
    private ClickHouseConnectionSettings? _settings;
    // Re-initialized whenever ConnectionString is set so the registry tracks
    // the new settings' StringMaterialization mode. Defaults to the standard
    // registry so the field is never null even before ConnectionString is set.
    private ColumnReaderRegistry _columnReaderRegistry = ColumnReaderRegistry.Default;
    private readonly AsyncLocal<MapShapeHint?> _currentMapShapeHint = new();
    // Fast-path flag: set when at least one push is live anywhere on this connection.
    // Avoids paying the AsyncLocal.Value (ExecutionContext) lookup on every block read
    // when no typed call site has installed a hint — the common case.
    private int _mapShapeHintPushCount;
    // Re-initialized whenever ConnectionString is set so the logger's target
    // ILoggerFactory tracks the new settings' Telemetry config. Defaults to a
    // no-op logger so handshake logging never NRE's if a caller managed to
    // reach Open without a connection string (RequireSettings catches it first).
    private ClickHouseLogger _logger = new(null);

    internal ClickHouseLogger Logger => _logger;
    // Connect-time retry policy, built from settings.Resilience.Retry in ApplySettings.
    // Null when no retry was configured — OpenAsync then runs connect+handshake directly.
    // Applies retry-on-connect to the direct/ADO/Dapper open path (the pooled path's
    // rents flow through OpenAsync too, so they inherit it). Circuit-breaking and
    // multi-server load-balancing remain the province of ResilientConnection.
    private RetryPolicy? _retryPolicy;
    private readonly SchemaCache _schemaCache = new();
    private readonly object _queryLock = new();
    // Serializes every _pipeWriter.WriteAsync+FlushAsync pair. PipeWriter is not
    // thread-safe, and SendCancelAsync fires from CancellationToken.Register on
    // a detached task that can race with the in-flight query write. Without
    // this gate, concurrent writes can interleave bytes on the wire and
    // poison the connection — a pooled connection would then hand the
    // corruption to the next caller.
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private TcpClient? _tcpClient;
    private Stream? _networkStream;
    private SslStream? _sslStream;
    private PipeReader? _pipeReader;
    private PipeWriter? _pipeWriter;
    // Both are read lock-free via Volatile.Read in IsOpen (R3) and other
    // best-effort guards. Mark volatile so writers also use release semantics
    // — pre-R7 the writes were plain stores and a reader on weakly-ordered
    // hardware (ARM) could observe stale values despite the Volatile.Read.
    private volatile bool _isOpen;
    private volatile bool _disposed;
    private bool _compressionEnabled;
    // True between the moment a public Execute path (or OpenAsync handshake) is
    // entered and the moment it returns or releases the slot. Read under
    // _queryLock by EnterBusy to reject overlapping operations on the same
    // connection. This is the busy-state authority — _currentQueryId tracks
    // the in-flight query id for cancellation/pool-safety, but it transiently
    // flips during role-sync recursion, so it is unsafe to use as the busy
    // gate on its own. See ClickHouseConnectionBusyException.
    private bool _busy;
    // The query id reported in ClickHouseConnectionBusyException. Set by
    // EnterBusy, cleared by ExitBusy. Distinct from _currentQueryId because
    // _currentQueryId is owned by SendQueryAsync/ReadServerMessagesAsync and
    // gets temporarily clobbered/cleared by role-sync recursion. This field
    // tracks the *outermost* caller's id and stays stable for the busy
    // window, so a concurrent caller's exception message always names the
    // real owner — never the inner SET ROLE id and never <handshake> just
    // because the inner role-sync finally cleared the slot a microsecond ago.
    private string? _busyOwnerQueryId;
    private string? _currentQueryId;
    private string? _lastQueryId;
    // Set when a ClickHouseProtocolException escapes the read path. The protocol
    // stream is at an unknown offset, so the connection must be discarded — not
    // returned to the pool, not reused for the next query. Read by CanBePooled.
    private volatile bool _protocolFatal;

    // ── Wire-conversation evidence ─────────────────────────────────────────────
    // Together these derive "is the wire at a protocol boundary?" instead of
    // trusting method-exit bookkeeping:
    //   _conversationWrote  — this conversation put non-cancel bytes on the wire.
    //   _boundaryProven     — since the last non-cancel byte we sent, a full
    //                         response terminator (EndOfStream, or a completely
    //                         consumed server-exception envelope) was consumed.
    // Set/cleared in WriteAndFlushAsync; proven only at terminator-consumption
    // sites; reset at EnterBusy. Consumed by ExitBusyResolve's pessimistic gate.
    private volatile bool _conversationWrote;
    private volatile bool _boundaryProven;

    /// <summary>
    /// True when the current conversation has put non-cancel bytes on the wire.
    /// Consumed by the cancellation-drain gate and (from the pessimism step)
    /// ExitBusyResolve; internal for test assertions.
    /// </summary>
    internal bool ConversationWrote => _conversationWrote;

    /// <summary>
    /// True when a full response terminator has been consumed since the last
    /// non-cancel write — i.e., the server owes us nothing. Internal for test
    /// assertions until ExitBusyResolve consumes it.
    /// </summary>
    internal bool BoundaryProven => _boundaryProven;
    private X509Certificate2? _customCaCertificate;

    // Role-sync state: tracks what we last sent via SET ROLE on this session.
    // null + !_rolesExplicitlySet = server's login-time defaults still in effect.
    // null + _rolesExplicitlySet  = SET ROLE DEFAULT already sent (restore requested).
    // list                         = exact SET ROLE list currently active.
    // Guarded by _queryLock for reads; only written from inside EnsureRolesResolvedAsync
    // (which runs serialised behind the same lock by virtue of being inside SendQueryAsync).
    private IReadOnlyList<string>? _currentServerRoles;
    private bool _rolesExplicitlySet;
    private bool _inRoleSync;

    // Sticky override from ChangeRolesAsync. When set, takes precedence over
    // Settings.DefaultRoles for any call without a per-invocation rolesOverride.
    // Analogous to how ChangeDatabase changes the default database for
    // subsequent queries. Cleared on disconnect along with the rest of the state.
    private IReadOnlyList<string>? _pinnedRoles;
    private bool _pinnedRolesSet;

    // Session-state tracking for pool-return reset (ResetSessionStateAsync).
    // ClickHouse `SET <name> = <value>` and `USE <db>` and
    // `CREATE TEMPORARY TABLE <t>` persist for the session's lifetime, which
    // means a pool that reuses the same physical connection silently leaks
    // session state from one renter to the next. We intercept the relevant
    // statements at execute time (cheap regex over the SQL) and reset them
    // when the pool returns the connection to the idle stack.
    //
    // This is best-effort: complex SQL (`SET a=1, b=2`, `WITH ... SET ...`,
    // dynamically-built statements) may not be caught. The escape hatch for
    // those callers is `ClickHouseDataSourceOptions.ResetSessionStateOnReturn = false`.
    private readonly HashSet<string> _dirtySettings = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> _tempTables = new(StringComparer.Ordinal);
    private string? _userSetDatabase;
    private readonly object _sessionStateLock = new();

    // Pool integration: when set, DisposeAsync hands the connection back to this
    // callback instead of tearing down the socket. The hook is one-shot — it is
    // atomically cleared on invocation, so a second Dispose (e.g. if the pool
    // decides to discard and calls DisposeAsync itself) falls through to the
    // normal teardown path. Only ClickHouseDataSource sets this.
    private Func<ClickHouseConnection, ValueTask>? _poolReturnHook;

    internal Func<ClickHouseConnection, ValueTask>? PoolReturnHook
    {
        get => _poolReturnHook;
        set => _poolReturnHook = value;
    }

    /// <summary>
    /// Pool-safety check: returns true when this connection has no session-state
    /// drift that would leak between tenants (no in-flight query, no sticky role
    /// override). A pool should discard any connection where this returns false.
    /// </summary>
    internal bool CanBePooled
    {
        get
        {
            // All four fields must be read under _queryLock to get a
            // consistent snapshot. Reading _isOpen/_disposed outside the lock
            // could latch a stale "true" after a concurrent close has already
            // committed its flip under the lock, leading the pool to accept a
            // dead connection.
            lock (_queryLock)
            {
                if (_disposed || !_isOpen) return false;
                if (_protocolFatal) return false;
                if (_busy) return false;
                if (_currentQueryId is not null) return false;
                if (_pinnedRolesSet || _rolesExplicitlySet) return false;
                return true;
            }
        }
    }

    /// <summary>
    /// Pool-eligibility check that runs *before* <see cref="ResetSessionStateAsync"/>.
    /// Excludes the role latch because reset issues SET ROLE DEFAULT and clears it;
    /// gating on <see cref="_rolesExplicitlySet"/> here would discard every
    /// connection that ever ran <c>SET ROLE</c> before reset got a chance to fix it.
    /// </summary>
    internal bool CanBePooledBeforeReset
    {
        get
        {
            lock (_queryLock)
            {
                if (_disposed || !_isOpen) return false;
                if (_protocolFatal) return false;
                if (_busy) return false;
                if (_currentQueryId is not null) return false;
                if (_pinnedRolesSet) return false;
                return true;
            }
        }
    }

    /// <summary>
    /// Atomically rejects a second concurrent operation on this connection. Throws
    /// <see cref="ClickHouseConnectionBusyException"/> if a public Execute path
    /// (or the OpenAsync handshake) is already in flight; otherwise sets
    /// <see cref="_busy"/> = true and records the owner's query id for the
    /// exception message that any subsequent concurrent caller would see.
    /// Pair every successful call with <see cref="ExitBusyResolve"/> in a finally.
    /// </summary>
    /// <param name="queryIdForOwner">
    /// The query id (or <see cref="ClickHouseConnectionBusyException.HandshakeSentinel"/>)
    /// of the caller claiming the slot. Stored verbatim in
    /// <see cref="_busyOwnerQueryId"/> and surfaced to any rejected concurrent
    /// caller. Pre-resolve via <c>ResolveQueryId</c> at the public-method
    /// boundary so the message names the same id the server logs.
    /// </param>
    private void EnterBusy(string queryIdForOwner)
    {
        lock (_queryLock)
        {
            if (_protocolFatal)
            {
                throw new InvalidOperationException(
                    "Connection is broken: a previous operation left the wire in an indeterminate state. " +
                    "Open a new ClickHouseConnection to continue.");
            }
            if (_busy)
            {
                throw new ClickHouseConnectionBusyException(_busyOwnerQueryId ?? queryIdForOwner);
            }
            _busy = true;
            _busyOwnerQueryId = queryIdForOwner;
            // Fresh conversation, fresh evidence. Deliberately AFTER the reject
            // guards: a losing contender (busy-collision) must not wipe the
            // winning conversation's in-flight evidence. Belt-and-braces — the
            // write-clears-proof rule in WriteAndFlushAsync is the load-bearing
            // reset; this keeps a no-write conversation from inheriting stale
            // "wrote" state from its predecessor.
            _conversationWrote = false;
            _boundaryProven = false;
        }
    }

    /// <summary>
    /// Marks the connection as protocol-fatal: subsequent <see cref="EnterBusy"/>
    /// gates throw <see cref="InvalidOperationException"/>, and the pool
    /// (<see cref="CanBePooled"/>) refuses to hand it back out. Called by
    /// <see cref="BulkInsert.BulkInserter{T}"/> when a bulk-insert write
    /// fails after the wire has been put into INSERT state and Dispose's
    /// recovery path has been disabled by <c>_completeStarted</c>.
    /// </summary>
    internal void MarkProtocolFatal() => _protocolFatal = true;

    /// <summary>
    /// Throws the not-open guard failure. When the connection was closed because a
    /// response could not be fully read (<see cref="_protocolFatal"/>), says so —
    /// "Connection is not open" on a connection the caller never closed would
    /// misattribute the failure.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.DoesNotReturn]
    private void ThrowNotOpen() =>
        throw new InvalidOperationException(_protocolFatal
            ? "Connection is broken: a previous response could not be fully read and the connection has been closed. " +
              "Open a new ClickHouseConnection to continue."
            : "Connection is not open.");

    /// <summary>
    /// Clears <see cref="_currentQueryId"/> if it still matches the given id.
    /// Used by callers (BulkInserter) that own a query id but bail out before
    /// the read loop processes EndOfStream — without this, a server exception
    /// during schema fetch leaves <c>_currentQueryId</c> set and
    /// <see cref="CanBePooled"/> reports false, causing the pool to discard
    /// an otherwise-clean connection. Mirrors the safety pattern in
    /// <see cref="DrainAfterCancellationAsync"/>: only clear if the slot is
    /// still ours so we don't clobber a subsequent query that already
    /// installed its own id.
    /// </summary>
    internal void ClearOwnedQueryId(string? ownedId)
    {
        if (ownedId is null) return;
        lock (_queryLock)
        {
            if (_currentQueryId == ownedId)
                _currentQueryId = null;
        }
    }

    /// <summary>
    /// Releases the busy slot acquired by <see cref="EnterBusy"/>. Idempotent —
    /// calling on a non-busy connection is a no-op so reader paths can call it
    /// from both natural-completion and Dispose without risk of double-release.
    /// Internal so <see cref="Results.ClickHouseDataReader"/> can release the
    /// slot when its enumerator naturally completes.
    /// </summary>
    /// <summary>
    /// Ends the busy conversation owned by <paramref name="ownerQueryId"/> and
    /// resolves the connection's health from the conversation evidence.
    /// </summary>
    /// <remarks>
    /// <para><b>Owner-gated:</b> a no-op unless the slot is held AND (when an owner
    /// id is supplied) held by that owner. This makes double-resolution safe and —
    /// critically — stops a stale caller from resolving a SUCCESSOR's conversation:
    /// the reader releases the slot at EOS (<c>MarkCompleted</c>) precisely so a new
    /// query can start before the reader is disposed; the dispose safety-net must
    /// not then evaluate (and poison) the new query's mid-flight evidence.</para>
    /// <para><b>Pessimistic:</b> if this conversation put non-cancel bytes on the
    /// wire and no response terminator was consumed since, the wire position is
    /// unknown — the connection is marked protocol-fatal rather than being allowed
    /// to look reusable. Every legitimate completion path ends at a proof site
    /// (EOS, consumed server-exception envelope, drain success, handshake success),
    /// so only genuinely indeterminate exits convict. The <c>_isOpen</c> gate keeps
    /// failed handshakes and already-classified (closed) connections benign.</para>
    /// <para>The query id is cleared here unconditionally on a matched exit: the id
    /// lifecycle IS the conversation lifecycle, in both directions — a completed
    /// conversation must never leave an id behind (that blocked pooling after bulk
    /// inserts), and an abnormal exit needs no id latch because the pessimistic
    /// gate carries the verdict.</para>
    /// </remarks>
    internal void ExitBusyResolve(string? ownerQueryId)
    {
        lock (_queryLock)
        {
            if (!_busy)
                return; // already resolved (e.g. MarkCompleted ran; safety-net no-ops)
            if (ownerQueryId is not null && _busyOwnerQueryId is not null && _busyOwnerQueryId != ownerQueryId)
                return; // slot belongs to a successor conversation — not ours to resolve
            _busy = false;
            _busyOwnerQueryId = null;
            _currentQueryId = null;
            if (_isOpen && _conversationWrote && !_boundaryProven)
                _protocolFatal = true;
        }
    }

    /// <summary>
    /// Bulk-insert variant of <see cref="EnterBusy"/>. The bulk-insert path holds
    /// the wire from <c>InitAsync</c> through <c>CompleteAsync</c>/<c>DisposeAsync</c>,
    /// so the slot must persist across the inserter's lifetime. Internal so
    /// <see cref="BulkInsert.BulkInserter{T}"/> can call it; semantics identical
    /// to <see cref="EnterBusy"/>.
    /// </summary>
    internal void EnterBusyForBulkInsert(string queryIdForOwner) => EnterBusy(queryIdForOwner);

    /// <summary>
    /// Internal accessor for <see cref="ResolveQueryId"/>. Lets
    /// <see cref="BulkInsert.BulkInserter{T}"/> resolve the effective query id
    /// once at <c>InitAsync</c> entry — both for <see cref="EnterBusyForBulkInsert"/>
    /// reporting and for the eventual <c>SendInsertQueryAsync</c> call — without
    /// duplicating the GUID-vs-supplied logic.
    /// </summary>
    internal static string ResolveQueryIdInternal(string? supplied) => ResolveQueryId(supplied);

    /// <summary>
    /// Constructs a <see cref="ProtocolReader"/> bound to this connection's configured
    /// <see cref="ClickHouseConnectionSettings.MaxStringLengthBytes"/> cap. Use this in
    /// place of <c>new ProtocolReader(buffer)</c> for any reader that will parse server
    /// bytes — the cap is the only thing standing between a hostile / malformed
    /// length-prefix and a multi-gigabyte allocation.
    /// </summary>
    private ProtocolReader CreateProtocolReader(System.Buffers.ReadOnlySequence<byte> buffer)
        => new(buffer) { MaxStringLengthBytes = Settings.MaxStringLengthBytes };

    /// <summary>
    /// Gets the server information received during handshake.
    /// </summary>
    public ServerHello? ServerInfo { get; private set; }

    /// <summary>
    /// Gets a value indicating whether the connection is open. Best-effort:
    /// the pair of reads is volatile-safe across cores but the value can flip
    /// the instant after the caller observes it (a concurrent
    /// <see cref="DisposeAsync"/> can close the wire after this returns true).
    /// Callers using this as a guard before issuing IO must still handle
    /// <see cref="ObjectDisposedException"/> from the subsequent call.
    /// </summary>
    public bool IsOpen => _isOpen && !_disposed; // both fields are `volatile`

    /// <summary>
    /// Gets the negotiated protocol version (minimum of client and server).
    /// </summary>
    public int NegotiatedProtocolVersion { get; private set; }

    /// <summary>
    /// Gets the connection settings.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the connection was constructed via the parameterless ctor and
    /// <see cref="ConnectionString"/> has not yet been assigned.
    /// </exception>
    public ClickHouseConnectionSettings Settings =>
        _settings ?? throw new InvalidOperationException(
            "ConnectionString has not been set. Either pass a connection string to " +
            "the ClickHouseConnection(string) ctor or assign ConnectionString before " +
            "calling OpenAsync().");

    /// <summary>
    /// Creates a new connection with no settings configured. Used by
    /// <see cref="System.Data.Common.DbProviderFactory.CreateConnection"/> and other
    /// ADO.NET factory patterns that build a connection then set <see cref="ConnectionString"/>
    /// before opening.
    /// </summary>
    /// <remarks>
    /// You must assign <see cref="ConnectionString"/> before calling <see cref="OpenAsync(CancellationToken)"/>
    /// or any other operation that requires settings — those will throw
    /// <see cref="InvalidOperationException"/> if settings are missing.
    /// </remarks>
    public ClickHouseConnection()
    {
    }

    /// <summary>
    /// Creates a new connection using a connection string.
    /// </summary>
    /// <param name="connectionString">The connection string.</param>
    public ClickHouseConnection(string connectionString)
        : this(ClickHouseConnectionSettings.Parse(connectionString))
    {
    }

    /// <summary>
    /// Creates a new connection using settings.
    /// </summary>
    /// <param name="settings">The connection settings.</param>
    public ClickHouseConnection(ClickHouseConnectionSettings settings)
    {
        ArgumentNullException.ThrowIfNull(settings);
        ApplySettings(settings);
    }

    /// <summary>
    /// Centralised "install these settings on this connection" path used by the
    /// <see cref="ClickHouseConnection(ClickHouseConnectionSettings)"/> ctor and the
    /// <see cref="ConnectionString"/> setter. Rebuilds the
    /// settings-dependent fields (column-reader registry, logger) so both the
    /// ctor and the property-set ADO pattern produce identical state.
    /// </summary>
    private void ApplySettings(ClickHouseConnectionSettings settings)
    {
        _settings = settings;
        _columnReaderRegistry = settings.StringMaterialization == StringMaterialization.Lazy
            ? ColumnReaderRegistry.LazyStrings
            : ColumnReaderRegistry.Default;
        _logger = new ClickHouseLogger(settings.Telemetry?.LoggerFactory);
        _retryPolicy = settings.Resilience?.Retry is { } retryOptions
            ? new RetryPolicy(retryOptions, _logger)
            : null;
    }

    // ===================== DbConnection / IDbConnection surface =====================
    //
    // These overrides satisfy the abstract DbConnection contract so a ClickHouseConnection
    // returned from ClickHouseDataSource.OpenConnectionAsync() flows directly into Dapper /
    // EF Core / OpenTelemetry instrumentation without an intermediate wrapper. The native
    // surface above is unaffected — these are additive.
    //
    // ConnectionString is settable until the first OpenAsync. After that the
    // setter throws, so the post-open invariant — settings are stable for the
    // lifetime of the open socket — still holds for every cached read of
    // _settings.* downstream.

    /// <inheritdoc />
#pragma warning disable CS8765 // Nullability of parameter doesn't match overridden member — DbConnection's contract is [AllowNull].
    public override string ConnectionString
    {
        // Settings.ToString() is the canonical password-less connection-string
        // representation in this codebase. Empty string when no settings have
        // been assigned yet (matches DbConnection.ConnectionString documented
        // default for a fresh, unconfigured instance).
        get => _settings?.ToString() ?? string.Empty;
        set
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_isOpen)
                throw new InvalidOperationException(
                    "ClickHouseConnection.ConnectionString cannot be changed while the connection is open. " +
                    "Call CloseAsync() first, or create a new connection.");

            if (string.IsNullOrEmpty(value))
            {
                // Allow clearing to the default state so callers can reset
                // before reusing the instance.
                _settings = null;
                _columnReaderRegistry = ColumnReaderRegistry.Default;
                _logger = new ClickHouseLogger(null);
                return;
            }

            ApplySettings(ClickHouseConnectionSettings.Parse(value));
        }
    }
#pragma warning restore CS8765

    /// <inheritdoc />
    /// <remarks>
    /// Returns the session-active database — either the one set via a
    /// <c>USE &lt;db&gt;</c> statement (tracked by the session-state inspector
    /// at <see cref="TrackSessionStateMutation"/>) or the connection-string
    /// default. Mirrors the standard <see cref="DbConnection.Database"/>
    /// contract so ADO consumers (and humans reading logs) see the database
    /// that subsequent unqualified table references will resolve against.
    /// </remarks>
    public override string Database
    {
        get
        {
            lock (_sessionStateLock)
            {
                return _userSetDatabase ?? _settings?.Database ?? "";
            }
        }
    }

    /// <inheritdoc />
    public override string DataSource =>
        _settings is null ? string.Empty : $"{Settings.Host}:{Settings.EffectivePort}";

    /// <inheritdoc />
    public override string ServerVersion => ServerInfo is null
        ? ""
        : $"{ServerInfo.VersionMajor}.{ServerInfo.VersionMinor}";

    /// <inheritdoc />
    public override ConnectionState State
    {
        get
        {
            // Best-effort snapshot; both backing fields are volatile.
            if (_disposed) return ConnectionState.Closed;
            return _isOpen ? ConnectionState.Open : ConnectionState.Closed;
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// Dispatched via <see cref="Task.Run(Func{Task})"/> so a captured
    /// single-threaded <see cref="SynchronizationContext"/> (UI / classic ASP.NET)
    /// cannot deadlock against the handshake's async continuation. Async callers
    /// should prefer <see cref="OpenAsync(CancellationToken)"/>.
    /// </remarks>
    public override void Open() => Task.Run(() => OpenAsync(CancellationToken.None)).GetAwaiter().GetResult();

    /// <inheritdoc />
    /// <remarks>
    /// Dispatched via <see cref="Task.Run(Func{Task})"/> for the same reason as
    /// <see cref="Open"/>. Async callers should prefer <see cref="CloseAsync()"/>.
    /// </remarks>
    public override void Close() => Task.Run(() => CloseAsync()).GetAwaiter().GetResult();

    /// <inheritdoc />
    /// <remarks>
    /// Issues a <c>USE</c> statement on the current connection. The new database
    /// is tracked in the session-state inspector so the pool resets it on return.
    /// </remarks>
    public override void ChangeDatabase(string databaseName)
    {
        if (!_isOpen)
            ThrowNotOpen();
        if (string.IsNullOrWhiteSpace(databaseName))
            throw new ArgumentException("Database name cannot be empty.", nameof(databaseName));
        if (databaseName.Length > 255)
            throw new ArgumentException(
                $"Database name length ({databaseName.Length}) exceeds the 255-character ClickHouse identifier maximum.",
                nameof(databaseName));
        for (int i = 0; i < databaseName.Length; i++)
        {
            var c = databaseName[i];
            if (char.IsControl(c))
                throw new ArgumentException(
                    $"Database name contains control character U+{(int)c:X4} at position {i}; reject before sending.",
                    nameof(databaseName));
        }

        // Same quoting as the wrapper: backtick-quote, double any embedded backticks.
        Task.Run(() => ExecuteNonQueryAsync(
            $"USE `{databaseName.Replace("`", "``")}`",
            cancellationToken: CancellationToken.None)).GetAwaiter().GetResult();

        // Track the un-quoted name so Database getters return it without the
        // wire-quoting backticks. The session-state inspector that watches USE
        // would otherwise pick up the quoted identifier.
        lock (_sessionStateLock)
        {
            _userSetDatabase = databaseName;
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// ClickHouse does not support ACID transactions. INSERTs are atomic per batch;
    /// for mutations use <c>ALTER TABLE … DELETE/UPDATE</c>.
    /// </remarks>
    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) =>
        throw new NotSupportedException(
            "ClickHouse does not support ACID transactions. " +
            "INSERTs are atomic per batch. For mutations, use ALTER TABLE...DELETE/UPDATE.");

    /// <inheritdoc />
    protected override DbCommand CreateDbCommand()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return new ClickHouseCommand(this);
    }

    /// <inheritdoc />
    /// <remarks>
    /// The async path is the authority; this override bridges sync Dispose
    /// (via the <see cref="System.ComponentModel.Component"/> chain) into
    /// <see cref="DisposeAsync"/>. Async callers should prefer <c>await using</c>
    /// for the natural completion path.
    /// </remarks>
    protected override void Dispose(bool disposing)
    {
        if (disposing && !_disposed)
        {
            // Same deadlock-safety dispatch as Close()/Open().
            Task.Run(async () => await DisposeAsync().ConfigureAwait(false)).GetAwaiter().GetResult();
        }
        base.Dispose(disposing);
    }

    // =================== End DbConnection / IDbConnection surface ===================

    /// <summary>
    /// Resolves the Map-shape hint for the current operation. Returns <c>null</c>
    /// when no per-query override is in play — that's the byte-for-byte legacy path
    /// that uses the registry's composite-reader cache (every Map column reads as a
    /// <see cref="Dictionary{TKey, TValue}"/>). When a typed call site has pushed a
    /// hint (via <see cref="PushMapShapeHintFor"/>), that hint is returned so the
    /// per-column entries readers get selected.
    /// </summary>
    internal MapShapeHint? EffectiveMapShapeHintOrNull()
        => Volatile.Read(ref _mapShapeHintPushCount) == 0 ? null : _currentMapShapeHint.Value;

    /// <summary>
    /// Sets a per-call Map-shape hint scoped to the current async flow. The caller
    /// is responsible for clearing it (typically via the returned disposable
    /// pattern from typed call sites). Used by <see cref="ClickHouseCommand"/>
    /// when executing a typed query whose row type <c>T</c> declares
    /// entries-shaped Map properties.
    /// </summary>
    internal IDisposable PushMapShapeHint(MapShapeHint hint)
    {
        var previous = _currentMapShapeHint.Value;
        _currentMapShapeHint.Value = hint;
        Interlocked.Increment(ref _mapShapeHintPushCount);
        return new PopMapShapeHint(this, previous);
    }

    // The pop runs through this instance method so the Interlocked ref-access lands on
    // `this` rather than a sibling reference — that's what keeps CS0197 (ref to a field
    // of a marshal-by-reference class) off the disposer below.
    private void PopMapShapeHintCore(MapShapeHint? previous)
    {
        _currentMapShapeHint.Value = previous;
        Interlocked.Decrement(ref _mapShapeHintPushCount);
    }

    private sealed class PopMapShapeHint : IDisposable
    {
        private readonly ClickHouseConnection _connection;
        private readonly MapShapeHint? _previous;
        public PopMapShapeHint(ClickHouseConnection connection, MapShapeHint? previous)
        {
            _connection = connection;
            _previous = previous;
        }
        public void Dispose() => _connection.PopMapShapeHintCore(_previous);
    }

    /// <summary>
    /// Single funnel for all pipe writes. PipeWriter is not thread-safe, so
    /// every WriteAsync+FlushAsync pair routes through here under
    /// <see cref="_writeLock"/>. Prevents SendCancelAsync — which fires from a
    /// detached cancellation callback — from racing with an in-flight query or
    /// bulk-insert write and corrupting the wire.
    /// </summary>
    /// <param name="data">The bytes to write.</param>
    /// <param name="cancellationToken">Cancellation token; a cancellation during the
    /// lock wait leaves no conversation evidence (nothing was sent).</param>
    /// <param name="isCancelPacket">
    /// True only for <see cref="SendCancelAsync"/>. A Cancel packet must not touch
    /// the conversation evidence: it can race in from the detached cancellation
    /// callback AFTER the response terminator was already consumed, and un-proving
    /// a clean boundary there would spuriously poison a healthy connection. The
    /// server ignores Cancel packets received in the idle state, so a proven
    /// boundary stays valid. Excluding cancel also keeps all evidence transitions
    /// conversation-sequential (no concurrent writers).
    /// </param>
    private async Task WriteAndFlushAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken, bool isCancelPacket = false)
    {
        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // Evidence is stamped after the lock wait succeeds but BEFORE the
            // write: once we attempt WriteAsync, bytes may be on the wire even if
            // the flush later faults — the conversation must count as "wrote".
            // A write invalidates any prior boundary proof: from this point the
            // server owes us a response again (this also correctly handles nested
            // conversations, e.g. role-sync running inside an outer busy span).
            if (!isCancelPacket)
            {
                _conversationWrote = true;
                _boundaryProven = false;
            }
            await _pipeWriter!.WriteAsync(data, cancellationToken).ConfigureAwait(false);
            await _pipeWriter.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    /// <summary>
    /// Opens the connection and performs the handshake.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    public override async Task OpenAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_isOpen)
            throw new InvalidOperationException("Connection is already open.");

        if (_settings is null)
            throw new InvalidOperationException(
                "ConnectionString has not been set. Either pass a connection string to " +
                "the ClickHouseConnection(string) ctor or assign ConnectionString before " +
                "calling OpenAsync().");

        // Claim the busy slot for the duration of the handshake. Today _isOpen
        // is set strictly after PerformHandshakeAsync completes, so a concurrent
        // Execute call during handshake would already throw "Connection is not
        // open". The sentinel closes the fire-and-forget hole where a future
        // refactor could move the _isOpen flip earlier, and it gives the busy
        // check a precise id ("<handshake>") to surface to the caller.
        EnterBusy(ClickHouseConnectionBusyException.HandshakeSentinel);
        lock (_queryLock)
        {
            _currentQueryId = ClickHouseConnectionBusyException.HandshakeSentinel;
        }

        // ADO contract: StateChange fires on every transition. EF Core, Dapper,
        // OpenTelemetry SqlClient instrumentation, and connection-pool tracking
        // all observe these.
        OnStateChange(new System.Data.StateChangeEventArgs(
            System.Data.ConnectionState.Closed, System.Data.ConnectionState.Connecting));

        using var activity = ClickHouseActivitySource.StartConnect(
            Settings.Host, Settings.EffectivePort, Settings.Telemetry);
        var stopwatch = Stopwatch.StartNew();

        try
        {
            if (_retryPolicy is not null)
            {
                // Retry transient connect/handshake failures (socket refused, network
                // blip). Each failed attempt must reset the half-initialised socket/pipe
                // state first: ConnectTcpAsync allocates a fresh TcpClient and reassigns
                // _networkStream/_pipeReader/_pipeWriter every call, so without this the
                // next attempt would overwrite — and leak — the previous attempt's socket.
                // CloseInternalAsync is a no-op StateChange-wise here (we were never Open).
                await _retryPolicy.ExecuteAsync(async retryCt =>
                {
                    try
                    {
                        await ConnectTcpAsync(retryCt);
                        await PerformHandshakeAsync(retryCt);
                    }
                    catch
                    {
                        await CloseInternalAsync().ConfigureAwait(false);
                        throw;
                    }
                }, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await ConnectTcpAsync(cancellationToken);
                await PerformHandshakeAsync(cancellationToken);
            }
            lock (_queryLock)
            {
                _isOpen = true;
                // Handshake boundary proof is set EXPLICITLY (the handshake does
                // not run through the query pumps, and its final action is a
                // response-less addendum write, so terminator-site rules alone can
                // never prove it). Placed with _isOpen inside the lock, BEFORE
                // OnStateChange/telemetry — those can throw after the connection
                // is nominally open, and the failure path closes (benign).
                _boundaryProven = true;
            }
            OnStateChange(new System.Data.StateChangeEventArgs(
                System.Data.ConnectionState.Connecting, System.Data.ConnectionState.Open));

            // Record telemetry on success
            stopwatch.Stop();
            ClickHouseActivitySource.SetServerInfo(activity, ServerInfo!);
            ClickHouseMeter.ConnectDuration.Record(stopwatch.Elapsed.TotalSeconds);
            ClickHouseMeter.IncrementConnections();
            _logger.ConnectionOpened(Settings.Host, Settings.EffectivePort,
                stopwatch.Elapsed.TotalMilliseconds, NegotiatedProtocolVersion);
            if (Settings.AllowInsecureTls)
                _logger.AllowInsecureTlsEnabled(Settings.Host, Settings.EffectivePort);
        }
        catch (Exception ex)
        {
            ClickHouseActivitySource.SetError(activity, ex);
            _logger.ConnectionFailed(Settings.Host, Settings.EffectivePort, ex.Message);
            await CloseInternalAsync();
            // CloseInternalAsync only fires Open→Closed; on a failed handshake
            // we were in Connecting state, so emit the Connecting→Closed event
            // explicitly to keep the ADO StateChange contract complete.
            OnStateChange(new System.Data.StateChangeEventArgs(
                System.Data.ConnectionState.Connecting, System.Data.ConnectionState.Closed));
            throw;
        }
        finally
        {
            lock (_queryLock)
            {
                if (_currentQueryId == ClickHouseConnectionBusyException.HandshakeSentinel)
                    _currentQueryId = null;
            }
            // Handshake conversation: success set boundary proof next to
            // _isOpen = true; failure paths run with _isOpen still false, so the
            // pessimistic gate stays benign for them.
            ExitBusyResolve(ClickHouseConnectionBusyException.HandshakeSentinel);
        }
    }

    private async Task ConnectTcpAsync(CancellationToken cancellationToken)
    {
        _tcpClient = new TcpClient
        {
            ReceiveBufferSize = Settings.ReceiveBufferSize,
            SendBufferSize = Settings.SendBufferSize,
            NoDelay = true
        };

        // Use the effective port (TlsPort if TLS enabled, otherwise Port)
        var port = Settings.EffectivePort;

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(Settings.ConnectTimeout);

        try
        {
            await _tcpClient.ConnectAsync(Settings.Host, port, timeoutCts.Token);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw ClickHouseConnectionException.Timeout(Settings.Host, port, Settings.ConnectTimeout);
        }
        catch (SocketException ex)
        {
            throw ClickHouseConnectionException.Refused(Settings.Host, port, ex);
        }

        _networkStream = _tcpClient.GetStream();

        // Wrap with TLS if enabled
        if (Settings.UseTls)
        {
            _networkStream = await EstablishTlsAsync(_networkStream, timeoutCts.Token);
        }

        // Wrap with MeteredNetworkStream so BytesSentTotal/BytesReceivedTotal
        // capture every byte that crosses the wire, including post-TLS framing
        // and ClickHouse-level compression headers.
        _networkStream = new MeteredNetworkStream(_networkStream, leaveOpen: false);

        // Use larger buffer sizes to reduce fragmentation and improve IsSingleSegment hit rate
        var pipeReaderOptions = new StreamPipeReaderOptions(
            bufferSize: Settings.PipeBufferSize,
            minimumReadSize: Settings.PipeBufferSize / 4);
        _pipeReader = PipeReader.Create(_networkStream, pipeReaderOptions);
        _pipeWriter = PipeWriter.Create(_networkStream);
    }

    private async Task<Stream> EstablishTlsAsync(Stream innerStream, CancellationToken cancellationToken)
    {
        // Load custom CA certificate if specified
        if (!string.IsNullOrEmpty(Settings.TlsCaCertificatePath))
        {
#if NET9_0_OR_GREATER
            _customCaCertificate = X509CertificateLoader.LoadCertificateFromFile(Settings.TlsCaCertificatePath);
#else
            _customCaCertificate = new X509Certificate2(Settings.TlsCaCertificatePath);
#endif
        }

        _sslStream = new SslStream(
            innerStream,
            leaveInnerStreamOpen: false,
            ValidateServerCertificate);

        var options = new SslClientAuthenticationOptions
        {
            TargetHost = Settings.Host,
            EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
        };

        // Add client certificate for mTLS if specified
        if (Settings.TlsClientCertificate != null)
        {
            options.ClientCertificates = new X509CertificateCollection { Settings.TlsClientCertificate };
        }

        try
        {
            await _sslStream.AuthenticateAsClientAsync(options, cancellationToken);
        }
        catch (AuthenticationException ex)
        {
            throw new ClickHouseAuthenticationException($"TLS authentication failed: {ex.Message}", ex);
        }

        return _sslStream;
    }

    private bool ValidateServerCertificate(
        object sender,
        X509Certificate? certificate,
        X509Chain? chain,
        SslPolicyErrors sslPolicyErrors)
    {
        // If insecure TLS is allowed, accept any certificate
        if (Settings.AllowInsecureTls)
            return true;

        // No errors - certificate is valid
        if (sslPolicyErrors == SslPolicyErrors.None)
            return true;

        // If we have a custom CA certificate, validate against it
        if (_customCaCertificate != null && certificate != null)
        {
            // Create a new chain with our custom CA. Pre-fix this set
            // RevocationMode = NoCheck, so a server cert that had been
            // explicitly revoked (CRL / OCSP) but still chained to the
            // pinned CA was accepted. Switch to Online and tolerate the
            // "unknown" status that self-signed CAs without OCSP / CRL
            // endpoints surface — actively revoked certificates fail
            // validation, certs with no published revocation info still
            // succeed (matching existing test setups).
            using var customChain = new X509Chain();
            customChain.ChainPolicy.ExtraStore.Add(_customCaCertificate);
            customChain.ChainPolicy.VerificationFlags =
                X509VerificationFlags.AllowUnknownCertificateAuthority
                | X509VerificationFlags.IgnoreCertificateAuthorityRevocationUnknown
                | X509VerificationFlags.IgnoreCtlSignerRevocationUnknown
                | X509VerificationFlags.IgnoreEndRevocationUnknown
                | X509VerificationFlags.IgnoreRootRevocationUnknown;
            customChain.ChainPolicy.RevocationMode = X509RevocationMode.Online;

#if NET9_0_OR_GREATER
            var cert2 = certificate as X509Certificate2 ?? X509CertificateLoader.LoadCertificate(certificate.Export(X509ContentType.Cert));
#else
            var cert2 = certificate as X509Certificate2 ?? new X509Certificate2(certificate);
#endif
            if (customChain.Build(cert2))
            {
                // Verify the chain ends with our custom CA
                var rootCert = customChain.ChainElements[^1].Certificate;
                if (rootCert.Thumbprint == _customCaCertificate.Thumbprint)
                    return true;
            }
        }

        // Certificate validation failed
        return false;
    }

    private async Task PerformHandshakeAsync(CancellationToken cancellationToken)
    {
        _logger.HandshakeStart(Settings.Host, Settings.EffectivePort);
        await SendClientHelloAsync(cancellationToken);

        if (Settings.AuthMethod == ClickHouseAuthMethod.SshKey)
            await PerformSshChallengeExchangeAsync(cancellationToken);

        ServerInfo = await ReceiveServerHelloAsync(cancellationToken);
        NegotiatedProtocolVersion = Math.Min(ProtocolVersion.Current, ServerInfo.ProtocolRevision);

        // We always advertise "notchunked" in the client addendum. If the
        // server insists on chunked framing (no "notchunked" path), we must
        // refuse the connection up-front rather than silently send un-chunked
        // frames that the server will desync. "chunked_optional" / empty are
        // permissive and accepted.
        EnsureChunkedNegotiable(ServerInfo.ProtoSendChunkedServer, "send");
        EnsureChunkedNegotiable(ServerInfo.ProtoRecvChunkedServer, "recv");

        // For protocol versions >= WithAddendum, send client hello addendum to server
        if (NegotiatedProtocolVersion >= ProtocolVersion.WithAddendum)
        {
            await SendHelloAddendumAsync(cancellationToken);
        }
    }

    /// <summary>
    /// Forwards a progress message to the user-supplied <see cref="IProgress{T}"/>
    /// handler, swallowing exceptions so a buggy observer can't unwind the
    /// message loop and corrupt connection state. Pre-fix a throwing handler
    /// escaped the read loop, leaving <c>_currentQueryId</c> set and the wire
    /// at an unknown offset; <c>_protocolFatal</c> was never set, so the pool
    /// happily took the connection back.
    /// </summary>
    private void ReportProgressSafely(IProgress<Data.QueryProgress>? progress, ProgressMessage progressMessage)
    {
        if (progress is null) return;
        try
        {
            progress.Report(progressMessage.ToQueryProgress());
        }
        catch (Exception ex)
        {
            _logger.ProgressHandlerThrew(ex.Message, ex);
        }
    }

    private static void EnsureChunkedNegotiable(string? serverDeclared, string direction)
    {
        // Empty / null means the server doesn't speak the chunked vocabulary
        // (older revision or no preference) — fall through.
        if (string.IsNullOrEmpty(serverDeclared)) return;

        // ClickHouse server values: "notchunked", "chunked", "notchunked_optional",
        // "chunked_optional". Anything containing "notchunked" or "_optional" is
        // accepted. A bare "chunked" means the server requires chunked framing,
        // which CH.Native does not yet implement.
        if (serverDeclared.Contains("notchunked", StringComparison.Ordinal) ||
            serverDeclared.Contains("optional", StringComparison.Ordinal))
        {
            return;
        }

        throw new ClickHouseProtocolException(
            $"Server requires chunked framing ({direction} = '{serverDeclared}') but CH.Native " +
            "only supports the unchunked protocol path. Configure the server to allow " +
            "'notchunked' or 'chunked_optional' for this client.");
    }

    private async Task PerformSshChallengeExchangeAsync(CancellationToken cancellationToken)
    {
        // 1. Send SSHChallengeRequest (varint packet type only, no body).
        var requestBuffer = new ArrayBufferWriter<byte>();
        var requestWriter = new ProtocolWriter(requestBuffer);
        requestWriter.WriteVarInt((ulong)ClientMessageType.SSHChallengeRequest);
        await WriteAndFlushAsync(requestBuffer.WrittenMemory, cancellationToken);

        // 2. Read server reply — must be SSHChallenge (18). An Exception (2) typically
        //    means the server is older than 23.9 or the user's SSH key isn't configured.
        var challenge = await ReceiveSshChallengeAsync(cancellationToken);

        // 3. Sign: str(protocol_version) + database + user + challenge (raw concat).
        using var signer = CreateSshSigner();
        var payload = Auth.SshKeySigner.BuildSignedPayload(
            ProtocolVersion.Current,
            Settings.Database,
            Settings.Username,
            challenge);
        var signature = signer.Sign(payload);

        // 4. Send SSHChallengeResponse + length-prefixed signature blob.
        var responseBuffer = new ArrayBufferWriter<byte>();
        var responseWriter = new ProtocolWriter(responseBuffer);
        responseWriter.WriteVarInt((ulong)ClientMessageType.SSHChallengeResponse);
        responseWriter.WriteVarInt((ulong)signature.Length);
        responseWriter.WriteBytes(signature);
        await WriteAndFlushAsync(responseBuffer.WrittenMemory, cancellationToken);
    }

    private async Task<byte[]> ReceiveSshChallengeAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            var result = await _pipeReader!.ReadAsync(cancellationToken);
            var buffer = result.Buffer;

            if (result.IsCanceled)
                throw new OperationCanceledException(cancellationToken);
            if (buffer.IsEmpty && result.IsCompleted)
                throw new ClickHouseConnectionException(
                    "Server closed connection during SSH challenge. " +
                    "Verify the server supports SSH auth (revision >= 54466, ClickHouse 23.9+) " +
                    "and that the user is configured with an ssh_key.");

            try
            {
                var reader = CreateProtocolReader(buffer);
                var packetType = reader.ReadVarInt();

                if (packetType == (ulong)ServerMessageType.Exception)
                {
                    var ex = ExceptionMessage.Read(ref reader);
                    _pipeReader.AdvanceTo(buffer.GetPosition(reader.Consumed));
                    throw Exceptions.ClickHouseServerException.FromExceptionMessage(ex);
                }
                if (packetType != (ulong)ServerMessageType.SSHChallenge)
                    throw new ClickHouseConnectionException(
                        $"Unexpected packet type {packetType} during SSH challenge; expected SSHChallenge ({(int)ServerMessageType.SSHChallenge}) or Exception.");

                var challengeLength = reader.ReadVarIntAsInt32("SSH challenge length");
                var challenge = reader.ReadBytes(challengeLength).ToArray();
                _pipeReader.AdvanceTo(buffer.GetPosition(reader.Consumed));
                return challenge;
            }
            catch (InvalidOperationException)
            {
                // Partial frame — wait for more bytes.
                _pipeReader.AdvanceTo(buffer.Start, buffer.End);
                if (result.IsCompleted)
                    throw new ClickHouseConnectionException("Incomplete SSH challenge from server.");
            }
        }
    }

    private Auth.SshKeySigner CreateSshSigner()
    {
        if (Settings.SshPrivateKey is not null)
            return new Auth.SshKeySigner(Settings.SshPrivateKey, Settings.SshPrivateKeyPassphrase);
        if (Settings.SshPrivateKeyPath is not null)
            return new Auth.SshKeySigner(Settings.SshPrivateKeyPath, Settings.SshPrivateKeyPassphrase);
        throw new InvalidOperationException(
            "AuthMethod is SshKey but neither SshPrivateKey nor SshPrivateKeyPath is set.");
    }

    private async Task SendClientHelloAsync(CancellationToken cancellationToken)
    {
        var (wireUsername, wirePassword) = BuildHandshakeCredentials(Settings);
        var clientHello = ClientHello.Create(
            Settings.ClientName,
            Settings.Database,
            wireUsername,
            wirePassword);

        var bufferWriter = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(bufferWriter);
        clientHello.Write(ref writer);

        await WriteAndFlushAsync(bufferWriter.WrittenMemory, cancellationToken);
    }

    // ClickHouse wire markers for non-password auth, from src/Core/Protocol.h.
    // Exact literal strings (leading + trailing spaces) as the server parses them.
    internal const string JwtAuthMarker = " JWT AUTHENTICATION ";
    internal const string SshKeyAuthMarker = " SSH KEY AUTHENTICATION ";

    internal static (string username, string password) BuildHandshakeCredentials(
        ClickHouseConnectionSettings settings)
    {
        return settings.AuthMethod switch
        {
            ClickHouseAuthMethod.Jwt => (JwtAuthMarker, settings.JwtToken ?? ""),
            ClickHouseAuthMethod.SshKey => (SshKeyAuthMarker + settings.Username, ""),
            ClickHouseAuthMethod.TlsClientCertificate => (settings.Username, ""),
            _ => (settings.Username, settings.Password),
        };
    }

    private async Task<ServerHello> ReceiveServerHelloAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            var result = await _pipeReader!.ReadAsync(cancellationToken);
            var buffer = result.Buffer;

            if (result.IsCanceled)
                throw new OperationCanceledException(cancellationToken);

            if (buffer.IsEmpty && result.IsCompleted)
                throw new ClickHouseConnectionException("Server closed connection during handshake");

            try
            {
                var reader = CreateProtocolReader(buffer);
                var serverHello = ServerHello.Read(ref reader);
                _pipeReader.AdvanceTo(buffer.GetPosition(reader.Consumed));
                return serverHello;
            }
            catch (InvalidOperationException)
            {
                // Not enough data yet, need more
                _pipeReader.AdvanceTo(buffer.Start, buffer.End);

                if (result.IsCompleted)
                    throw new ClickHouseConnectionException("Incomplete server hello response");
            }
        }
    }

    private async Task SendHelloAddendumAsync(CancellationToken cancellationToken)
    {
        // Server addendum wire order (see TCPHandler::receiveAddendum): quota_key,
        // proto_send_chunked + proto_recv_chunked (54470+), parallel-replicas version (54471+).
        var bufferWriter = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(bufferWriter);
        writer.WriteString(string.Empty); // quota_key

        if (NegotiatedProtocolVersion >= ProtocolVersion.WithChunkedPackets)
        {
            writer.WriteString("notchunked");
            writer.WriteString("notchunked");
        }

        if (NegotiatedProtocolVersion >= ProtocolVersion.WithVersionedParallelReplicas)
        {
            writer.WriteVarInt(0); // we do not initiate parallel-replicas reads
        }

        await WriteAndFlushAsync(bufferWriter.WrittenMemory, cancellationToken);
    }

    /// <summary>
    /// Changes the active ClickHouse roles for this connection, equivalent to
    /// running <c>SET ROLE …</c>. Analogous to <c>DbConnection.ChangeDatabase</c>.
    /// <list type="bullet">
    ///   <item><c>null</c> — restore the user's server-configured default roles
    ///   (issues <c>SET ROLE DEFAULT</c> if any prior explicit set is active).</item>
    ///   <item>empty list — strip all active roles (<c>SET ROLE NONE</c>).</item>
    ///   <item>populated list — activate exactly these roles.</item>
    /// </list>
    /// Calls to this method are no-ops when the requested set already matches the
    /// session's current roles (tracked per-connection and reset across reconnects).
    /// </summary>
    /// <param name="roles">Roles to activate, or null / empty as above.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="InvalidOperationException">Connection is not open.</exception>
    /// <exception cref="Exceptions.ClickHouseServerException">
    /// Server refused the SET ROLE (typically <c>ACCESS_DENIED</c> when a role is
    /// not granted to the current user).
    /// </exception>
    public async Task ChangeRolesAsync(IReadOnlyList<string>? roles, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_isOpen)
            ThrowNotOpen();

        // Defensive copy so callers can't mutate the pinned reference after pinning.
        IReadOnlyList<string>? toPin = roles is null ? null : roles.ToArray();

        // This runs a full wire conversation (SET ROLE), so it must hold the busy
        // slot like every other public entry point — previously it ran outside the
        // busy pairing entirely, able to interleave with a running query and, on
        // failure, leave the wire indeterminate with nobody to convict.
        var effectiveQueryId = ResolveQueryId(null);
        EnterBusy(effectiveQueryId);
        try
        {
            await EnsureRolesResolvedAsync(toPin, cancellationToken);

            // Commit the sticky override only after the server accepted the change.
            _pinnedRoles = toPin;
            _pinnedRolesSet = true;
        }
        finally
        {
            ExitBusyResolve(effectiveQueryId);
        }
    }

    /// <summary>
    /// Closes the connection gracefully.
    /// </summary>
    public override Task CloseAsync() => CloseAsync(CancellationToken.None);

    /// <summary>
    /// Closes the connection gracefully.
    /// </summary>
    /// <param name="cancellationToken">
    /// Checked once at entry; if signalled this throws
    /// <see cref="OperationCanceledException"/> before any teardown runs. The
    /// teardown itself (PipeWriter/PipeReader.CompleteAsync, SslStream.DisposeAsync,
    /// TcpClient.Dispose) is uncancellable by API surface, so once close starts
    /// it always completes — the token cannot interrupt mid-flight without
    /// leaking the underlying TCP/SSL handles.
    /// </param>
    public async Task CloseAsync(CancellationToken cancellationToken)
    {
        if (!_isOpen)
            return;

        cancellationToken.ThrowIfCancellationRequested();

        ClickHouseMeter.DecrementConnections();
        _logger.ConnectionClosed(Settings.Host);
        await CloseInternalAsync();
    }

    /// <summary>
    /// Cancels the currently executing query on the server.
    /// If no query is executing, this method does nothing.
    /// </summary>
    /// <remarks>
    /// This sends a Cancel message to the server which will abort the query.
    /// The query method will throw an OperationCanceledException after cancellation.
    /// The connection remains usable for subsequent queries after cancellation.
    /// <para>
    /// The parameterless form is a deliberate convenience: cancellation is itself the
    /// caller's "stop now" intent, so there is usually nothing to cancel the cancel
    /// <i>with</i>. Pass a token via <see cref="CancelCurrentQueryAsync(CancellationToken)"/>
    /// only to bound the Cancel-packet write. The parameterless overload existing here
    /// is intentional and not an oversight.
    /// </para>
    /// </remarks>
    public Task CancelCurrentQueryAsync() => CancelCurrentQueryAsync(CancellationToken.None);

    /// <summary>
    /// Cancels the currently executing query on the server.
    /// If no query is executing, this method does nothing.
    /// </summary>
    /// <param name="cancellationToken">
    /// Token applied to the Cancel-packet write. If it fires mid-write the
    /// underlying <see cref="System.IO.Pipelines.PipeWriter"/> flush throws
    /// <see cref="OperationCanceledException"/>, which <see cref="SendCancelAsync"/>
    /// swallows under its best-effort contract — the connection is left in the
    /// state the partial write produced and should be discarded.
    /// </param>
    public async Task CancelCurrentQueryAsync(CancellationToken cancellationToken)
    {
        string? queryId;
        lock (_queryLock)
        {
            queryId = _currentQueryId;
        }

        if (queryId == null)
            return; // No query to cancel

        await SendCancelAsync(cancellationToken);
    }

    /// <summary>
    /// Gets the ID of the currently executing query, or null if no query is running.
    /// </summary>
    /// <remarks>
    /// Query IDs are auto-generated GUIDs assigned when a query starts.
    /// This can be used with <see cref="KillQueryAsync"/> to cancel queries from another connection.
    /// </remarks>
    public string? CurrentQueryId
    {
        get
        {
            lock (_queryLock)
            {
                return _currentQueryId;
            }
        }
    }

    /// <summary>
    /// Gets the ID of the most-recently-executed query (or the currently executing one).
    /// Persists after the query completes, unlike <see cref="CurrentQueryId"/>. Reflects the
    /// caller-supplied value when set, otherwise the auto-generated GUID. Null if no query
    /// has executed on this connection yet.
    /// </summary>
    public string? LastQueryId
    {
        get
        {
            lock (_queryLock)
            {
                return _lastQueryId;
            }
        }
    }

    private const int MaxQueryIdLength = 128;

    private static string ResolveQueryId(string? supplied)
    {
        if (string.IsNullOrEmpty(supplied))
            return Guid.NewGuid().ToString("D");
        if (supplied.Length > MaxQueryIdLength)
            throw new ArgumentException(
                $"Query ID must be {MaxQueryIdLength} characters or fewer.",
                nameof(supplied));
        return supplied;
    }

    /// <summary>
    /// Kills a query by its ID using a separate connection.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This creates a new connection to execute KILL QUERY, which is more reliable
    /// than sending a Cancel message when the original connection may be blocked.
    /// Use <see cref="CurrentQueryId"/> to get the ID of a running query.
    /// </para>
    /// <para>
    /// Natural-completion race: the original query may finish between this call
    /// issuing the <c>KILL</c> and the server processing it. In that window the
    /// caller of the original query may see either a clean End-Of-Stream
    /// (query completed before kill landed) or a server-cancelled exception
    /// (kill landed first). Both outcomes are valid; the caller should treat
    /// either as "query is no longer running".
    /// </para>
    /// </remarks>
    /// <param name="queryId">The query ID to kill.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task KillQueryAsync(string queryId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(queryId))
            throw new ArgumentNullException(nameof(queryId));

        // Validate the GUID shape BEFORE opening a fresh TCP connection. The
        // KILL QUERY statement does not accept parameters so we have to inline
        // the id, and a malformed value can't ever satisfy that contract — burn
        // the handshake on a hopeless call. Pre-fix this validation ran after
        // OpenAsync, costing a full connect on every malformed input.
        if (!Guid.TryParse(queryId, out _))
            throw new ArgumentException("Invalid query ID format. Expected a GUID.", nameof(queryId));

        await using var killConnection = new ClickHouseConnection(Settings);
        await killConnection.OpenAsync(cancellationToken);

        await killConnection.ExecuteNonQueryAsync(
            $"KILL QUERY WHERE query_id = '{queryId}'",
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Resets session-scoped state (settings, default database, temporary
    /// tables) so the connection is safe to hand to a different renter from
    /// the pool. ClickHouse session state otherwise persists for the lifetime
    /// of the physical connection, leaking from one pool consumer to the next.
    /// </summary>
    /// <remarks>
    /// Best-effort tracking: only state set via the standard <c>SET name = value</c>,
    /// <c>USE db</c>, and <c>CREATE TEMPORARY TABLE t</c> statements is caught.
    /// Compound statements (<c>SET a=1, b=2</c>) and dynamic SQL may bypass
    /// the inspector — callers needing stricter isolation should use
    /// per-query <c>SETTINGS</c> clauses or set
    /// <c>ClickHouseDataSourceOptions.ResetSessionStateOnReturn = false</c>
    /// and manage state explicitly.
    /// </remarks>
    internal async Task ResetSessionStateAsync(CancellationToken cancellationToken = default)
    {
        if (!_isOpen || _protocolFatal) return;

        string[] dirtySettings;
        string[] tempTables;
        string? userSetDatabase;
        lock (_sessionStateLock)
        {
            dirtySettings = _dirtySettings.Count > 0 ? _dirtySettings.ToArray() : Array.Empty<string>();
            tempTables = _tempTables.Count > 0 ? _tempTables.ToArray() : Array.Empty<string>();
            userSetDatabase = _userSetDatabase;
        }

        // Restore default database first — a USE'd database may shadow temp
        // table or settings names, and resetting the database makes
        // subsequent statements unambiguous.
        if (userSetDatabase is not null)
        {
            var defaultDb = string.IsNullOrEmpty(Settings.Database) ? "default" : Settings.Database!;
            await ExecuteNonQueryAsync(
                $"USE {Sql.ClickHouseIdentifier.Quote(defaultDb)}",
                cancellationToken: cancellationToken).ConfigureAwait(false);
            lock (_sessionStateLock) { _userSetDatabase = null; }
        }

        // Reset each dirty setting to its default value. ClickHouse supports
        // `SET <name> = DEFAULT` per-setting, so we batch one statement per
        // dirty setting. Failures are best-effort: if a setting is unknown
        // (e.g. set on an older server, then connection reused on a newer
        // server with the setting renamed), we swallow and move on.
        foreach (var name in dirtySettings)
        {
            try
            {
                await ExecuteNonQueryAsync(
                    $"SET {name} = DEFAULT",
                    cancellationToken: cancellationToken).ConfigureAwait(false);
            }
            catch (Exceptions.ClickHouseServerException)
            {
                // Setting may have been renamed or removed; tolerate.
            }
        }
        lock (_sessionStateLock) { _dirtySettings.Clear(); }

        // Drop temporary tables. ClickHouse's TEMPORARY TABLEs are
        // session-scoped; an explicit DROP is required.
        foreach (var name in tempTables)
        {
            try
            {
                await ExecuteNonQueryAsync(
                    $"DROP TEMPORARY TABLE IF EXISTS {Sql.ClickHouseIdentifier.Quote(name)}",
                    cancellationToken: cancellationToken).ConfigureAwait(false);
            }
            catch (Exceptions.ClickHouseServerException)
            {
                // Best-effort.
            }
        }
        lock (_sessionStateLock) { _tempTables.Clear(); }

        // Restore roles to login-time defaults if a previous query on this
        // connection issued SET ROLE. Without this, the role survives the
        // return-to-pool boundary and leaks to the next renter — observable
        // as the next caller seeing currentRoles() return the prior tenant's
        // role list. Best-effort: a server-side denial leaves the latch alone
        // so the connection won't be re-handed-out with stale state.
        bool needsRoleReset;
        lock (_queryLock)
        {
            needsRoleReset = _rolesExplicitlySet || _currentServerRoles is not null;
        }
        if (needsRoleReset)
        {
            try
            {
                await ExecuteNonQueryAsync(
                    "SET ROLE DEFAULT",
                    cancellationToken: cancellationToken).ConfigureAwait(false);
                lock (_queryLock)
                {
                    _currentServerRoles = null;
                    _rolesExplicitlySet = false;
                }
            }
            catch (Exceptions.ClickHouseServerException)
            {
                // Best-effort: if the server rejects SET ROLE DEFAULT, leave
                // the latch set so CanBePooled keeps the connection out of
                // rotation rather than handing it back with stale roles.
            }
        }
    }

    /// <summary>
    /// Inspects user-issued SQL for session-scoped state changes (SET, USE,
    /// CREATE TEMPORARY TABLE) and records them so <see cref="ResetSessionStateAsync"/>
    /// can undo them at pool-return time. Best-effort: the patterns match the
    /// canonical statement forms; complex / dynamic SQL may bypass the inspector.
    /// </summary>
    internal void TrackSessionStateMutation(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql)) return;
        var trimmed = sql.AsSpan().TrimStart();

        // SET <identifier> =
        if (StartsWithIgnoreCase(trimmed, "SET "))
        {
            var rest = trimmed.Slice(4).TrimStart();
            var nameLen = 0;
            while (nameLen < rest.Length && (char.IsLetterOrDigit(rest[nameLen]) || rest[nameLen] == '_'))
                nameLen++;
            if (nameLen > 0)
            {
                var name = rest.Slice(0, nameLen).ToString();
                // SET ROLE is normally driven by EnsureRolesResolvedAsync which
                // updates _currentServerRoles and _rolesExplicitlySet itself.
                // A raw `SET ROLE …` issued via ExecuteNonQueryAsync bypasses
                // that path, so latch _rolesExplicitlySet here so the
                // pool-return reset still issues SET ROLE DEFAULT instead of
                // leaking the role to the next renter. The setting itself is
                // not tracked in _dirtySettings (resetting via SET ROLE=DEFAULT
                // would fail — it's a role command, not a settings command).
                if (name.Equals("ROLE", StringComparison.OrdinalIgnoreCase))
                {
                    lock (_queryLock) { _rolesExplicitlySet = true; }
                }
                else
                {
                    lock (_sessionStateLock) { _dirtySettings.Add(name); }
                }
            }
            return;
        }

        // USE <identifier>
        if (StartsWithIgnoreCase(trimmed, "USE "))
        {
            var rest = trimmed.Slice(4).TrimStart();
            var nameLen = 0;
            while (nameLen < rest.Length && (char.IsLetterOrDigit(rest[nameLen]) || rest[nameLen] == '_' || rest[nameLen] == '`'))
                nameLen++;
            if (nameLen > 0)
            {
                lock (_sessionStateLock) { _userSetDatabase = rest.Slice(0, nameLen).ToString(); }
            }
            return;
        }

        // CREATE TEMPORARY TABLE <identifier>
        if (StartsWithIgnoreCase(trimmed, "CREATE TEMPORARY TABLE "))
        {
            var rest = trimmed.Slice("CREATE TEMPORARY TABLE ".Length).TrimStart();
            // Skip optional IF NOT EXISTS
            if (StartsWithIgnoreCase(rest, "IF NOT EXISTS "))
                rest = rest.Slice("IF NOT EXISTS ".Length).TrimStart();
            var nameLen = 0;
            while (nameLen < rest.Length && (char.IsLetterOrDigit(rest[nameLen]) || rest[nameLen] == '_' || rest[nameLen] == '`'))
                nameLen++;
            if (nameLen > 0)
            {
                var name = rest.Slice(0, nameLen).ToString().Trim('`');
                lock (_sessionStateLock) { _tempTables.Add(name); }
            }
        }
    }

    private static bool StartsWithIgnoreCase(ReadOnlySpan<char> span, string prefix)
        => span.Length >= prefix.Length && span.Slice(0, prefix.Length).Equals(prefix.AsSpan(), StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Creates a new command associated with this connection.
    /// </summary>
    /// <remarks>
    /// Hides <see cref="DbConnection.CreateCommand"/> via <c>new</c> so the native
    /// API keeps returning the rich <see cref="ClickHouseCommand"/> type. ADO.NET
    /// consumers reach the ADO surface via <see cref="DbConnection.CreateCommand"/>
    /// (dispatched through the <see cref="CreateDbCommand"/> override below).
    /// </remarks>
    /// <returns>A new command instance.</returns>
    public new ClickHouseCommand CreateCommand()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return new ClickHouseCommand(this);
    }

    /// <summary>
    /// Creates a new command with the specified SQL text.
    /// </summary>
    /// <param name="sql">The SQL command text.</param>
    /// <returns>A new command instance with the specified SQL.</returns>
    public ClickHouseCommand CreateCommand(string sql)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return new ClickHouseCommand(this, sql);
    }

    /// <summary>
    /// Creates a new bulk inserter for high-performance batch inserts.
    /// </summary>
    /// <typeparam name="T">The POCO type representing a row.</typeparam>
    /// <param name="tableName">
    /// The table name to insert into. May be qualified as <c>database.table</c>.
    /// </param>
    /// <param name="options">Optional bulk insert options.</param>
    /// <returns>A new bulk inserter instance. Call InitAsync() before use.</returns>
    public BulkInserter<T> CreateBulkInserter<T>(string tableName, BulkInsertOptions? options = null)
        where T : class
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_isOpen)
            ThrowNotOpen();

        return new BulkInserter<T>(this, tableName, options);
    }

    /// <summary>
    /// Creates a new bulk inserter for high-performance batch inserts, targeting
    /// the explicitly-supplied <paramref name="database"/> and <paramref name="tableName"/>.
    /// </summary>
    /// <typeparam name="T">The POCO type representing a row.</typeparam>
    /// <param name="database">The database segment, quoted independently in the rendered SQL.</param>
    /// <param name="tableName">The table segment, used verbatim — dots in the name are not split.</param>
    /// <param name="options">Optional bulk insert options.</param>
#pragma warning disable RS0026, RS0027 // Intentional sibling overload — distinct parameter shape, no resolution ambiguity.
    public BulkInserter<T> CreateBulkInserter<T>(string database, string tableName, BulkInsertOptions? options = null)
        where T : class
#pragma warning restore RS0026, RS0027
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_isOpen)
            ThrowNotOpen();

        return new BulkInserter<T>(this, database, tableName, options);
    }

    /// <summary>
    /// Creates a new dynamic (POCO-less) bulk inserter for high-performance batch inserts.
    /// Rows are supplied as <c>object?[]</c> arrays whose element order matches
    /// <paramref name="columnNames"/>.
    /// </summary>
    /// <param name="tableName">
    /// The table name to insert into. May be qualified as <c>database.table</c>.
    /// </param>
    /// <param name="columnNames">The columns this inserter will write to, in row-element order.</param>
    /// <param name="options">Optional bulk insert options.</param>
#pragma warning disable RS0026, RS0027 // Distinct from generic CreateBulkInserter<T> via parameter shape.
    public DynamicBulkInserter CreateBulkInserter(
        string tableName,
        IReadOnlyList<string> columnNames,
        BulkInsertOptions? options = null)
#pragma warning restore RS0026, RS0027
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_isOpen)
            ThrowNotOpen();

        return new DynamicBulkInserter(this, tableName, columnNames, options);
    }

    /// <summary>
    /// Creates a new dynamic (POCO-less) bulk inserter targeting the explicitly-supplied
    /// <paramref name="database"/> and <paramref name="tableName"/>.
    /// </summary>
#pragma warning disable RS0026, RS0027 // Sibling overload with distinct (database, table) parameter shape.
    public DynamicBulkInserter CreateBulkInserter(
        string database,
        string tableName,
        IReadOnlyList<string> columnNames,
        BulkInsertOptions? options = null)
#pragma warning restore RS0026, RS0027
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_isOpen)
            ThrowNotOpen();

        return new DynamicBulkInserter(this, database, tableName, columnNames, options);
    }

    internal SchemaCache SchemaCache => _schemaCache;

    /// <summary>
    /// Evicts cached bulk-insert schemas on this connection. Call after server-side
    /// ALTER TABLE on a table whose schema is cached, or pass <c>null</c> to clear the
    /// entire cache.
    /// </summary>
    /// <remarks>
    /// Synchronous and <c>void</c> by design on this otherwise async-first API: it is a
    /// purely in-memory cache eviction with no I/O, so it is safe to call from an async
    /// context without awaiting.
    /// </remarks>
    /// <param name="tableName">
    /// The table to evict. When null, the entire cache is cleared. May be qualified
    /// as <c>database.table</c>; an unqualified name targets the connection's default
    /// database (<see cref="ClickHouseConnectionSettings.Database"/>).
    /// </param>
#pragma warning disable RS0026, RS0027 // Shipped overload preserved for source-compat; the (database, tableName) overload is the maximal one.
    public void InvalidateSchemaCache(string? tableName = null)
#pragma warning restore RS0026, RS0027
    {
        if (tableName is null)
        {
            _schemaCache.Clear();
            return;
        }

        var (database, table) = ClickHouseIdentifier.SplitQualifiedName(tableName, Settings.Database);
        _schemaCache.InvalidateTable(database, table);
    }

    /// <summary>
    /// Evicts cached bulk-insert schemas for the specified <paramref name="database"/>
    /// and <paramref name="tableName"/> on this connection.
    /// </summary>
    public void InvalidateSchemaCache(string database, string tableName)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentNullException.ThrowIfNull(tableName);
        _schemaCache.InvalidateTable(database, tableName);
    }

    /// <summary>
    /// Bulk inserts rows from an enumerable into the specified table.
    /// </summary>
    /// <typeparam name="T">The POCO type representing a row.</typeparam>
    /// <param name="tableName">The table name to insert into.</param>
    /// <param name="rows">The rows to insert.</param>
    /// <param name="options">Optional bulk insert options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task BulkInsertAsync<T>(
        string tableName,
        IEnumerable<T> rows,
        BulkInsertOptions? options = null,
        CancellationToken cancellationToken = default) where T : class
    {
        await using var inserter = CreateBulkInserter<T>(tableName, options);
        await inserter.InitAsync(cancellationToken);

        // Use streaming path when preferred (default) for reduced GC pressure
        if (options?.PreferDirectStreaming ?? true)
            await inserter.AddRangeStreamingAsync(rows, cancellationToken);
        else
            await inserter.AddRangeAsync(rows, cancellationToken);

        await inserter.CompleteAsync(cancellationToken);
    }

    /// <summary>
    /// Bulk inserts rows from an async enumerable into the specified table.
    /// Enables streaming inserts with bounded memory usage.
    /// </summary>
    /// <typeparam name="T">The POCO type representing a row.</typeparam>
    /// <param name="tableName">The table name to insert into.</param>
    /// <param name="rows">The async enumerable of rows to insert.</param>
    /// <param name="options">Optional bulk insert options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task BulkInsertAsync<T>(
        string tableName,
        IAsyncEnumerable<T> rows,
        BulkInsertOptions? options = null,
        CancellationToken cancellationToken = default) where T : class
    {
        await using var inserter = CreateBulkInserter<T>(tableName, options);
        await inserter.InitAsync(cancellationToken);

        // Use streaming path when preferred (default) for reduced GC pressure
        if (options?.PreferDirectStreaming ?? true)
        {
            await inserter.AddRangeStreamingAsync(rows, cancellationToken);
        }
        else
        {
            await foreach (var row in rows.WithCancellation(cancellationToken))
            {
                await inserter.AddAsync(row, cancellationToken);
            }
        }

        await inserter.CompleteAsync(cancellationToken);
    }

#pragma warning disable RS0026, RS0027 // Sibling BulkInsertAsync overloads — distinct parameter shapes for (database, tableName) and dynamic columns. No overload-resolution ambiguity.
    /// <summary>
    /// Bulk inserts rows from an enumerable into the explicitly-supplied
    /// <paramref name="database"/> and <paramref name="tableName"/>.
    /// </summary>
    public async Task BulkInsertAsync<T>(
        string database,
        string tableName,
        IEnumerable<T> rows,
        BulkInsertOptions? options = null,
        CancellationToken cancellationToken = default) where T : class
    {
        await using var inserter = CreateBulkInserter<T>(database, tableName, options);
        await inserter.InitAsync(cancellationToken);

        if (options?.PreferDirectStreaming ?? true)
            await inserter.AddRangeStreamingAsync(rows, cancellationToken);
        else
            await inserter.AddRangeAsync(rows, cancellationToken);

        await inserter.CompleteAsync(cancellationToken);
    }

    /// <summary>
    /// Bulk inserts rows from an async enumerable into the explicitly-supplied
    /// <paramref name="database"/> and <paramref name="tableName"/>.
    /// </summary>
    public async Task BulkInsertAsync<T>(
        string database,
        string tableName,
        IAsyncEnumerable<T> rows,
        BulkInsertOptions? options = null,
        CancellationToken cancellationToken = default) where T : class
    {
        await using var inserter = CreateBulkInserter<T>(database, tableName, options);
        await inserter.InitAsync(cancellationToken);

        if (options?.PreferDirectStreaming ?? true)
        {
            await inserter.AddRangeStreamingAsync(rows, cancellationToken);
        }
        else
        {
            await foreach (var row in rows.WithCancellation(cancellationToken))
            {
                await inserter.AddAsync(row, cancellationToken);
            }
        }

        await inserter.CompleteAsync(cancellationToken);
    }

    /// <summary>
    /// Bulk inserts rows from an enumerable of <c>object?[]</c> into the specified
    /// table without requiring a POCO type.
    /// </summary>
    /// <param name="tableName">
    /// The table name to insert into. May be qualified as <c>database.table</c>.
    /// </param>
    /// <param name="columnNames">The columns to write to, in row-element order.</param>
    /// <param name="rows">The rows to insert.</param>
    /// <param name="options">Optional bulk insert options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task BulkInsertAsync(
        string tableName,
        IReadOnlyList<string> columnNames,
        IEnumerable<object?[]> rows,
        BulkInsertOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        await using var inserter = CreateBulkInserter(tableName, columnNames, options);
        await inserter.InitAsync(cancellationToken);
        await inserter.AddRangeAsync(rows, cancellationToken);
        await inserter.CompleteAsync(cancellationToken);
    }

    /// <summary>
    /// Bulk inserts rows from an async enumerable of <c>object?[]</c> into the
    /// specified table without requiring a POCO type.
    /// </summary>
    public async Task BulkInsertAsync(
        string tableName,
        IReadOnlyList<string> columnNames,
        IAsyncEnumerable<object?[]> rows,
        BulkInsertOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        await using var inserter = CreateBulkInserter(tableName, columnNames, options);
        await inserter.InitAsync(cancellationToken);
        await inserter.AddRangeAsync(rows, cancellationToken);
        await inserter.CompleteAsync(cancellationToken);
    }

    /// <summary>
    /// Bulk inserts rows from an enumerable of <c>object?[]</c> into the
    /// explicitly-supplied <paramref name="database"/> and <paramref name="tableName"/>.
    /// </summary>
    public async Task BulkInsertAsync(
        string database,
        string tableName,
        IReadOnlyList<string> columnNames,
        IEnumerable<object?[]> rows,
        BulkInsertOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        await using var inserter = CreateBulkInserter(database, tableName, columnNames, options);
        await inserter.InitAsync(cancellationToken);
        await inserter.AddRangeAsync(rows, cancellationToken);
        await inserter.CompleteAsync(cancellationToken);
    }

    /// <summary>
    /// Bulk inserts rows from an async enumerable of <c>object?[]</c> into the
    /// explicitly-supplied <paramref name="database"/> and <paramref name="tableName"/>.
    /// </summary>
    public async Task BulkInsertAsync(
        string database,
        string tableName,
        IReadOnlyList<string> columnNames,
        IAsyncEnumerable<object?[]> rows,
        BulkInsertOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        await using var inserter = CreateBulkInserter(database, tableName, columnNames, options);
        await inserter.InitAsync(cancellationToken);
        await inserter.AddRangeAsync(rows, cancellationToken);
        await inserter.CompleteAsync(cancellationToken);
    }
#pragma warning restore RS0026, RS0027

    /// <summary>
    /// Executes a query and returns the first column of the first row.
    /// </summary>
    /// <typeparam name="T">The expected result type.</typeparam>
    /// <param name="sql">The SQL query to execute.</param>
    /// <param name="progress">Optional progress reporter.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <param name="queryId">Optional caller-supplied query ID. Null or empty auto-generates a GUID; max length 128.</param>
    /// <returns>The scalar result, or default if no rows returned.</returns>
    public async Task<T?> ExecuteScalarAsync<T>(
        string sql,
        IProgress<QueryProgress>? progress = null,
        CancellationToken cancellationToken = default,
        string? queryId = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_isOpen)
            ThrowNotOpen();

        var effectiveQueryId = ResolveQueryId(queryId);
        EnterBusy(effectiveQueryId);
        try
        {
            using var mapHintScope = PushMapShapeHintFor(typeof(T));
            return await ExecuteScalarCoreAsync<T>(sql, progress, cancellationToken, effectiveQueryId);
        }
        finally
        {
            ExitBusyResolve(effectiveQueryId);
        }
    }

    private async Task<T?> ExecuteScalarCoreAsync<T>(
        string sql,
        IProgress<QueryProgress>? progress,
        CancellationToken cancellationToken,
        string effectiveQueryId)
    {
        using var activity = ClickHouseActivitySource.StartQuery(sql, effectiveQueryId, Settings.Database, Settings.Telemetry);
        var stopwatch = Stopwatch.StartNew();
        long rowsRead = 0;
        var success = false;

        _logger.LogQueryStarted(effectiveQueryId, sql);

        try
        {
            await SendQueryAsync(sql, cancellationToken, effectiveQueryId);

            // Register cancellation callback to send Cancel message to server
            await using var registration = cancellationToken.Register(() => _ = SendCancelAsync());

            object? result = null;
            bool hasResult = false;

            await foreach (var message in ReadServerMessagesAsync(cancellationToken))
            {
                switch (message)
                {
                    case ProgressMessage progressMessage:
                        ReportProgressSafely(progress, progressMessage);
                        rowsRead = (long)progressMessage.Rows;
                        break;

                    case DataMessage dataMessage:
                        if (!hasResult && dataMessage.Block.RowCount > 0 && dataMessage.Block.ColumnCount > 0)
                        {
                            // DateTime64(8/9) raw escape hatch: ExecuteScalarAsync<long>
                            // returns the exact wire value (sub-tick digits intact).
                            result = typeof(T) == typeof(long)
                                && dataMessage.Block.Columns[0] is DateTime64RawColumn rawDt64
                                    ? rawDt64.GetRawValue(0)
                                    : dataMessage.Block.GetValue(0, 0);
                            hasResult = true;
                        }
                        dataMessage.Block.Dispose();
                        break;

                    case EndOfStreamMessage:
                        success = true;
                        ClickHouseActivitySource.SetQueryResults(activity, hasResult ? 1 : 0, 0);
                        return ConvertResult<T>(result);
                }
            }

            return default;
        }
        // Gate on write EVIDENCE, not on "did the detached callback finish sending
        // Cancel" (_cancellationRequested) — that flag is set at the END of a
        // fire-and-forget task and routinely loses the race with this OCE, which
        // used to skip the drain and leave the response bytes to corrupt the next
        // query. If this conversation wrote, a response exists (or is owed) and
        // draining is both safe and required; if it never wrote, there is nothing
        // to drain and draining would hang against a silent server.
        catch (OperationCanceledException ex) when (_conversationWrote)
        {
            ClickHouseActivitySource.SetError(activity, ex);
            // Server cancellation was requested - drain remaining messages to reset connection state
            await DrainAfterCancellationAsync();
            throw;
        }
        catch (Exception ex)
        {
            ClickHouseActivitySource.SetError(activity, ex);
            ClickHouseMeter.ErrorsTotal.Add(1);
            _logger.QueryFailed(effectiveQueryId, ex.Message);
            if (ex is ClickHouseProtocolException) await CloseInternalAsync();
            throw;
        }
        finally
        {
            stopwatch.Stop();
            ClickHouseMeter.RecordQuery(Settings.Database, stopwatch.Elapsed, rowsRead, success);
            if (success)
                _logger.QueryCompleted(effectiveQueryId, rowsRead, stopwatch.Elapsed.TotalMilliseconds);
        }
    }

    /// <summary>
    /// Executes a query that does not return rows.
    /// </summary>
    /// <param name="sql">The SQL query to execute.</param>
    /// <param name="progress">Optional progress reporter.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <param name="queryId">Optional caller-supplied query ID. Null or empty auto-generates a GUID; max length 128.</param>
    /// <returns>The number of rows affected.</returns>
    public async Task<long> ExecuteNonQueryAsync(
        string sql,
        IProgress<QueryProgress>? progress = null,
        CancellationToken cancellationToken = default,
        string? queryId = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_isOpen)
            ThrowNotOpen();

        var effectiveQueryId = ResolveQueryId(queryId);
        EnterBusy(effectiveQueryId);
        try
        {
            var result = await ExecuteNonQueryCoreAsync(sql, progress, cancellationToken, effectiveQueryId);
            // Record session-state mutations only on successful execution so
            // a failed `SET` doesn't poison the dirty list.
            TrackSessionStateMutation(sql);
            return result;
        }
        finally
        {
            ExitBusyResolve(effectiveQueryId);
        }
    }

    /// <summary>
    /// Body of <see cref="ExecuteNonQueryAsync"/> without the busy gate. Called
    /// directly by <see cref="EnsureRolesResolvedAsync"/> for the SET ROLE
    /// subquery — that path runs inside an outer EnterBusy and must not
    /// re-enter the gate.
    /// </summary>
    private async Task<long> ExecuteNonQueryCoreAsync(
        string sql,
        IProgress<QueryProgress>? progress,
        CancellationToken cancellationToken,
        string effectiveQueryId)
    {
        using var activity = ClickHouseActivitySource.StartQuery(sql, effectiveQueryId, Settings.Database, Settings.Telemetry);
        var stopwatch = Stopwatch.StartNew();
        long totalRows = 0;
        var success = false;

        _logger.LogQueryStarted(effectiveQueryId, sql);

        try
        {
            await SendQueryAsync(sql, cancellationToken, effectiveQueryId);

            // Register cancellation callback to send Cancel message to server
            await using var registration = cancellationToken.Register(() => _ = SendCancelAsync());

            await foreach (var message in ReadServerMessagesAsync(cancellationToken))
            {
                switch (message)
                {
                    case ProgressMessage progressMessage:
                        ReportProgressSafely(progress, progressMessage);
                        totalRows = (long)progressMessage.Rows;
                        break;

                    case DataMessage nonQueryDataMessage:
                        nonQueryDataMessage.Block.Dispose();
                        break;

                    case EndOfStreamMessage:
                        success = true;
                        ClickHouseActivitySource.SetQueryResults(activity, totalRows, 0);
                        return totalRows;
                }
            }

            return totalRows;
        }
        // Gate on write EVIDENCE, not on "did the detached callback finish sending
        // Cancel" (_cancellationRequested) — that flag is set at the END of a
        // fire-and-forget task and routinely loses the race with this OCE, which
        // used to skip the drain and leave the response bytes to corrupt the next
        // query. If this conversation wrote, a response exists (or is owed) and
        // draining is both safe and required; if it never wrote, there is nothing
        // to drain and draining would hang against a silent server.
        catch (OperationCanceledException ex) when (_conversationWrote)
        {
            ClickHouseActivitySource.SetError(activity, ex);
            // Server cancellation was requested - drain remaining messages to reset connection state
            await DrainAfterCancellationAsync();
            throw;
        }
        catch (Exception ex)
        {
            ClickHouseActivitySource.SetError(activity, ex);
            ClickHouseMeter.ErrorsTotal.Add(1);
            _logger.QueryFailed(effectiveQueryId, ex.Message);
            if (ex is ClickHouseProtocolException) await CloseInternalAsync();
            throw;
        }
        finally
        {
            stopwatch.Stop();
            ClickHouseMeter.RecordQuery(Settings.Database, stopwatch.Elapsed, totalRows, success);
            if (success)
                _logger.QueryCompleted(effectiveQueryId, totalRows, stopwatch.Elapsed.TotalMilliseconds);
        }
    }

    /// <summary>
    /// Executes a query and returns a data reader for streaming results.
    /// </summary>
    /// <param name="sql">The SQL query to execute.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <param name="queryId">Optional caller-supplied query ID. Null or empty auto-generates a GUID; max length 128.</param>
    /// <returns>A data reader for iterating through results.</returns>
    public async Task<ClickHouseDataReader> ExecuteReaderAsync(
        string sql,
        CancellationToken cancellationToken = default,
        string? queryId = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_isOpen)
            ThrowNotOpen();

        var effectiveQueryId = ResolveQueryId(queryId);
        EnterBusy(effectiveQueryId);
        var slotReleased = false;
        try
        {
            // Activity, stopwatch, and rows-read counter all live for the reader's
            // lifetime — the reader records the query metric on disposal.
            var activity = ClickHouseActivitySource.StartQuery(sql, effectiveQueryId, Settings.Database, Settings.Telemetry);
            var stopwatch = Stopwatch.StartNew();
            _logger.LogQueryStarted(effectiveQueryId, sql);

            try
            {
                await SendQueryAsync(sql, cancellationToken, effectiveQueryId);

                var enumerator = ReadServerMessagesAsync(cancellationToken)
                    .GetAsyncEnumerator(cancellationToken);

                // Slot is owned by the returned reader from this point on — it
                // releases via ExitBusy on natural completion or DisposeAsync.
                slotReleased = true;
                return new ClickHouseDataReader(enumerator, this, activity, effectiveQueryId, stopwatch);
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                ClickHouseActivitySource.SetError(activity, ex);
                activity?.Dispose();
                ClickHouseMeter.RecordQuery(Settings.Database, stopwatch.Elapsed, 0, success: false);
                ClickHouseMeter.ErrorsTotal.Add(1);
                _logger.QueryFailed(effectiveQueryId, ex.Message);
                if (ex is ClickHouseProtocolException) await CloseInternalAsync();
                throw;
            }
        }
        finally
        {
            if (!slotReleased) ExitBusyResolve(effectiveQueryId);
        }
    }

    /// <summary>
    /// Streams query results as an async enumerable of rows. Use <c>await foreach</c>
    /// to iterate; rows arrive lazily without buffering the full result set in
    /// memory. Renamed from <c>QueryAsync</c> in Phase 2 so the call site name
    /// no longer collides with Dapper's materialized
    /// <see cref="System.Data.IDbConnection"/> extension methods — bare
    /// <c>conn.QueryAsync&lt;T&gt;(sql)</c> on a <see cref="ClickHouseConnection"/>
    /// now binds to Dapper (returns <c>Task&lt;IEnumerable&lt;T&gt;&gt;</c>).
    /// </summary>
    /// <param name="sql">The SQL query to execute.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <param name="queryId">Optional caller-supplied query ID. Null or empty auto-generates a GUID; max length 128.</param>
    /// <returns>An async enumerable of rows.</returns>
    public async IAsyncEnumerable<ClickHouseRow> QueryStreamAsync(
        string sql,
        [EnumeratorCancellation] CancellationToken cancellationToken = default,
        string? queryId = null)
    {
        await using var reader = await ExecuteReaderAsync(sql, cancellationToken, queryId);

        while (await reader.ReadAsync(cancellationToken))
        {
            yield return new ClickHouseRow(reader);
        }
    }

    /// <summary>
    /// Streams query results as an async enumerable of mapped objects.
    /// Renamed from <c>QueryAsync</c> in Phase 2 to free the call-site name for
    /// Dapper's <see cref="System.Data.IDbConnection"/> extension methods.
    /// </summary>
    /// <typeparam name="T">The type to map rows to. Must have a parameterless constructor.</typeparam>
    /// <param name="sql">The SQL query to execute.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <param name="queryId">Optional caller-supplied query ID. Null or empty auto-generates a GUID; max length 128.</param>
    /// <returns>An async enumerable of mapped objects.</returns>
    public async IAsyncEnumerable<T> QueryStreamAsync<T>(
        string sql,
        [EnumeratorCancellation] CancellationToken cancellationToken = default,
        string? queryId = null)
    {
        using var mapHintScope = PushMapShapeHintFor(typeof(T));

        await using var reader = await ExecuteReaderAsync(sql, cancellationToken, queryId);

        // Need to call ReadAsync at least once to initialize schema before creating mapper
        if (!await reader.ReadAsync(cancellationToken))
            yield break;

        var mapper = new TypeMapper<T>(reader);

        // Map the first row
        yield return mapper.Map(reader);

        // Map remaining rows
        while (await reader.ReadAsync(cancellationToken))
        {
            yield return mapper.Map(reader);
        }
    }

    /// <summary>
    /// Pushes a per-call Map-shape hint derived from a typed call site's <c>T</c>
    /// onto the AsyncLocal stack. For scalar types whose runtime classification is
    /// entries-shape (e.g. <c>T = ClickHouseMap&lt;,&gt;</c>), forces every Map
    /// column to entries. For POCO types, walks properties for per-column hints.
    /// Returns a disposable that pops the hint when the typed call completes;
    /// returns a no-op when T has no Map-shape signals.
    /// </summary>
    internal IDisposable PushMapShapeHintFor(Type rowType)
    {
        if (Mapping.MapShapeInspector.InspectScalar(rowType) == MapShape.Entries)
        {
            return PushMapShapeHint(MapShapeHint.AllEntries);
        }

        var perColumn = Mapping.MapShapeInspector.Inspect(rowType);
        // The Empty sentinel is the cached "no Map-shape properties" case — avoid
        // pushing a hint at all so the reader factory stays on the byte-for-byte
        // legacy Dictionary path.
        if (ReferenceEquals(perColumn, Mapping.MapShapeInspector.Empty))
        {
            return NoOpDisposable.Instance;
        }

        return PushMapShapeHint(new MapShapeHint(perColumn));
    }

    private sealed class NoOpDisposable : IDisposable
    {
        public static readonly NoOpDisposable Instance = new();
        public void Dispose() { }
    }

    /// <summary>
    /// Executes a query and returns an async enumerable of mapped objects using the fast typed path.
    /// This method avoids boxing for primitive types, providing significantly better performance
    /// for large result sets.
    /// </summary>
    /// <typeparam name="T">The type to map rows to. Must have a parameterless constructor.</typeparam>
    /// <param name="sql">The SQL query to execute.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <param name="queryId">Optional caller-supplied query ID. Null or empty auto-generates a GUID; max length 128.</param>
    /// <returns>An async enumerable of mapped objects.</returns>
    public async IAsyncEnumerable<T> QueryTypedAsync<T>(
        string sql,
        [EnumeratorCancellation] CancellationToken cancellationToken = default,
        string? queryId = null)
        where T : new()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_isOpen)
            ThrowNotOpen();

        using var mapHintScope = PushMapShapeHintFor(typeof(T));
        var effectiveQueryId = ResolveQueryId(queryId);
        EnterBusy(effectiveQueryId);
        try
        {
            using var activity = ClickHouseActivitySource.StartQuery(sql, effectiveQueryId, Settings.Database, Settings.Telemetry);
            var stopwatch = Stopwatch.StartNew();
            long totalRows = 0;
            var success = false;

            _logger.LogQueryStarted(effectiveQueryId, sql);

            Mapping.ITypedRowMapper<T>? mapper = null;

            try
            {
                await SendQueryAsync(sql, cancellationToken, effectiveQueryId);

                // Register cancellation callback to send Cancel message to server
                await using var registration = cancellationToken.Register(() => _ = SendCancelAsync());

                await foreach (var block in ReadTypedBlocksAsync(cancellationToken))
                {
                    using (block)
                    {
                        if (block.RowCount == 0)
                            continue;

                        // Create mapper on first non-empty block
                        mapper ??= Mapping.TypedRowMapperFactory.GetMapper<T>(block.ColumnNames);

                        // Map all rows in this block
                        for (int i = 0; i < block.RowCount; i++)
                        {
                            totalRows++;
                            yield return mapper.MapRow(block.Columns, i);
                        }
                    }
                }

                success = true;
                ClickHouseActivitySource.SetQueryResults(activity, totalRows, 0);
            }
            finally
            {
                // Wire realignment for EVERY abnormal exit of the typed stream.
                // This lives in a finally (not a catch) because `yield return`
                // forbids an enclosing catch, and because iterator ABANDONMENT
                // (caller breaks out of `await foreach`) runs only finallys —
                // a catch would miss it entirely. Covers: cancellation (OCE from
                // the pump), consumer exceptions between yields, and early break.
                // Without this, the un-consumed response bytes are read by the
                // next query on this connection as its own response — silent
                // wrong results. Skipped when the terminator was already consumed
                // (boundary proven) or nothing was written.
                if (!success && _isOpen && _conversationWrote && !_boundaryProven)
                {
                    // Tell the server to stop producing before draining what's
                    // in flight (mirrors ClickHouseDataReader.DisposeAsync). A
                    // duplicate Cancel (token registration may have already sent
                    // one) is tolerated by the server.
                    try { await SendCancelAsync(); } catch { /* best effort */ }
                    try { await DrainAfterCancellationAsync(); } catch { /* drain marks fatal on failure */ }
                }

                stopwatch.Stop();
                ClickHouseMeter.RecordQuery(Settings.Database, stopwatch.Elapsed, totalRows, success);
                if (success)
                    _logger.QueryCompleted(effectiveQueryId, totalRows, stopwatch.Elapsed.TotalMilliseconds);
                else
                    ClickHouseMeter.ErrorsTotal.Add(1);
            }
        }
        finally
        {
            ExitBusyResolve(effectiveQueryId);
        }
    }

    /// <summary>
    /// Reads typed blocks from the server message stream.
    /// </summary>
    private async IAsyncEnumerable<TypedBlock> ReadTypedBlocksAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var registry = _columnReaderRegistry;

        try
        {
            while (true)
            {
                var result = await _pipeReader!.ReadAsync(cancellationToken);
                var buffer = result.Buffer;

                if (result.IsCanceled)
                    throw new OperationCanceledException(cancellationToken);

                if (buffer.IsEmpty && result.IsCompleted)
                {
                    // Server closed the socket mid-response — the connection is dead.
                    // Mark it fatal so the finally below tears it down and the pool's
                    // CanBePooled gate discards it. Without this, a connection with no
                    // dirty session state to reset passes every pool health check
                    // (_isOpen stays true, ResetSessionStateAsync does no I/O) and is
                    // re-pooled dead — wedging recovery until idle/lifetime timeout.
                    _protocolFatal = true;
                    throw new ClickHouseConnectionException("Server closed connection unexpectedly");
                }

                bool messageRead = false;
                bool advancedInLoop = false;
                while (TryReadTypedMessage(ref buffer, registry, out var message, out var typedBlock))
                {
                    messageRead = true;

                    if (message is EndOfStreamMessage)
                    {
                        // Parity with the untyped pump: EndOfStream must be the
                        // last byte of this read. Trailing bytes in the SAME
                        // segment mean the stream is out of sync — pin the
                        // failure here rather than blessing a corrupt boundary.
                        if (!buffer.IsEmpty)
                        {
                            var trailingLength = buffer.Length;
                            _pipeReader.AdvanceTo(buffer.End);
                            _protocolFatal = true;
                            throw new ClickHouseProtocolException(
                                $"Server sent {trailingLength} byte(s) after EndOfStream; protocol stream is out of sync.");
                        }
                        _pipeReader.AdvanceTo(buffer.Start);
                        // Terminator consumed cleanly — boundary proven.
                        _boundaryProven = true;
                        yield break;
                    }

                    if (typedBlock != null)
                    {
                        _pipeReader.AdvanceTo(buffer.Start);
                        yield return typedBlock;
                        advancedInLoop = true;
                        // Need to re-read buffer after yielding
                        break;
                    }
                }

                if (!advancedInLoop)
                {
                    // Advance to consumed position, examined to end
                    _pipeReader.AdvanceTo(buffer.Start, buffer.End);
                }

                if (result.IsCompleted && !messageRead)
                {
                    // Truncated response (socket EOF mid-stream) — connection is dead.
                    // Mark fatal so the pool discards it rather than re-pooling it (see
                    // the server-closed case above).
                    _protocolFatal = true;
                    throw new ClickHouseConnectionException("Incomplete response from server");
                }
            }
        }
        finally
        {
            // Clear query tracking when done (success or error)
            lock (_queryLock)
            {
                _currentQueryId = null;
            }

            // A parse failure mid-block leaves unread response bytes in the pipe.
            // Close eagerly so the server releases the session now and callers see
            // State == Closed immediately, instead of the dead connection lingering
            // until its next use. (_protocolFatal stays set: ThrowNotOpen keeps the
            // diagnostic message, and the pool refuses the connection either way.)
            if (_protocolFatal && _isOpen)
            {
                // Runs in a finally, so swallow any close-time error: the original
                // parse exception is the one the caller needs to see, and the
                // connection is poisoned regardless of whether the close succeeds.
                try
                {
                    await CloseInternalAsync();
                }
                catch
                {
                    // Intentionally ignored — see above.
                }
            }
        }
    }

    private bool TryReadTypedMessage(ref ReadOnlySequence<byte> buffer, ColumnReaderRegistry registry, out object? message, out TypedBlock? typedBlock)
    {
        message = null;
        typedBlock = null;

        try
        {
            // Iterate rather than tail-recurse on skip-and-continue message types
            // (ProfileInfo / ProfileEvents). A single response can carry many
            // consecutive profiling blocks; recursing once per block overflowed
            // the stack and crashed the process.
        restart:
            if (buffer.IsEmpty)
                return false;

            var reader = CreateProtocolReader(buffer);
            var messageType = (ServerMessageType)reader.ReadVarInt();

            switch (messageType)
            {
                case ServerMessageType.Data:
                    // For uncompressed data, do a non-allocating scan pass first
                    // to validate block completeness before parsing
                    if (!_compressionEnabled)
                    {
                        // Create a scanner positioned after the message type
                        var scanBuffer = buffer.Slice(reader.Consumed);
                        if (!TryScanUncompressedDataMessage(scanBuffer, out var messageLength))
                        {
                            // Not enough data yet - don't consume buffer, wait for more
                            return false;
                        }

                        // Buffer into contiguous memory if fragmented for faster parsing
                        // This matches what the compressed path does after decompression
                        if (!scanBuffer.IsSingleSegment)
                        {
                            var pool = ArrayPool<byte>.Shared;
                            var contiguous = pool.Rent((int)messageLength);
                            try
                            {
                                scanBuffer.Slice(0, messageLength).CopyTo(contiguous);
                                var contiguousSeq = new ReadOnlySequence<byte>(contiguous, 0, (int)messageLength);
                                var contiguousReader = CreateProtocolReader(contiguousSeq);

                                // Read table name and block from contiguous buffer
                                var tableName = contiguousReader.ReadString();
                                typedBlock = Block.ReadTypedBlockWithTableName(ref contiguousReader, registry, tableName, NegotiatedProtocolVersion, EffectiveMapShapeHintOrNull());

                                // Advance original buffer past message type + message content
                                buffer = buffer.Slice(reader.Consumed + messageLength);
                                return true;
                            }
                            finally
                            {
                                pool.Return(contiguous);
                            }
                        }
                    }
                    else if (_compressionEnabled)
                    {
                        var scanBuffer = buffer.Slice(reader.Consumed);
                        if (!TryScanCompressedDataMessage(scanBuffer))
                            return false;
                    }

                    typedBlock = ReadTypedDataMessage(ref reader, registry);
                    buffer = buffer.Slice(reader.Consumed);
                    return true;

                case ServerMessageType.Exception:
                {
                    // Scan-then-parse — see TryReadMessage for the full rationale.
                    var bodyBuffer = buffer.Slice(reader.Consumed);
                    var scanReader = CreateProtocolReader(bodyBuffer);
                    if (!ExceptionMessage.TryScan(ref scanReader))
                        return false;

                    var parseReader = CreateProtocolReader(bodyBuffer);
                    var exceptionMessage = ExceptionMessage.Read(ref parseReader);
                    buffer = bodyBuffer.Slice(parseReader.Consumed);
                    _pipeReader!.AdvanceTo(buffer.Start);
                    // Envelope fully consumed and advanced past: the server-side
                    // exception IS this response's terminator — boundary proven.
                    // Proof must stay colocated with the AdvanceTo above.
                    _boundaryProven = true;
                    throw ClickHouseServerException.FromExceptionMessage(exceptionMessage);
                }

                case ServerMessageType.Progress:
                {
                    var bodyBuffer = buffer.Slice(reader.Consumed);
                    var scanReader = CreateProtocolReader(bodyBuffer);
                    if (!ProgressMessage.TryScan(ref scanReader, NegotiatedProtocolVersion))
                        return false;

                    var parseReader = CreateProtocolReader(bodyBuffer);
                    var progressMessage = ProgressMessage.Read(ref parseReader, NegotiatedProtocolVersion);
                    buffer = bodyBuffer.Slice(parseReader.Consumed);
                    message = progressMessage;
                    return true;
                }

                case ServerMessageType.EndOfStream:
                    buffer = buffer.Slice(reader.Consumed);
                    message = EndOfStreamMessage.Instance;
                    return true;

                case ServerMessageType.ProfileInfo:
                {
                    // Scan-then-skip: lets a partial ProfileInfo return false
                    // cleanly instead of relying on the catch-IOException backstop.
                    var bodyBuffer = buffer.Slice(reader.Consumed);
                    var scanReader = CreateProtocolReader(bodyBuffer);
                    if (!TrySkipProfileInfo(ref scanReader))
                        return false;

                    var parseReader = CreateProtocolReader(bodyBuffer);
                    SkipProfileInfo(ref parseReader);
                    buffer = bodyBuffer.Slice(parseReader.Consumed);
                    goto restart; // continue reading; iterative to avoid stack overflow
                }

                case ServerMessageType.ProfileEvents:
                    // ProfileEvents - read and discard using regular block (not typed)
                    // For uncompressed data, do a scan pass first
                    if (!_compressionEnabled)
                    {
                        var scanBuffer = buffer.Slice(reader.Consumed);
                        if (!TryScanUncompressedDataMessage(scanBuffer, out _))
                            return false;
                    }
                    else if (_compressionEnabled)
                    {
                        var scanBuffer = buffer.Slice(reader.Consumed);
                        if (!TryScanCompressedDataMessage(scanBuffer))
                            return false;
                    }
                    var profileEventsMsg = ReadDataMessage(ref reader, registry);
                    profileEventsMsg.Block.Dispose();
                    buffer = buffer.Slice(reader.Consumed);
                    goto restart; // continue reading; iterative to avoid stack overflow

                case ServerMessageType.Totals:
                case ServerMessageType.Extremes:
                    // These are data blocks - read as typed
                    // For uncompressed data, do a scan pass first
                    if (!_compressionEnabled)
                    {
                        var scanBuffer = buffer.Slice(reader.Consumed);
                        if (!TryScanUncompressedDataMessage(scanBuffer, out var totalsMessageLength))
                            return false;

                        // Buffer into contiguous memory if fragmented for faster parsing
                        if (!scanBuffer.IsSingleSegment)
                        {
                            var pool = ArrayPool<byte>.Shared;
                            var contiguous = pool.Rent((int)totalsMessageLength);
                            try
                            {
                                scanBuffer.Slice(0, totalsMessageLength).CopyTo(contiguous);
                                var contiguousSeq = new ReadOnlySequence<byte>(contiguous, 0, (int)totalsMessageLength);
                                var contiguousReader = CreateProtocolReader(contiguousSeq);

                                var tableName = contiguousReader.ReadString();
                                typedBlock = Block.ReadTypedBlockWithTableName(ref contiguousReader, registry, tableName, NegotiatedProtocolVersion, EffectiveMapShapeHintOrNull());

                                buffer = buffer.Slice(reader.Consumed + totalsMessageLength);
                                return true;
                            }
                            finally
                            {
                                pool.Return(contiguous);
                            }
                        }
                    }
                    else if (_compressionEnabled)
                    {
                        var scanBuffer = buffer.Slice(reader.Consumed);
                        if (!TryScanCompressedDataMessage(scanBuffer))
                            return false;
                    }
                    typedBlock = ReadTypedDataMessage(ref reader, registry);
                    buffer = buffer.Slice(reader.Consumed);
                    return true;

                default:
                    throw new ClickHouseException($"Unexpected server message type: {messageType}");
            }
        }
        catch (ClickHouseServerException)
        {
            // Server-side error from the ServerMessageType.Exception arm. That
            // arm advances the pipe past the exception body inline before
            // throwing, so the wire is in a clean state — the connection
            // stays usable for subsequent operations.
            throw;
        }
        catch (ClickHouseProtocolException)
        {
            // Wire bytes were malformed; the protocol stream is now at an
            // unknown offset. Mark the connection as poison so the pool
            // discards it on return — see CanBePooled.
            _protocolFatal = true;
            throw;
        }
        catch (InvalidOperationException)
        {
            // Not enough data yet
            return false;
        }
        catch (NotSupportedException ex)
        {
            // The reader factory rejected an unsupported column type mid-block:
            // wire bytes were partially consumed at an indeterminate offset, so
            // the connection is poisoned and eagerly closed by the message-pump
            // finally. Rethrow the same exception type with the consequence
            // appended so the ORIGINAL failure carries the full diagnosis,
            // rather than the user discovering a dead connection one query later.
            _protocolFatal = true;
            throw new NotSupportedException(
                $"{ex.Message} The connection has been closed because the response could not be fully read; " +
                "open a new connection (or take a fresh one from the pool) to continue.",
                ex);
        }
        catch (Exception)
        {
            // Any other exception (type-arg list malformed, value decode
            // overflowed, …) means wire bytes were partially consumed at an
            // indeterminate offset. Poison the connection so the pool discards
            // it and the next call surfaces a clean "Connection is broken"
            // instead of silently re-parsing stale bytes.
            _protocolFatal = true;
            throw;
        }
    }

    /// <summary>
    /// Scans an uncompressed data message without allocating arrays.
    /// This is the first pass of two-pass parsing for uncompressed data.
    /// </summary>
    /// <param name="buffer">The buffer positioned after the message type.</param>
    /// <param name="messageLength">The total length of the message in bytes.</param>
    /// <returns>True if the entire message is available; false if not enough data.</returns>
    private bool TryScanUncompressedDataMessage(ReadOnlySequence<byte> buffer, out long messageLength)
    {
        messageLength = 0;
        var scanner = CreateProtocolReader(buffer);

        // Skip table name
        if (!scanner.TrySkipString())
            return false;

        // Read block header (BlockInfo, columnCount, rowCount)
        var header = Block.TryReadBlockHeader(ref scanner);
        if (header == null)
            return false;

        // Skip all column data
        var skipperRegistry = ColumnSkipperRegistry.Default;
        if (!Block.TrySkipBlockColumns(ref scanner, skipperRegistry,
            header.Value.ColumnCount, header.Value.RowCount, NegotiatedProtocolVersion))
            return false;

        messageLength = scanner.Consumed;
        return true;
    }

    /// <summary>
    /// Scans a compressed data message to check if the full compressed block is available.
    /// Peeks at the compressed block header to read compressedSize without allocating.
    /// </summary>
    /// <param name="buffer">The buffer positioned after the message type.</param>
    /// <returns>True if the entire compressed block is available; false if not enough data.</returns>
    private bool TryScanCompressedDataMessage(ReadOnlySequence<byte> buffer)
    {
        var scanner = CreateProtocolReader(buffer);

        // Skip table name (VarInt length prefix + bytes)
        if (!scanner.TrySkipString())
            return false;

        // Need at least 16 (checksum) + 9 (header) = 25 bytes to read compressed block header
        if (scanner.Remaining < 25)
            return false;

        // Peek at algorithm byte at offset 16 (after checksum)
        byte alg = scanner.PeekByte(16);

        // If not a recognized compression algorithm, it's uncompressed — let the parser handle it
        if (alg != 0x82 && alg != 0x90)
            return true;

        // Read compressedSize (UInt32LE at offset 17-20 from current position)
        uint compressedSize = (uint)scanner.PeekByte(17)
            | ((uint)scanner.PeekByte(18) << 8)
            | ((uint)scanner.PeekByte(19) << 16)
            | ((uint)scanner.PeekByte(20) << 24);

        // Full block available? Need 16 (checksum) + compressedSize bytes
        return scanner.Remaining >= 16 + compressedSize;
    }

    private TypedBlock ReadTypedDataMessage(ref ProtocolReader reader, ColumnReaderRegistry registry)
    {
        if (!_compressionEnabled)
        {
            // Scan pass already validated completeness in TryReadTypedMessage
            // Just parse directly now - we know we have all the data
            var tableName = reader.ReadString();
            return Block.ReadTypedBlockWithTableName(ref reader, registry, tableName, NegotiatedProtocolVersion, EffectiveMapShapeHintOrNull());
        }

        // Table name is read OUTSIDE the compressed block
        var tableNameFromCompressed = reader.ReadString();

        bool isCompressed = IsNextBlockCompressed(ref reader);

        if (!isCompressed)
        {
            // Data is not compressed - read directly
            return Block.ReadTypedBlockWithTableName(ref reader, registry, tableNameFromCompressed, NegotiatedProtocolVersion, EffectiveMapShapeHintOrNull());
        }

        // ClickHouse sends block data in multiple compressed chunks (typically 1MB each).
        // Read and decompress all chunks, concatenate, then parse the complete block.
        using var accumulator = new PooledBufferWriter(1024 * 1024);

        do
        {
            using var compressedData = CompressedBlock.ReadFromProtocol(ref reader);
            using var decompressed = CompressedBlock.DecompressPooled(compressedData.Span);

            var span = accumulator.GetSpan(decompressed.Length);
            decompressed.Span.CopyTo(span);
            accumulator.Advance(decompressed.Length);

            // Try to parse the accumulated decompressed data
            try
            {
                var seq = new ReadOnlySequence<byte>(accumulator.WrittenMemory);
                var blockReader = CreateProtocolReader(seq);
                return Block.ReadTypedBlockWithTableName(ref blockReader, registry, tableNameFromCompressed, NegotiatedProtocolVersion, EffectiveMapShapeHintOrNull());
            }
            catch (InvalidOperationException)
            {
                // Not enough decompressed data yet — read next compressed chunk
            }
        } while (IsNextBlockCompressed(ref reader));

        throw new InvalidDataException(
            $"Failed to parse block after decompressing all available compressed chunks ({accumulator.WrittenCount} bytes decompressed).");
    }

    /// <summary>
    /// Internal method for executing a parameterized scalar query.
    /// Used by ClickHouseCommand.
    /// </summary>
    internal async Task<T?> ExecuteScalarWithParametersAsync<T>(
        string sql,
        ClickHouseParameterCollection parameters,
        IProgress<QueryProgress>? progress = null,
        CancellationToken cancellationToken = default,
        IReadOnlyList<string>? rolesOverride = null,
        string? queryId = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_isOpen)
            ThrowNotOpen();

        var effectiveQueryId = ResolveQueryId(queryId);
        EnterBusy(effectiveQueryId);
        try
        {
            var (rewrittenSql, settings) = SqlParameterRewriter.Process(sql, parameters);
            using var activity = ClickHouseActivitySource.StartQuery(rewrittenSql, effectiveQueryId, Settings.Database, Settings.Telemetry);
            var stopwatch = Stopwatch.StartNew();
            long rowsRead = 0;
            var success = false;

            _logger.LogQueryStarted(effectiveQueryId, rewrittenSql);

            try
            {
                await SendQueryAsync(rewrittenSql, settings, rolesOverride, cancellationToken, effectiveQueryId);

                // Register cancellation callback to send Cancel message to server
                await using var registration = cancellationToken.Register(() => _ = SendCancelAsync());

                object? result = null;
                bool hasResult = false;

                await foreach (var message in ReadServerMessagesAsync(cancellationToken))
                {
                    switch (message)
                    {
                        case ProgressMessage progressMessage:
                            ReportProgressSafely(progress, progressMessage);
                            rowsRead = (long)progressMessage.Rows;
                            break;

                        case DataMessage dataMessage:
                            if (!hasResult && dataMessage.Block.RowCount > 0 && dataMessage.Block.ColumnCount > 0)
                            {
                                // DateTime64(8/9) raw escape hatch — see ExecuteScalarCoreAsync.
                                result = typeof(T) == typeof(long)
                                    && dataMessage.Block.Columns[0] is DateTime64RawColumn rawDt64
                                        ? rawDt64.GetRawValue(0)
                                        : dataMessage.Block.GetValue(0, 0);
                                hasResult = true;
                            }
                            dataMessage.Block.Dispose();
                            break;

                        case EndOfStreamMessage:
                            success = true;
                            ClickHouseActivitySource.SetQueryResults(activity, hasResult ? 1 : 0, 0);
                            return ConvertResult<T>(result);
                    }
                }

                return default;
            }
            // Gate on write EVIDENCE, not on "did the detached callback finish sending
        // Cancel" (_cancellationRequested) — that flag is set at the END of a
        // fire-and-forget task and routinely loses the race with this OCE, which
        // used to skip the drain and leave the response bytes to corrupt the next
        // query. If this conversation wrote, a response exists (or is owed) and
        // draining is both safe and required; if it never wrote, there is nothing
        // to drain and draining would hang against a silent server.
        catch (OperationCanceledException ex) when (_conversationWrote)
            {
                ClickHouseActivitySource.SetError(activity, ex);
                await DrainAfterCancellationAsync();
                throw;
            }
            catch (Exception ex)
            {
                ClickHouseActivitySource.SetError(activity, ex);
                ClickHouseMeter.ErrorsTotal.Add(1);
                _logger.QueryFailed(effectiveQueryId, ex.Message);
                if (ex is ClickHouseProtocolException) await CloseInternalAsync();
                throw;
            }
            finally
            {
                stopwatch.Stop();
                ClickHouseMeter.RecordQuery(Settings.Database, stopwatch.Elapsed, rowsRead, success);
                if (success)
                    _logger.QueryCompleted(effectiveQueryId, rowsRead, stopwatch.Elapsed.TotalMilliseconds);
            }
        }
        finally
        {
            ExitBusyResolve(effectiveQueryId);
        }
    }

    /// <summary>
    /// Internal method for executing a parameterized non-query.
    /// Used by ClickHouseCommand.
    /// </summary>
    internal async Task<long> ExecuteNonQueryWithParametersAsync(
        string sql,
        ClickHouseParameterCollection parameters,
        IProgress<QueryProgress>? progress = null,
        CancellationToken cancellationToken = default,
        IReadOnlyList<string>? rolesOverride = null,
        string? queryId = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_isOpen)
            ThrowNotOpen();

        var effectiveQueryId = ResolveQueryId(queryId);
        EnterBusy(effectiveQueryId);
        try
        {
            var (rewrittenSql, settings) = SqlParameterRewriter.Process(sql, parameters);
            using var activity = ClickHouseActivitySource.StartQuery(rewrittenSql, effectiveQueryId, Settings.Database, Settings.Telemetry);
            var stopwatch = Stopwatch.StartNew();
            long totalRows = 0;
            var success = false;

            _logger.LogQueryStarted(effectiveQueryId, rewrittenSql);

            try
            {
                await SendQueryAsync(rewrittenSql, settings, rolesOverride, cancellationToken, effectiveQueryId);

                // Register cancellation callback to send Cancel message to server
                await using var registration = cancellationToken.Register(() => _ = SendCancelAsync());

                await foreach (var message in ReadServerMessagesAsync(cancellationToken))
                {
                    switch (message)
                    {
                        case ProgressMessage progressMessage:
                            ReportProgressSafely(progress, progressMessage);
                            totalRows = (long)progressMessage.Rows;
                            break;

                        case DataMessage nonQueryParamDataMessage:
                            nonQueryParamDataMessage.Block.Dispose();
                            break;

                        case EndOfStreamMessage:
                            success = true;
                            ClickHouseActivitySource.SetQueryResults(activity, totalRows, 0);
                            return totalRows;
                    }
                }

                return totalRows;
            }
            // Gate on write EVIDENCE, not on "did the detached callback finish sending
        // Cancel" (_cancellationRequested) — that flag is set at the END of a
        // fire-and-forget task and routinely loses the race with this OCE, which
        // used to skip the drain and leave the response bytes to corrupt the next
        // query. If this conversation wrote, a response exists (or is owed) and
        // draining is both safe and required; if it never wrote, there is nothing
        // to drain and draining would hang against a silent server.
        catch (OperationCanceledException ex) when (_conversationWrote)
            {
                ClickHouseActivitySource.SetError(activity, ex);
                await DrainAfterCancellationAsync();
                throw;
            }
            catch (Exception ex)
            {
                ClickHouseActivitySource.SetError(activity, ex);
                ClickHouseMeter.ErrorsTotal.Add(1);
                _logger.QueryFailed(effectiveQueryId, ex.Message);
                if (ex is ClickHouseProtocolException) await CloseInternalAsync();
                throw;
            }
            finally
            {
                stopwatch.Stop();
                ClickHouseMeter.RecordQuery(Settings.Database, stopwatch.Elapsed, totalRows, success);
                if (success)
                    _logger.QueryCompleted(effectiveQueryId, totalRows, stopwatch.Elapsed.TotalMilliseconds);
            }
        }
        finally
        {
            ExitBusyResolve(effectiveQueryId);
        }
    }

    /// <summary>
    /// Internal method for executing a parameterized reader query.
    /// Used by ClickHouseCommand.
    /// </summary>
    internal async Task<ClickHouseDataReader> ExecuteReaderWithParametersAsync(
        string sql,
        ClickHouseParameterCollection parameters,
        CancellationToken cancellationToken = default,
        IReadOnlyList<string>? rolesOverride = null,
        string? queryId = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_isOpen)
            ThrowNotOpen();

        var effectiveQueryId = ResolveQueryId(queryId);
        EnterBusy(effectiveQueryId);
        var slotReleased = false;
        try
        {
            var (rewrittenSql, settings) = SqlParameterRewriter.Process(sql, parameters);
            var activity = ClickHouseActivitySource.StartQuery(rewrittenSql, effectiveQueryId, Settings.Database, Settings.Telemetry);
            var stopwatch = Stopwatch.StartNew();
            _logger.LogQueryStarted(effectiveQueryId, rewrittenSql);

            try
            {
                await SendQueryAsync(rewrittenSql, settings, rolesOverride, cancellationToken, effectiveQueryId);

                var enumerator = ReadServerMessagesAsync(cancellationToken)
                    .GetAsyncEnumerator(cancellationToken);

                // Reader owns the busy slot from this point on.
                slotReleased = true;
                return new ClickHouseDataReader(enumerator, this, activity, effectiveQueryId, stopwatch);
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                ClickHouseActivitySource.SetError(activity, ex);
                activity?.Dispose();
                ClickHouseMeter.RecordQuery(Settings.Database, stopwatch.Elapsed, 0, success: false);
                ClickHouseMeter.ErrorsTotal.Add(1);
                _logger.QueryFailed(effectiveQueryId, ex.Message);
                if (ex is ClickHouseProtocolException) await CloseInternalAsync();
                throw;
            }
        }
        finally
        {
            if (!slotReleased) ExitBusyResolve(effectiveQueryId);
        }
    }

    private Task SendQueryAsync(string sql, CancellationToken cancellationToken, string? queryId = null)
        => SendQueryAsync(sql, null, rolesOverride: null, cancellationToken, queryId);

    private Task SendQueryAsync(
        string sql,
        IReadOnlyDictionary<string, string>? parameters,
        CancellationToken cancellationToken,
        string? queryId = null)
        => SendQueryAsync(sql, parameters, rolesOverride: null, cancellationToken, queryId);

    private async Task SendQueryAsync(
        string sql,
        IReadOnlyDictionary<string, string>? parameters,
        IReadOnlyList<string>? rolesOverride,
        CancellationToken cancellationToken,
        string? queryId = null)
    {
        // Role sync must precede the real query. When rolesOverride is null we fall
        // back to the connection-level default. The sync itself is a plain query, so
        // re-entering SendQueryAsync would loop — _inRoleSync short-circuits that.
        if (!_inRoleSync)
        {
            await EnsureRolesResolvedAsync(rolesOverride, cancellationToken);
        }

        // Set compression state for response handling
        _compressionEnabled = Settings.Compress;

        // Build settings dictionary for JSON/Dynamic serialization on CH 25.6+.
        // write_json_as_string keeps JSON columns on the simple String path; the FLATTENED
        // flag routes Dynamic columns through the Native-only FLATTENED encoding. Server
        // precedence (SerializationObject.cpp:268-271) means STRING wins over FLATTENED for
        // Object, so JSON is unaffected by the second setting.
        Dictionary<string, string>? querySettings = null;
        if (IsClickHouse25_6OrLater())
        {
            querySettings = new Dictionary<string, string>
            {
                ["output_format_native_write_json_as_string"] = "1",
                ["output_format_native_use_flattened_dynamic_and_json_serialization"] = "1",
            };
        }

        var queryMessage = QueryMessage.Create(
            sql,
            Settings.ClientName,
            Settings.Username,
            NegotiatedProtocolVersion,
            useCompression: Settings.Compress,
            parameters: parameters,
            settings: querySettings,
            queryId: queryId);

        // Track the query ID for cancellation support
        lock (_queryLock)
        {
            _currentQueryId = queryMessage.QueryId;
            _lastQueryId = queryMessage.QueryId;
        }

        var bufferWriter = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(bufferWriter);

        // Write query message
        queryMessage.Write(ref writer, NegotiatedProtocolVersion);

        // Write empty data block (required after query)
        // When compression is enabled, the client must also send compressed data blocks
        if (Settings.Compress)
        {
            var compressor = CompressedBlock.GetCompressor(Settings.CompressionMethod);
            Block.WriteEmpty(ref writer, compressor);
        }
        else
        {
            Block.WriteEmpty(ref writer);
        }

        await WriteAndFlushAsync(bufferWriter.WrittenMemory, cancellationToken);
    }

    /// <summary>
    /// Ensures the server's active role set matches the desired set before executing
    /// a query. Issues <c>SET ROLE …</c> (or <c>SET ROLE NONE</c> / <c>SET ROLE DEFAULT</c>)
    /// when the state differs; no-ops otherwise.
    /// </summary>
    /// <param name="desired">
    /// The override roles for this call; null means fall back to the connection-level
    /// <see cref="ClickHouseConnectionSettings.Roles"/>.
    /// </param>
    /// <param name="cancellationToken">Token to cancel the role-resolution round-trip.</param>
    private async Task EnsureRolesResolvedAsync(IReadOnlyList<string>? desired, CancellationToken cancellationToken)
    {
        // Precedence: per-call override > sticky override from ChangeRolesAsync > connection default.
        var effective = desired;
        if (effective is null)
            effective = _pinnedRolesSet ? _pinnedRoles : Settings.Roles;

        // Tag the surrounding Activity with the effective role set. Attaches to
        // the CH.Native query Activity or any outer OTEL span (ADO, user-wrapped),
        // and costs nothing when no Activity is active.
        Telemetry.ClickHouseActivitySource.TagActiveRoles(effective);

        if (RoleSetsEqual(effective, _currentServerRoles, _rolesExplicitlySet))
            return;

        string sql;
        if (effective is null)
        {
            // Restore server-assigned default roles (only reachable after a previous
            // explicit SET — otherwise RoleSetsEqual short-circuits before we get here).
            sql = "SET ROLE DEFAULT";
        }
        else if (effective.Count == 0)
        {
            sql = "SET ROLE NONE";
        }
        else
        {
            var quoted = new List<string>(effective.Count);
            foreach (var role in effective)
                quoted.Add(Sql.ClickHouseIdentifier.Quote(role));
            sql = "SET ROLE " + string.Join(", ", quoted);
        }

        _inRoleSync = true;
        try
        {
            // Reuse the non-query message-reading path so we get exception
            // translation. _inRoleSync prevents EnsureRolesResolvedAsync from
            // re-entering itself via SendQueryAsync. We call the Core method
            // directly to bypass the busy gate — the outer Execute caller
            // already owns the slot. The synthetic id ensures server-side
            // logs distinguish the implicit SET ROLE from the outer query.
            var roleSyncQueryId = ResolveQueryId(null);
            await ExecuteNonQueryCoreAsync(sql, progress: null, cancellationToken, roleSyncQueryId).ConfigureAwait(false);
        }
        finally
        {
            _inRoleSync = false;
        }

        // Commit the state only AFTER the server acknowledges — an ACCESS_DENIED on
        // SET ROLE throws above and we retain the previous _currentServerRoles.
        _currentServerRoles = effective;
        _rolesExplicitlySet = true;
    }

    /// <summary>
    /// Structural equality over role sets, taking the "explicitly set" latch into
    /// account so null-on-first-use (server defaults) short-circuits without any
    /// SET ROLE traffic.
    /// </summary>
    private static bool RoleSetsEqual(IReadOnlyList<string>? desired, IReadOnlyList<string>? current, bool explicitlySet)
    {
        if (desired is null && !explicitlySet)
            return true; // first query, caller wants defaults, server has defaults

        if (desired is null && current is null)
            return true;

        if (desired is null || current is null)
            return false;

        if (desired.Count != current.Count)
            return false;

        for (var i = 0; i < desired.Count; i++)
        {
            if (!string.Equals(desired[i], current[i], StringComparison.Ordinal))
                return false;
        }

        return true;
    }

    /// <summary>
    /// Sends a cancel message to the server to abort the current query.
    /// This is an internal method called when cancellation is requested.
    /// </summary>
    /// <param name="cancellationToken">
    /// Optional token applied to the Cancel-packet write. Default is
    /// <see cref="CancellationToken.None"/> because the existing
    /// <c>cancellationToken.Register(() =&gt; _ = SendCancelAsync())</c>
    /// callers fire this *from* a token that is already cancelled — passing it
    /// in would short-circuit the very write that performs the cancel.
    /// </param>
    internal async Task SendCancelAsync(CancellationToken cancellationToken = default)
    {
        if (_pipeWriter == null || !_isOpen)
            return;

        try
        {
            var bufferWriter = new ArrayBufferWriter<byte>();
            var writer = new ProtocolWriter(bufferWriter);
            CancelMessage.Write(ref writer);

            // isCancelPacket: this write must not touch conversation evidence —
            // see WriteAndFlushAsync's parameter doc.
            await WriteAndFlushAsync(bufferWriter.WrittenMemory, cancellationToken, isCancelPacket: true);

        }
        catch
        {
            // Best effort - connection may already be closed
        }
    }

    /// <summary>
    /// Drains remaining server messages after a cancellation to reset connection state.
    /// After Cancel is sent, the server responds with either Exception or EndOfStream.
    /// </summary>
    /// <remarks>
    /// Captures the owned queryId at entry and only clears <c>_currentQueryId</c>
    /// in the finally if it still matches — otherwise a drain running on a
    /// detached continuation could null out the slot belonging to a subsequent
    /// query started on the same connection.
    /// </remarks>
    internal async Task DrainAfterCancellationAsync()
    {
        if (_pipeReader == null || !_isOpen)
            return;

        string? ownedQueryId;
        lock (_queryLock)
        {
            ownedQueryId = _currentQueryId;
        }

        var registry = _columnReaderRegistry;

        try
        {
            // Bounded drain wait so a wedged server doesn't strand the
            // connection forever. Pre-fix this was 5 s; under loaded networks
            // or slow servers a normal drain occasionally exceeded that and
            // flipped _protocolFatal, discarding an otherwise-fine connection.
            // 30 s gives realistic tail-latency headroom while still capping
            // the worst case.
            using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

            while (true)
            {
                var result = await _pipeReader.ReadAsync(timeoutCts.Token);
                var buffer = result.Buffer;

                if (buffer.IsEmpty && result.IsCompleted)
                    break;

                while (TryReadMessage(ref buffer, registry, out var message))
                {
                    if (message is EndOfStreamMessage)
                    {
                        // Same trailing-bytes discipline as the pumps: the drain
                        // is the path that CERTIFIES a connection as salvageable
                        // after cancellation, so trailing garbage after its EOS
                        // must poison, not be blessed as a clean boundary.
                        if (!buffer.IsEmpty)
                        {
                            _pipeReader.AdvanceTo(buffer.End);
                            _protocolFatal = true;
                            return;
                        }
                        _pipeReader.AdvanceTo(buffer.Start);
                        // Drain reached the terminator — boundary proven; the
                        // connection is salvageable after the cancellation.
                        _boundaryProven = true;
                        return;
                    }
                }

                _pipeReader.AdvanceTo(buffer.Start, buffer.End);

                if (result.IsCompleted)
                    break;
                // NOTE (intentional): the two `break` exits above fall through
                // WITHOUT boundary proof — the socket died mid-drain, so under
                // the pessimistic resolve the conversation correctly classifies
                // as broken. Do not "fix" them by adding proof here.
            }
        }
        catch (ClickHouseServerException)
        {
            // Expected - server sends exception for cancelled query.
            // TryReadMessage's exception arm consumed the envelope (and set
            // boundary proof) before throwing — connection is in a clean state.
        }
        catch
        {
            // Drain timed out or read garbled bytes. The wire is at an unknown
            // offset, so the connection must not be reused. _protocolFatal flips
            // CanBePooled to false, so a pool / ResilientConnection discards it.
            _protocolFatal = true;
        }
        finally
        {
            lock (_queryLock)
            {
                // Only clear the slot we were draining for. A subsequent query
                // may have installed its own id while we were mid-drain; we
                // must not clobber it.
                if (_currentQueryId == ownedQueryId)
                    _currentQueryId = null;
            }
        }
    }

    private async IAsyncEnumerable<object> ReadServerMessagesAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var registry = _columnReaderRegistry;

        try
        {
            while (true)
            {
                var result = await _pipeReader!.ReadAsync(cancellationToken);
                var buffer = result.Buffer;

                if (result.IsCanceled)
                    throw new OperationCanceledException(cancellationToken);

                if (buffer.IsEmpty && result.IsCompleted)
                {
                    // Server closed the socket mid-response — the connection is dead.
                    // Mark it fatal so the finally below tears it down and the pool's
                    // CanBePooled gate discards it. Without this, a connection with no
                    // dirty session state to reset passes every pool health check
                    // (_isOpen stays true, ResetSessionStateAsync does no I/O) and is
                    // re-pooled dead — wedging recovery until idle/lifetime timeout.
                    _protocolFatal = true;
                    throw new ClickHouseConnectionException("Server closed connection unexpectedly");
                }

                bool messageRead = false;
                bool advancedInLoop = false;
                while (TryReadMessage(ref buffer, registry, out var message))
                {
                    messageRead = true;

                    if (message is EndOfStreamMessage)
                    {
                        // EndOfStream must be the last byte the server sends for
                        // this query. If anything follows in the SAME pipe segment
                        // we just read, the server is out of spec and the stream
                        // is no longer trustworthy. Pin the failure here rather
                        // than letting stale bytes corrupt the next query.
                        //
                        // We deliberately don't peek into _pipeReader for trailing
                        // bytes that haven't yet been ReadAsync'd — calling TryRead
                        // here would return the same un-AdvanceTo'd result that
                        // contained the EOS byte itself, falsely flagging it as
                        // trailing. Bytes that arrive in a *later* packet after
                        // EOS will be caught on the next query when the pump tries
                        // to dispatch them as a fresh message type — typically the
                        // unknown-message-type defence (L1) tears the connection
                        // down at that point.
                        if (!buffer.IsEmpty)
                        {
                            var trailingLength = buffer.Length;
                            _pipeReader.AdvanceTo(buffer.End);
                            _protocolFatal = true;
                            throw new ClickHouseProtocolException(
                                $"Server sent {trailingLength} byte(s) after EndOfStream; protocol stream is out of sync.");
                        }

                        _pipeReader.AdvanceTo(buffer.Start);
                        // Terminator consumed cleanly (trailing-bytes check above
                        // passed): the server owes us nothing — boundary proven.
                        _boundaryProven = true;
                        yield return message;
                        yield break;
                    }

                    _pipeReader.AdvanceTo(buffer.Start);
                    if (message is not Protocol.Messages.SkipMessage)
                    {
                        yield return message;
                    }
                    advancedInLoop = true;
                    // Need to re-read buffer after yielding (or after a skip).
                    break;
                }

                if (!advancedInLoop)
                {
                    // Advance to consumed position, examined to end
                    _pipeReader.AdvanceTo(buffer.Start, buffer.End);
                }

                if (result.IsCompleted && !messageRead)
                {
                    // Truncated response (socket EOF mid-stream) — connection is dead.
                    // Mark fatal so the pool discards it rather than re-pooling it (see
                    // the server-closed case above).
                    _protocolFatal = true;
                    throw new ClickHouseConnectionException("Incomplete response from server");
                }
            }
        }
        finally
        {
            // Clear query tracking when done (success or error)
            lock (_queryLock)
            {
                _currentQueryId = null;
            }

            // A parse failure mid-block leaves unread response bytes in the pipe.
            // Close eagerly so the server releases the session now and callers see
            // State == Closed immediately, instead of the dead connection lingering
            // until its next use. (_protocolFatal stays set: ThrowNotOpen keeps the
            // diagnostic message, and the pool refuses the connection either way.)
            if (_protocolFatal && _isOpen)
            {
                // Runs in a finally, so swallow any close-time error: the original
                // parse exception is the one the caller needs to see, and the
                // connection is poisoned regardless of whether the close succeeds.
                try
                {
                    await CloseInternalAsync();
                }
                catch
                {
                    // Intentionally ignored — see above.
                }
            }
        }
    }

    private bool TryReadMessage(ref ReadOnlySequence<byte> buffer, ColumnReaderRegistry registry, out object message)
    {
        message = null!;

        try
        {
            // Iterate rather than tail-recurse on skip-and-continue message types
            // (ProfileInfo / ProfileEvents). A single response — or a cancellation
            // drain — can carry many consecutive profiling blocks; recursing once
            // per block overflowed the stack and crashed the process.
        restart:
            if (buffer.IsEmpty)
                return false;

            var reader = CreateProtocolReader(buffer);
            var messageType = (ServerMessageType)reader.ReadVarInt();

            switch (messageType)
            {
                case ServerMessageType.Data:
                    // For uncompressed data, do a non-allocating scan pass first
                    // to validate block completeness before parsing
                    if (!_compressionEnabled)
                    {
                        var scanBuffer = buffer.Slice(reader.Consumed);
                        if (!TryScanUncompressedDataMessage(scanBuffer, out var dataMessageLength))
                        {
                            return false;
                        }

                        // Buffer into contiguous memory if fragmented for faster parsing
                        if (!scanBuffer.IsSingleSegment)
                        {
                            var pool = ArrayPool<byte>.Shared;
                            var contiguous = pool.Rent((int)dataMessageLength);
                            try
                            {
                                scanBuffer.Slice(0, dataMessageLength).CopyTo(contiguous);
                                var contiguousSeq = new ReadOnlySequence<byte>(contiguous, 0, (int)dataMessageLength);
                                var contiguousReader = CreateProtocolReader(contiguousSeq);

                                var tableName = contiguousReader.ReadString();
                                var block = Block.ReadTypedBlockWithTableName(ref contiguousReader, registry, tableName, NegotiatedProtocolVersion, EffectiveMapShapeHintOrNull());

                                buffer = buffer.Slice(reader.Consumed + dataMessageLength);
                                message = new DataMessage { Block = block };
                                return true;
                            }
                            finally
                            {
                                pool.Return(contiguous);
                            }
                        }
                    }
                    else if (_compressionEnabled)
                    {
                        var scanBuffer = buffer.Slice(reader.Consumed);
                        if (!TryScanCompressedDataMessage(scanBuffer))
                            return false;
                    }

                    var dataMessage = ReadDataMessage(ref reader, registry);
                    buffer = buffer.Slice(reader.Consumed);
                    message = dataMessage;
                    return true;

                case ServerMessageType.Exception:
                {
                    // Scan-then-parse: validates the full ExceptionMessage is buffered
                    // before we commit to ExceptionMessage.Read (which would otherwise
                    // throw InvalidOperationException on a partial buffer and rely on
                    // the catch below to translate that into "incomplete"). The scan
                    // pass makes the contract explicit: false means incomplete (pump
                    // more bytes), throw means malformed (tear connection down).
                    var bodyBuffer = buffer.Slice(reader.Consumed);
                    var scanReader = CreateProtocolReader(bodyBuffer);
                    if (!ExceptionMessage.TryScan(ref scanReader))
                        return false;

                    var parseReader = CreateProtocolReader(bodyBuffer);
                    var exceptionMessage = ExceptionMessage.Read(ref parseReader);
                    buffer = bodyBuffer.Slice(parseReader.Consumed);
                    _pipeReader!.AdvanceTo(buffer.Start);
                    // Envelope fully consumed and advanced past: the server-side
                    // exception IS this response's terminator — boundary proven.
                    // Proof must stay colocated with the AdvanceTo above.
                    _boundaryProven = true;
                    throw ClickHouseServerException.FromExceptionMessage(exceptionMessage);
                }

                case ServerMessageType.Progress:
                {
                    var bodyBuffer = buffer.Slice(reader.Consumed);
                    var scanReader = CreateProtocolReader(bodyBuffer);
                    if (!ProgressMessage.TryScan(ref scanReader, NegotiatedProtocolVersion))
                        return false;

                    var parseReader = CreateProtocolReader(bodyBuffer);
                    var progressMessage = ProgressMessage.Read(ref parseReader, NegotiatedProtocolVersion);
                    buffer = bodyBuffer.Slice(parseReader.Consumed);
                    message = progressMessage;
                    return true;
                }

                case ServerMessageType.EndOfStream:
                    buffer = buffer.Slice(reader.Consumed);
                    message = EndOfStreamMessage.Instance;
                    return true;

                case ServerMessageType.ProfileInfo:
                {
                    // Scan-then-skip — see ProfileInfo branch in TryReadTypedMessage
                    // for the rationale (avoids leaning on catch-IOException for
                    // partial-data handling).
                    var bodyBuffer = buffer.Slice(reader.Consumed);
                    var scanReader = CreateProtocolReader(bodyBuffer);
                    if (!TrySkipProfileInfo(ref scanReader))
                        return false;

                    var parseReader = CreateProtocolReader(bodyBuffer);
                    SkipProfileInfo(ref parseReader);
                    buffer = bodyBuffer.Slice(parseReader.Consumed);
                    goto restart; // continue reading; iterative to avoid stack overflow
                }

                case ServerMessageType.ProfileEvents:
                    // ProfileEvents is a data block containing profiling information
                    // Read and discard it
                    // For uncompressed data, do a scan pass first
                    if (!_compressionEnabled)
                    {
                        var scanBuffer = buffer.Slice(reader.Consumed);
                        if (!TryScanUncompressedDataMessage(scanBuffer, out _))
                            return false;
                    }
                    else if (_compressionEnabled)
                    {
                        var scanBuffer = buffer.Slice(reader.Consumed);
                        if (!TryScanCompressedDataMessage(scanBuffer))
                            return false;
                    }
                    var profileEventsBlock = ReadDataMessage(ref reader, registry);
                    profileEventsBlock.Block.Dispose();
                    buffer = buffer.Slice(reader.Consumed);
                    goto restart; // continue reading; iterative to avoid stack overflow

                case ServerMessageType.Totals:
                case ServerMessageType.Extremes:
                    // These are data blocks, read them
                    // For uncompressed data, do a scan pass first
                    if (!_compressionEnabled)
                    {
                        var scanBuffer = buffer.Slice(reader.Consumed);
                        if (!TryScanUncompressedDataMessage(scanBuffer, out var specialMessageLength))
                            return false;

                        // Buffer into contiguous memory if fragmented for faster parsing
                        if (!scanBuffer.IsSingleSegment)
                        {
                            var pool = ArrayPool<byte>.Shared;
                            var contiguous = pool.Rent((int)specialMessageLength);
                            try
                            {
                                scanBuffer.Slice(0, specialMessageLength).CopyTo(contiguous);
                                var contiguousSeq = new ReadOnlySequence<byte>(contiguous, 0, (int)specialMessageLength);
                                var contiguousReader = CreateProtocolReader(contiguousSeq);

                                var tableName = contiguousReader.ReadString();
                                var block = Block.ReadTypedBlockWithTableName(ref contiguousReader, registry, tableName, NegotiatedProtocolVersion, EffectiveMapShapeHintOrNull());

                                buffer = buffer.Slice(reader.Consumed + specialMessageLength);
                                message = new DataMessage { Block = block };
                                return true;
                            }
                            finally
                            {
                                pool.Return(contiguous);
                            }
                        }
                    }
                    else if (_compressionEnabled)
                    {
                        var scanBuffer = buffer.Slice(reader.Consumed);
                        if (!TryScanCompressedDataMessage(scanBuffer))
                            return false;
                    }
                    var specialData = ReadDataMessage(ref reader, registry);
                    buffer = buffer.Slice(reader.Consumed);
                    message = specialData;
                    return true;

                case ServerMessageType.ReadTaskRequest:
                {
                    // Wire format (TCPHandler::sendReadTaskRequest): zero body
                    // beyond the message-type varint. Distributed read tasks
                    // emit this when asking the client to enqueue work; we
                    // don't participate in distributed task assignment, so
                    // just consume the byte and keep reading.
                    buffer = buffer.Slice(reader.Consumed);
                    message = Protocol.Messages.SkipMessage.Instance;
                    return true;
                }

                case ServerMessageType.PartUUIDs:
                {
                    // Wire format (TCPHandler::sendPartUUIDs): writeVectorBinary
                    // of UUIDs — varint count + N × 16 bytes. Drain so distributed
                    // SELECTs that emit part-uuid manifests don't poison the wire.
                    var bodyBuffer = buffer.Slice(reader.Consumed);
                    var scanReader = CreateProtocolReader(bodyBuffer);
                    if (!scanReader.TryReadVarInt(out var partCount))
                        return false;
                    var uuidBytes = checked((int)partCount * 16);
                    var consumedAfterVarInt = scanReader.Consumed;
                    if (bodyBuffer.Length < consumedAfterVarInt + uuidBytes)
                        return false;
                    buffer = bodyBuffer.Slice(consumedAfterVarInt + uuidBytes);
                    message = Protocol.Messages.SkipMessage.Instance;
                    return true;
                }

                case ServerMessageType.Log:
                    // Server-side log forwarding (set via SETTINGS send_logs_level).
                    // The body is a block — full Block decode requires the same
                    // context as Data messages and isn't worth implementing for
                    // a feature most clients disable. Surface a specific guidance
                    // message instead of the generic "malformed" default arm so
                    // operators know how to fix it.
                    throw new ClickHouseProtocolException(
                        "Server emitted a Log message (type 10). CH.Native does not consume server-side log " +
                        "forwarding; disable it via `SETTINGS send_logs_level = 'none'` on the query (or globally).");

                default:
                    // Unknown message-type byte — the stream is structurally
                    // malformed (or the server speaks a protocol we don't).
                    // Surface as a typed protocol exception so the catch below
                    // sets _protocolFatal and the pool discards this connection.
                    // Without this, the pump would keep reading the same byte
                    // and deadlock.
                    throw new ClickHouseProtocolException(
                        $"Unknown server message type 0x{(byte)messageType:X2} ({(int)messageType}); protocol stream is malformed.");
            }
        }
        catch (ClickHouseServerException)
        {
            // Server-side error from the ServerMessageType.Exception arm. That
            // arm advances the pipe past the exception body inline before
            // throwing, so the wire is in a clean state — the connection
            // stays usable for subsequent operations.
            throw;
        }
        catch (ClickHouseProtocolException)
        {
            // See TryReadTypedMessage — wire bytes were malformed, the stream is
            // out of sync, mark the connection as poison so the pool discards it.
            _protocolFatal = true;
            throw;
        }
        catch (InvalidOperationException)
        {
            // Not enough data yet
            return false;
        }
        catch (NotSupportedException ex)
        {
            // The reader factory rejected an unsupported column type mid-block:
            // wire bytes were partially consumed at an indeterminate offset, so
            // the connection is poisoned and eagerly closed by the message-pump
            // finally. Rethrow the same exception type with the consequence
            // appended so the ORIGINAL failure carries the full diagnosis,
            // rather than the user discovering a dead connection one query later.
            _protocolFatal = true;
            throw new NotSupportedException(
                $"{ex.Message} The connection has been closed because the response could not be fully read; " +
                "open a new connection (or take a fresh one from the pool) to continue.",
                ex);
        }
        catch (Exception)
        {
            // Any other exception (type-arg list malformed, value decode
            // overflowed, …) means wire bytes were partially consumed at an
            // indeterminate offset. Poison the connection so the pool discards
            // it and the next call surfaces a clean "Connection is broken"
            // instead of silently re-parsing stale bytes.
            _protocolFatal = true;
            throw;
        }
    }

    private DataMessage ReadDataMessage(ref ProtocolReader reader, ColumnReaderRegistry registry)
    {
        if (!_compressionEnabled)
        {
            // Scan pass already validated completeness in TryReadMessage
            // Just parse directly now - we know we have all the data
            var tableName = reader.ReadString();
            var block = Block.ReadTypedBlockWithTableName(ref reader, registry, tableName, NegotiatedProtocolVersion, EffectiveMapShapeHintOrNull());
            return new DataMessage { Block = block };
        }

        // Table name is read OUTSIDE the compressed block (per Python clickhouse-driver)
        var compressedTableName = reader.ReadString();

        // Check if the data is actually compressed by looking at the algorithm ID
        // The algorithm ID would be at offset 16 (after the 16-byte checksum)
        // Valid values: 0x82 (LZ4), 0x90 (Zstd)
        // Some messages (like ProfileEvents) may be sent uncompressed even when compression is enabled
        bool isCompressed = IsNextBlockCompressed(ref reader);

        if (!isCompressed)
        {
            // Data is not compressed - read directly
            return new DataMessage { Block = Block.ReadTypedBlockWithTableName(ref reader, registry, compressedTableName, NegotiatedProtocolVersion, EffectiveMapShapeHintOrNull()) };
        }

        // ClickHouse sends block data in multiple compressed chunks (typically 1MB each).
        // Read and decompress all chunks, concatenate, then parse the complete block.
        using var accumulator = new PooledBufferWriter(1024 * 1024);

        do
        {
            using var compressedData = CompressedBlock.ReadFromProtocol(ref reader);
            using var decompressed = CompressedBlock.DecompressPooled(compressedData.Span);

            var span = accumulator.GetSpan(decompressed.Length);
            decompressed.Span.CopyTo(span);
            accumulator.Advance(decompressed.Length);

            // Try to parse the accumulated decompressed data
            try
            {
                var seq = new ReadOnlySequence<byte>(accumulator.WrittenMemory);
                var blockReader = CreateProtocolReader(seq);
                return new DataMessage { Block = Block.ReadTypedBlockWithTableName(ref blockReader, registry, compressedTableName, NegotiatedProtocolVersion, EffectiveMapShapeHintOrNull()) };
            }
            catch (InvalidOperationException)
            {
                // Not enough decompressed data yet — read next compressed chunk
            }
        } while (IsNextBlockCompressed(ref reader));

        throw new InvalidDataException(
            $"Failed to parse block after decompressing all available compressed chunks ({accumulator.WrittenCount} bytes decompressed).");
    }

    /// <summary>
    /// Checks if the next block in the reader is compressed by examining the algorithm ID byte.
    /// </summary>
    private static bool IsNextBlockCompressed(ref ProtocolReader reader)
    {
        // A compressed block has:
        // - 16 bytes: checksum
        // - 1 byte: algorithm ID (0x82 = LZ4, 0x90 = Zstd)
        // We need at least 17 bytes to check
        if (reader.Remaining < 17)
            return false;

        // Peek at the algorithm ID byte at offset 16 (after checksum) without consuming bytes
        var algorithmId = reader.PeekByte(16);

        // Check if it's a valid compression algorithm
        return algorithmId == 0x82 || algorithmId == 0x90;
    }

    private void SkipProfileInfo(ref ProtocolReader reader)
    {
        // ProfileInfo structure (QueryPipeline/ProfileInfo.cpp::read):
        // - rows (VarInt)
        // - blocks (VarInt)
        // - bytes (VarInt)
        // - applied_limit (UInt8)
        // - rows_before_limit (VarInt)
        // - calculated_rows_before_limit (UInt8) — the server's obsolete_field
        // Added at 54469: applied_aggregation (UInt8) + rows_before_aggregation (VarInt)
        reader.ReadVarInt(); // rows
        reader.ReadVarInt(); // blocks
        reader.ReadVarInt(); // bytes
        reader.ReadByte();   // applied_limit
        reader.ReadVarInt(); // rows_before_limit
        reader.ReadByte();   // calculated_rows_before_limit / obsolete

        if (NegotiatedProtocolVersion >= ProtocolVersion.WithRowsBeforeAggregation)
        {
            reader.ReadByte();   // applied_aggregation
            reader.ReadVarInt(); // rows_before_aggregation
        }
    }

    /// <summary>
    /// Non-throwing scan that returns true iff the bytes for a complete ProfileInfo
    /// (per the negotiated protocol version) are buffered. Mirrors
    /// <see cref="SkipProfileInfo"/> field-for-field — keep them in sync. Used by
    /// the pumps to gate parse-or-wait without relying on a catch-IOException
    /// backstop conflating "incomplete" with any other parser failure.
    /// </summary>
    private bool TrySkipProfileInfo(ref ProtocolReader reader)
    {
        if (!reader.TrySkipVarInt()) return false; // rows
        if (!reader.TrySkipVarInt()) return false; // blocks
        if (!reader.TrySkipVarInt()) return false; // bytes
        if (!reader.TryReadByte(out _)) return false; // applied_limit
        if (!reader.TrySkipVarInt()) return false; // rows_before_limit
        if (!reader.TryReadByte(out _)) return false; // calculated_rows_before_limit / obsolete

        if (NegotiatedProtocolVersion >= ProtocolVersion.WithRowsBeforeAggregation)
        {
            if (!reader.TryReadByte(out _)) return false; // applied_aggregation
            if (!reader.TrySkipVarInt()) return false; // rows_before_aggregation
        }

        return true;
    }

    private static T? ConvertResult<T>(object? value)
    {
        if (value is null)
            return default;

        if (value is T typedValue)
            return typedValue;

        // Handle numeric conversions
        return (T)Convert.ChangeType(value, typeof(T));
    }

    private async Task CloseInternalAsync()
    {
        // Capture the prior state before the flip so the StateChange event
        // below reflects the real transition (Open→Closed, not Closed→Closed
        // if a double-close races in).
        var wasOpen = _isOpen;
        // Flip _isOpen under the lock so readers in CanBePooled / query
        // entrypoints see the transition atomically with _currentQueryId /
        // role-tracking clearing.
        lock (_queryLock)
        {
            _isOpen = false;
        }
        if (wasOpen)
        {
            // ADO contract: StateChange fires on every transition. Skip when
            // we were already closed so subscribers don't see a no-op event.
            OnStateChange(new System.Data.StateChangeEventArgs(
                System.Data.ConnectionState.Open, System.Data.ConnectionState.Closed));
        }
        _compressionEnabled = false;
        _schemaCache.Clear();
        // A fresh TCP session starts with the user's default roles; drop our tracking
        // so the next OpenAsync + query re-resolves from scratch. Sticky ChangeRolesAsync
        // overrides are dropped with the session — reconnecting is a fresh start.
        _currentServerRoles = null;
        _rolesExplicitlySet = false;
        _pinnedRoles = null;
        _pinnedRolesSet = false;

        if (_pipeWriter != null)
        {
            await _pipeWriter.CompleteAsync();
            _pipeWriter = null;
        }

        if (_pipeReader != null)
        {
            await _pipeReader.CompleteAsync();
            _pipeReader = null;
        }

        // Dispose SSL stream if used (this also closes the underlying network stream)
        if (_sslStream != null)
        {
            await _sslStream.DisposeAsync();
            _sslStream = null;
        }

        _networkStream = null;

        _tcpClient?.Dispose();
        _tcpClient = null;

        // Dispose custom CA certificate if loaded
        _customCaCertificate?.Dispose();
        _customCaCertificate = null;

        ServerInfo = null;
        NegotiatedProtocolVersion = 0;
    }

    /// <summary>
    /// Disposes the connection asynchronously.
    /// </summary>
    public override async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        // Pool-return path: if the DataSource has attached a return-to-pool hook,
        // route disposal through it instead of tearing down the socket. The hook
        // is one-shot (atomic swap) so a subsequent DisposeAsync — e.g. the pool
        // itself discarding the connection — falls through to the teardown path.
        var hook = Interlocked.Exchange(ref _poolReturnHook, null);
        if (hook is not null)
        {
            try
            {
                await hook(this).ConfigureAwait(false);
            }
            catch
            {
                // Hooks must be robust; if the pool-return path throws we fall back
                // to a real teardown so the caller's `await using` doesn't leak
                // a half-returned connection. Dispose `_writeLock` here too — pre-fix
                // this catch returned without disposing it, leaving a SemaphoreSlim
                // orphaned and any waiters blocked indefinitely instead of seeing
                // ObjectDisposedException.
                if (!_disposed)
                {
                    if (_isOpen)
                    {
                        ClickHouseMeter.DecrementConnections();
                        _logger.ConnectionClosed(Settings.Host);
                    }
                    _disposed = true;
                    await CloseInternalAsync().ConfigureAwait(false);
                    _writeLock.Dispose();
                }
            }
            return;
        }

        if (_isOpen)
        {
            ClickHouseMeter.DecrementConnections();
            _logger.ConnectionClosed(Settings.Host);
        }

        _disposed = true;
        await CloseInternalAsync();
        _writeLock.Dispose();
    }

    /// <summary>
    /// Checks if the connected server is ClickHouse 25.6 or later.
    /// Used to enable JSON flattened serialization format.
    /// </summary>
    private bool IsClickHouse25_6OrLater()
    {
        if (ServerInfo == null) return false;
        return ServerInfo.VersionMajor > 25 ||
               (ServerInfo.VersionMajor == 25 && ServerInfo.VersionMinor >= 6);
    }

    #region Bulk Insert Support

    /// <summary>
    /// Sends an INSERT query to the server with an initial empty block.
    /// The server will respond with a schema block defining the expected columns.
    /// </summary>
    /// <param name="sql">The INSERT SQL (e.g., "INSERT INTO table (col1, col2) VALUES").</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <param name="rolesOverride">Optional per-call role override.</param>
    /// <param name="queryId">Optional caller-supplied query ID. Null or empty auto-generates a GUID; max length 128.</param>
    /// <param name="settings">Optional per-query settings (e.g. <c>insert_deduplication_token</c>) sent in the query's settings section.</param>
    internal async Task SendInsertQueryAsync(
        string sql,
        CancellationToken cancellationToken,
        IReadOnlyList<string>? rolesOverride = null,
        string? queryId = null,
        IReadOnlyDictionary<string, string>? settings = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_isOpen)
            ThrowNotOpen();

        // Bulk insert bypasses SendQueryAsync, so apply role sync ourselves.
        if (!_inRoleSync)
            await EnsureRolesResolvedAsync(rolesOverride, cancellationToken);

        // Set compression state for response handling
        _compressionEnabled = Settings.Compress;
        // Mirror SendQueryAsync (line 1661) — clear stale cancel state from a
        // prior query so the bulk-path drain gate does not mis-trigger.

        var effectiveQueryId = ResolveQueryId(queryId);
        var queryMessage = QueryMessage.Create(
            sql,
            Settings.ClientName,
            Settings.Username,
            NegotiatedProtocolVersion,
            useCompression: Settings.Compress,
            parameters: null,
            settings: settings,
            queryId: effectiveQueryId);

        lock (_queryLock)
        {
            _currentQueryId = queryMessage.QueryId;
            _lastQueryId = queryMessage.QueryId;
        }

        var bufferWriter = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(bufferWriter);

        // Write query message
        queryMessage.Write(ref writer, NegotiatedProtocolVersion);

        // Write initial empty data block (required to trigger schema response)
        if (Settings.Compress)
        {
            var compressor = CompressedBlock.GetCompressor(Settings.CompressionMethod);
            Block.WriteEmpty(ref writer, compressor);
        }
        else
        {
            Block.WriteEmpty(ref writer);
        }


        await WriteAndFlushAsync(bufferWriter.WrittenMemory, cancellationToken);
    }

    /// <summary>
    /// Receives the schema block from the server after sending an INSERT query.
    /// The schema block has 0 rows but defines the column names and types.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The schema block with column definitions.</returns>
    internal async Task<TypedBlock> ReceiveSchemaBlockAsync(CancellationToken cancellationToken)
    {
        var registry = _columnReaderRegistry;


        while (true)
        {
            var result = await _pipeReader!.ReadAsync(cancellationToken);
            var buffer = result.Buffer;


            if (result.IsCanceled)
                throw new OperationCanceledException(cancellationToken);

            if (buffer.IsEmpty && result.IsCompleted)
                throw new ClickHouseConnectionException("Server closed connection while waiting for schema block");

            try
            {
                var reader = CreateProtocolReader(buffer);
                var messageType = (ServerMessageType)reader.ReadVarInt();


                if (messageType == ServerMessageType.Exception)
                {
                    var exceptionMessage = ExceptionMessage.Read(ref reader);
                    _pipeReader.AdvanceTo(buffer.GetPosition(reader.Consumed));
                    // Bulk-init failure (e.g. table missing): envelope consumed —
                    // boundary proven, so the connection stays reusable/retryable
                    // (pinned by BulkInserterInitFailureCleanupTests).
                    _boundaryProven = true;
                    throw ClickHouseServerException.FromExceptionMessage(exceptionMessage);
                }

                if (messageType == ServerMessageType.Data)
                {
                    // For uncompressed data, do a scan pass first
                    if (!_compressionEnabled)
                    {
                        var scanBuffer = buffer.Slice(reader.Consumed);
                        if (!TryScanUncompressedDataMessage(scanBuffer, out _))
                        {
                            // Not enough data yet
                            _pipeReader.AdvanceTo(buffer.Start, buffer.End);
                            continue;
                        }
                    }

                    var dataMessage = ReadDataMessage(ref reader, registry);
                    _pipeReader.AdvanceTo(buffer.GetPosition(reader.Consumed));
                    return dataMessage.Block;
                }

                if (messageType == ServerMessageType.TableColumns)
                {
                    // TableColumns message contains: external table name (string) + columns metadata (string)
                    // We skip both and continue waiting for the Data block (matching clickhouse-cpp behavior)
                    reader.ReadString(); // external table name
                    reader.ReadString(); // columns metadata
                    _pipeReader.AdvanceTo(buffer.GetPosition(reader.Consumed));

                    continue;
                }

                // Skip other message types and continue waiting
                _pipeReader.AdvanceTo(buffer.Start, buffer.End);
            }
            catch (InvalidOperationException)
            {
                // Not enough data yet, need more
                _pipeReader.AdvanceTo(buffer.Start, buffer.End);

                if (result.IsCompleted)
                    throw new ClickHouseConnectionException("Incomplete schema block response");
            }
        }
    }

    /// <summary>
    /// Sends a data block with column data for bulk insert.
    /// </summary>
    /// <param name="columnNames">Column names matching the schema.</param>
    /// <param name="columnTypes">ClickHouse type names matching the schema.</param>
    /// <param name="columnData">Column data arrays (column-major order).</param>
    /// <param name="rowCount">Number of rows in this batch.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    internal async Task SendDataBlockAsync(
        string[] columnNames,
        string[] columnTypes,
        object?[][] columnData,
        int rowCount,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_isOpen)
            ThrowNotOpen();

        var writerRegistry = ColumnWriterRegistry.Default;
        var bufferWriter = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(bufferWriter);

        // Write message type
        writer.WriteVarInt((ulong)Protocol.ClientMessageType.Data);

        // Write table name at Data message level (matching clickhouse-go's sendData structure)
        writer.WriteString(string.Empty);

        if (Settings.Compress)
        {
            var compressor = CompressedBlock.GetCompressor(Settings.CompressionMethod)!;
            Block.WriteDataCompressed(
                ref writer,
                columnNames,
                columnTypes,
                columnData,
                rowCount,
                writerRegistry,
                NegotiatedProtocolVersion,
                compressor);
        }
        else
        {
            Block.WriteData(
                ref writer,
                columnNames,
                columnTypes,
                columnData,
                rowCount,
                writerRegistry,
                NegotiatedProtocolVersion);
        }


        await WriteAndFlushAsync(bufferWriter.WrittenMemory, cancellationToken);
    }

    /// <summary>
    /// Sends an empty data block to signal the end of data for INSERT.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    internal async Task SendEmptyBlockAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_isOpen)
            ThrowNotOpen();

        var bufferWriter = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(bufferWriter);

        if (Settings.Compress)
        {
            var compressor = CompressedBlock.GetCompressor(Settings.CompressionMethod);
            Block.WriteEmpty(ref writer, compressor);
        }
        else
        {
            Block.WriteEmpty(ref writer);
        }


        await WriteAndFlushAsync(bufferWriter.WrittenMemory, cancellationToken);
    }

    /// <summary>
    /// Sends a data block using column extractors for direct-to-buffer writing (no boxing).
    /// </summary>
    /// <typeparam name="TRow">The row type.</typeparam>
    /// <param name="extractors">Column extractors.</param>
    /// <param name="rows">Source rows.</param>
    /// <param name="rowCount">Number of rows to write.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    internal async Task SendDataBlockDirectAsync<TRow>(
        IReadOnlyList<IColumnExtractor<TRow>> extractors,
        IReadOnlyList<TRow> rows,
        int rowCount,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_isOpen)
            ThrowNotOpen();

        // Estimate buffer size to avoid resize allocations during serialization
        // Heuristic: ~32 bytes per column per row + 1KB for protocol overhead
        // This is conservative but avoids most resize operations
        var estimatedSize = (rowCount * extractors.Count * 32) + 1024;
        var bufferWriter = BufferWriterPool.Shared.Rent(estimatedSize);

        try
        {
            var writer = new ProtocolWriter(bufferWriter);

            // Write message type
            writer.WriteVarInt((ulong)Protocol.ClientMessageType.Data);

            // Write table name at Data message level (matching clickhouse-go's sendData structure)
            writer.WriteString(string.Empty);

            if (Settings.Compress)
            {
                var compressor = CompressedBlock.GetCompressor(Settings.CompressionMethod)!;
                WriteDataBlockDirectCompressed(ref writer, extractors, rows, rowCount, compressor);
            }
            else
            {
                WriteDataBlockDirect(ref writer, extractors, rows, rowCount);
            }

            // OPTIMIZATION: Clear row references immediately after serialization, before await.
            // This allows GC to collect row objects during the network I/O await,
            // reducing Gen1 GC pressure by ensuring objects don't survive across await boundaries.
            if (rows is TRow[] rowArray)
            {
                Array.Clear(rowArray, 0, rowCount);
            }


            await WriteAndFlushAsync(bufferWriter.WrittenMemory, cancellationToken);
        }
        finally
        {
            BufferWriterPool.Shared.Return(bufferWriter);
        }
    }

    private void WriteDataBlockDirect<TRow>(
        ref ProtocolWriter writer,
        IReadOnlyList<IColumnExtractor<TRow>> extractors,
        IReadOnlyList<TRow> rows,
        int rowCount)
    {
        // NOTE: Table name is now written by the caller (SendDataBlockDirectAsync) at the Data message level

        // Block info
        BlockInfo.Default.Write(ref writer);

        // Column count and row count
        writer.WriteVarInt((ulong)extractors.Count);
        writer.WriteVarInt((ulong)rowCount);

        // Write each column directly from source data
        for (int i = 0; i < extractors.Count; i++)
        {
            var extractor = extractors[i];
            writer.WriteString(extractor.ColumnName);
            writer.WriteString(extractor.TypeName);

            // Custom serialization byte: server expects this for protocol >= 54454
            if (NegotiatedProtocolVersion >= Protocol.ProtocolVersion.WithCustomSerialization)
            {
                writer.WriteByte(0); // hasCustom = false
            }

            // Write column data directly - no intermediate arrays, no boxing
            if (rowCount > 0)
            {
                extractor.ExtractAndWrite(ref writer, rows, rowCount);
            }
        }
    }

    private void WriteDataBlockDirectCompressed<TRow>(
        ref ProtocolWriter writer,
        IReadOnlyList<IColumnExtractor<TRow>> extractors,
        IReadOnlyList<TRow> rows,
        int rowCount,
        ICompressor compressor)
    {
        // NOTE: Table name is now written by the caller (SendDataBlockDirectAsync) at the Data message level

        // Estimate buffer size for uncompressed data to avoid resize allocations
        var estimatedSize = (rowCount * extractors.Count * 32) + 1024;
        var uncompressedBuffer = BufferWriterPool.Shared.Rent(estimatedSize);

        try
        {
            var tempWriter = new ProtocolWriter(uncompressedBuffer);

            // Write block info
            BlockInfo.Default.Write(ref tempWriter);

            // Column count and row count
            tempWriter.WriteVarInt((ulong)extractors.Count);
            tempWriter.WriteVarInt((ulong)rowCount);

            // Write each column
            for (int i = 0; i < extractors.Count; i++)
            {
                var extractor = extractors[i];
                tempWriter.WriteString(extractor.ColumnName);
                tempWriter.WriteString(extractor.TypeName);

                // Custom serialization byte: server expects this for protocol >= 54454
                if (NegotiatedProtocolVersion >= Protocol.ProtocolVersion.WithCustomSerialization)
                {
                    tempWriter.WriteByte(0); // hasCustom = false
                }

                // Write column data
                if (rowCount > 0)
                {
                    extractor.ExtractAndWrite(ref tempWriter, rows, rowCount);
                }
            }

            // Compress and write (using pooled buffers to reduce GC pressure)
            using var compressed = CompressedBlock.CompressPooled(uncompressedBuffer.WrittenSpan, compressor);
            writer.WriteBytes(compressed.Span);
        }
        finally
        {
            BufferWriterPool.Shared.Return(uncompressedBuffer);
        }
    }

    /// <summary>
    /// Waits for the EndOfStream message from the server after INSERT completes.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    internal async Task ReceiveEndOfStreamAsync(CancellationToken cancellationToken)
    {
        var registry = _columnReaderRegistry;


        while (true)
        {
            var result = await _pipeReader!.ReadAsync(cancellationToken);
            var buffer = result.Buffer;


            if (result.IsCanceled)
                throw new OperationCanceledException(cancellationToken);

            if (buffer.IsEmpty && result.IsCompleted)
                throw new ClickHouseConnectionException("Server closed connection while waiting for INSERT completion");

            try
            {
                var reader = CreateProtocolReader(buffer);
                var messageType = (ServerMessageType)reader.ReadVarInt();


                switch (messageType)
                {
                    case ServerMessageType.EndOfStream:
                    {
                        // Advance past the EOS byte first, then check whether the
                        // same buffer contained any trailing bytes — same L4 invariant
                        // as the main pump (see ReadServerMessagesAsync). After a clean
                        // INSERT EOS the server must not send anything else for this
                        // query; trailing bytes mean the stream is out of sync.
                        var afterEos = buffer.GetPosition(reader.Consumed);
                        var trailing = buffer.Slice(afterEos);
                        if (!trailing.IsEmpty)
                        {
                            var trailingLength = trailing.Length;
                            _pipeReader.AdvanceTo(buffer.End);
                            _protocolFatal = true;
                            throw new ClickHouseProtocolException(
                                $"Server sent {trailingLength} byte(s) after EndOfStream during INSERT; protocol stream is out of sync.");
                        }
                        _pipeReader.AdvanceTo(afterEos);
                        // INSERT completed: clear the query id so the connection is
                        // pool-eligible again (the bulk path bypasses ReadServerMessagesAsync,
                        // whose finally would otherwise clear it). Without this a successful
                        // bulk insert leaves _currentQueryId set and CanBePooledBeforeReset
                        // stays false, so the pool discards every bulk connection.
                        lock (_queryLock) { _currentQueryId = null; }
                        // Terminator consumed cleanly (trailing check above passed)
                        // — boundary proven.
                        _boundaryProven = true;
                        return;
                    }

                    case ServerMessageType.Exception:
                    {
                        // Scan-then-parse — see TryReadMessage for the rationale.
                        // Without TryScan a partial Exception throws InvalidOperationException
                        // and the catch below treats it as "not enough bytes" — which
                        // works but conflates incomplete data with any other parser bug.
                        var bodyBuffer = buffer.Slice(reader.Consumed);
                        var scanReader = CreateProtocolReader(bodyBuffer);
                        if (!ExceptionMessage.TryScan(ref scanReader))
                        {
                            _pipeReader.AdvanceTo(buffer.Start, buffer.End);
                            if (result.IsCompleted)
                                throw new ClickHouseConnectionException("Incomplete INSERT response from server");
                            continue;
                        }

                        var parseReader = CreateProtocolReader(bodyBuffer);
                        var exceptionMessage = ExceptionMessage.Read(ref parseReader);
                        _pipeReader.AdvanceTo(bodyBuffer.GetPosition(parseReader.Consumed));
                        // INSERT rejected: envelope consumed — boundary proven.
                        _boundaryProven = true;
                        throw ClickHouseServerException.FromExceptionMessage(exceptionMessage);
                    }

                    case ServerMessageType.Progress:
                    {
                        var bodyBuffer = buffer.Slice(reader.Consumed);
                        var scanReader = CreateProtocolReader(bodyBuffer);
                        if (!ProgressMessage.TryScan(ref scanReader, NegotiatedProtocolVersion))
                        {
                            _pipeReader.AdvanceTo(buffer.Start, buffer.End);
                            if (result.IsCompleted)
                                throw new ClickHouseConnectionException("Incomplete INSERT response from server");
                            continue;
                        }

                        var parseReader = CreateProtocolReader(bodyBuffer);
                        _ = ProgressMessage.Read(ref parseReader, NegotiatedProtocolVersion);
                        _pipeReader.AdvanceTo(bodyBuffer.GetPosition(parseReader.Consumed));
                        break;
                    }

                    case ServerMessageType.Data:
                        // Skip any data messages (e.g., empty confirmation blocks)
                        // For uncompressed data, do a scan pass first
                        if (!_compressionEnabled)
                        {
                            var scanBuffer = buffer.Slice(reader.Consumed);
                            if (!TryScanUncompressedDataMessage(scanBuffer, out _))
                            {
                                _pipeReader.AdvanceTo(buffer.Start, buffer.End);
                                continue;
                            }
                        }
                        var dataMsg = ReadDataMessage(ref reader, registry);
                        dataMsg.Block.Dispose();
                        _pipeReader.AdvanceTo(buffer.GetPosition(reader.Consumed));
                        break;

                    case ServerMessageType.ProfileInfo:
                    {
                        // Scan-then-skip: removes the last catch-IOException
                        // dependency in this dispatch. A partial ProfileInfo now
                        // returns "wait for more bytes" cleanly instead of going
                        // through the catch backstop.
                        var bodyBuffer = buffer.Slice(reader.Consumed);
                        var scanReader = CreateProtocolReader(bodyBuffer);
                        if (!TrySkipProfileInfo(ref scanReader))
                        {
                            _pipeReader.AdvanceTo(buffer.Start, buffer.End);
                            if (result.IsCompleted)
                                throw new ClickHouseConnectionException("Incomplete INSERT response from server");
                            continue;
                        }

                        var parseReader = CreateProtocolReader(bodyBuffer);
                        SkipProfileInfo(ref parseReader);
                        _pipeReader.AdvanceTo(bodyBuffer.GetPosition(parseReader.Consumed));
                        break;
                    }

                    case ServerMessageType.ProfileEvents:
                        // For uncompressed data, do a scan pass first
                        if (!_compressionEnabled)
                        {
                            var scanBuffer = buffer.Slice(reader.Consumed);
                            if (!TryScanUncompressedDataMessage(scanBuffer, out _))
                            {
                                _pipeReader.AdvanceTo(buffer.Start, buffer.End);
                                continue;
                            }
                        }
                        var profileMsg = ReadDataMessage(ref reader, registry);
                        profileMsg.Block.Dispose();
                        _pipeReader.AdvanceTo(buffer.GetPosition(reader.Consumed));
                        break;

                    case ServerMessageType.TableColumns:
                        // TableColumns message: external table name (string) + columns metadata (string).
                        // Scan first so a partial message returns to the read loop instead of
                        // throwing InvalidOperationException through the catch below — same
                        // contract reasoning as Exception/Progress above.
                        {
                            var bodyBuffer = buffer.Slice(reader.Consumed);
                            var scanReader = CreateProtocolReader(bodyBuffer);
                            if (!scanReader.TrySkipString() || !scanReader.TrySkipString())
                            {
                                _pipeReader.AdvanceTo(buffer.Start, buffer.End);
                                if (result.IsCompleted)
                                    throw new ClickHouseConnectionException("Incomplete INSERT response from server");
                                continue;
                            }

                            var parseReader = CreateProtocolReader(bodyBuffer);
                            parseReader.ReadString();
                            parseReader.ReadString();
                            _pipeReader.AdvanceTo(bodyBuffer.GetPosition(parseReader.Consumed));
                        }
                        break;

                    default:
                        // Unknown message-type byte during the INSERT response — same
                        // L1 defence as the main pump. Without this, an unknown type
                        // would either throw the generic ClickHouseException (which
                        // does NOT set _protocolFatal — pool would reuse a poisoned
                        // connection) or fall through to the catch and silently retry.
                        _protocolFatal = true;
                        throw new ClickHouseProtocolException(
                            $"Unknown server message type 0x{(byte)messageType:X2} ({(int)messageType}) during INSERT; protocol stream is malformed.");
                }
            }
            catch (ClickHouseProtocolException)
            {
                // Already typed as a protocol exception (L1 default arm or L4 trailing
                // bytes); _protocolFatal is set above, just propagate.
                throw;
            }
            catch (InvalidOperationException)
            {
                // Defensive backstop. With L1 (default arm), L3 (Exception, Progress,
                // ProfileInfo, TableColumns scan-then-parse), and the existing
                // scan-then-parse for Data / ProfileEvents, no message-type branch
                // here SHOULD throw InvalidOperationException for "incomplete data" —
                // every parse path is gated by a TryScan that returned true. A
                // throw here would mean either (a) a parser invariant broke between
                // scan and parse, or (b) a future message-type branch was added
                // without a matching TryScan. Treat as "not enough data" (the
                // pre-L3 contract) so we don't deadlock, but the failure mode is
                // worth investigating if you see it in logs.
                _pipeReader.AdvanceTo(buffer.Start, buffer.End);

                if (result.IsCompleted)
                    throw new ClickHouseConnectionException("Incomplete INSERT response from server");
            }
        }
    }

    #endregion
}
