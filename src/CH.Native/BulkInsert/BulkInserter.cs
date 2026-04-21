using System.Buffers;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;
using CH.Native.Connection;
using CH.Native.Data;
using CH.Native.Exceptions;
using CH.Native.Mapping;
using CH.Native.Telemetry;

namespace CH.Native.BulkInsert;

/// <summary>
/// Provides high-performance bulk insert operations using the ClickHouse native protocol.
/// </summary>
/// <typeparam name="T">The POCO type representing a row. Must be a class with a parameterless constructor.</typeparam>
public sealed class BulkInserter<T> : IAsyncDisposable where T : class
{
    private readonly ClickHouseConnection _connection;
    private readonly string _tableName;
    private readonly BulkInsertOptions _options;
    private readonly List<T> _buffer;

    private string[]? _columnNames;
    private string[]? _columnTypes;
    private Func<T, object?>[]? _getters;
    private bool _initialized;
    private bool _completed;
    private bool _disposed;
    private int _totalRowsInserted;
    private bool _usedCachedSchema;
    private SchemaKey _schemaCacheKey;

    // Pooled column data arrays - reused across flushes (fallback path)
    private object?[][]? _pooledColumnData;
    private int _pooledArraySize;

    // Direct-to-buffer extractors - avoid boxing for most types
    private IColumnExtractor<T>[]? _extractors;
    private bool _useDirectPath;

    /// <summary>
    /// Creates a new bulk inserter for the specified table.
    /// </summary>
    /// <param name="connection">The ClickHouse connection to use.</param>
    /// <param name="tableName">The table name to insert into.</param>
    /// <param name="options">Optional bulk insert options.</param>
    public BulkInserter(ClickHouseConnection connection, string tableName, BulkInsertOptions? options = null)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _tableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        _options = options ?? BulkInsertOptions.Default;
        _buffer = new List<T>(_options.BatchSize);
    }

    /// <summary>
    /// Gets the number of rows currently buffered.
    /// </summary>
    public int BufferedCount => _buffer.Count;

    /// <summary>
    /// Initializes the bulk inserter by sending the INSERT query and receiving the schema.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <remarks>
    /// When <see cref="BulkInsertOptions.UseSchemaCache"/> is enabled and a matching schema
    /// is cached on the connection, this method still sends the INSERT query (required for
    /// server-side protocol context) but skips the synchronous wait for the schema Data
    /// block. The server's schema response is drained later by
    /// <see cref="ClickHouseConnection.ReceiveEndOfStreamAsync"/>, saving one round-trip.
    /// </remarks>
    public async Task InitAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_initialized)
            throw new InvalidOperationException("BulkInserter is already initialized.");

        // Build column list from POCO properties
        var propertyMappings = GetPropertyMappings();
        var columnList = string.Join(", ", propertyMappings.Select(p => p.ColumnName));
        _schemaCacheKey = new SchemaKey(_tableName, columnList);

        // Send INSERT query (required for server-side protocol state even on cache hit)
        var sql = $"INSERT INTO {_tableName} ({columnList}) VALUES";
        // Snapshot to IReadOnlyList so the wire path doesn't observe later mutation.
        var rolesSnapshot = _options.Roles is null ? null : (IReadOnlyList<string>)_options.Roles.ToArray();
        await _connection.SendInsertQueryAsync(
            sql,
            cancellationToken,
            rolesOverride: rolesSnapshot,
            queryId: _options.QueryId);

        // Per-call override wins; null falls back to the connection setting.
        var useCache = _options.UseSchemaCache ?? _connection.Settings.UseSchemaCache;

        if (useCache &&
            _connection.SchemaCache.TryGet(_schemaCacheKey, out var cachedSchema))
        {
            // Cache hit: use cached schema, skip the schema read round-trip.
            // The server's schema Data block will be drained by ReceiveEndOfStreamAsync.
            MapPropertiesToSchema(propertyMappings, cachedSchema.ColumnNames, cachedSchema.ColumnTypes);
            _usedCachedSchema = true;
            _connection.Logger.BulkInsertSchemaFetched(_tableName, cachedSchema.ColumnNames.Length, fromCache: true);
        }
        else
        {
            // Cache miss (or caching disabled): wait for server schema, then map.
            var schemaBlock = await _connection.ReceiveSchemaBlockAsync(cancellationToken);
            var names = schemaBlock.ColumnNames;
            var types = schemaBlock.ColumnTypes;
            MapPropertiesToSchema(propertyMappings, names, types);

            if (useCache)
            {
                _connection.SchemaCache.Set(_schemaCacheKey, new BulkInsertSchema(names, types));
            }

            _connection.Logger.BulkInsertSchemaFetched(_tableName, names.Length, fromCache: false);
        }

        _initialized = true;
    }

    /// <summary>
    /// Adds a single item to the buffer. Automatically flushes when batch size is reached.
    /// </summary>
    /// <param name="item">The item to add.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async ValueTask AddAsync(T item, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_initialized)
            throw new InvalidOperationException("BulkInserter must be initialized before adding items.");
        if (_completed)
            throw new InvalidOperationException("BulkInserter has been completed and cannot accept more items.");

        _buffer.Add(item);

        if (_buffer.Count >= _options.BatchSize)
        {
            await FlushAsync(cancellationToken);
        }
    }

    /// <summary>
    /// Adds multiple items to the buffer. Automatically flushes when batch size is reached.
    /// </summary>
    /// <param name="items">The items to add.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async ValueTask AddRangeAsync(IEnumerable<T> items, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_initialized)
            throw new InvalidOperationException("BulkInserter must be initialized before adding items.");
        if (_completed)
            throw new InvalidOperationException("BulkInserter has been completed and cannot accept more items.");

        foreach (var item in items)
        {
            _buffer.Add(item);

            if (_buffer.Count >= _options.BatchSize)
            {
                await FlushAsync(cancellationToken);
            }
        }
    }

    /// <summary>
    /// Adds multiple items using direct-to-wire streaming with reduced GC pressure.
    /// This method bypasses the internal List buffer and uses pooled arrays instead,
    /// reducing Gen1 GC collections for large streaming inserts.
    /// </summary>
    /// <param name="items">The items to add.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async ValueTask AddRangeStreamingAsync(IEnumerable<T> items, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_initialized)
            throw new InvalidOperationException("BulkInserter must be initialized before adding items.");
        if (_completed)
            throw new InvalidOperationException("BulkInserter has been completed and cannot accept more items.");

        // If we don't have the direct path available, fall back to regular AddRangeAsync
        if (!_useDirectPath || _extractors == null)
        {
            await AddRangeAsync(items, cancellationToken);
            return;
        }

        // Use pooled array to accumulate rows without long-lived List<T> references
        var batchSize = _options.BatchSize;
        var batch = ArrayPool<T>.Shared.Rent(batchSize);

        try
        {
            var batchIndex = 0;

            foreach (var item in items)
            {
                batch[batchIndex++] = item;

                if (batchIndex >= batchSize)
                {
                    // Send directly without copying to List<T>
                    await _connection.SendDataBlockDirectAsync(
                        _extractors,
                        batch,
                        batchIndex,
                        cancellationToken);

                    // Clear references immediately to allow GC of row objects
                    Array.Clear(batch, 0, batchIndex);
                    batchIndex = 0;
                }
            }

            // Flush any remaining rows
            if (batchIndex > 0)
            {
                await _connection.SendDataBlockDirectAsync(
                    _extractors,
                    batch,
                    batchIndex,
                    cancellationToken);
            }
        }
        finally
        {
            // Return pooled array with clearing to release object references
            ArrayPool<T>.Shared.Return(batch, clearArray: true);
        }
    }

    /// <summary>
    /// Adds multiple items from an async enumerable using direct-to-wire streaming with reduced GC pressure.
    /// This method bypasses the internal List buffer and uses pooled arrays instead,
    /// reducing Gen1 GC collections for large streaming inserts.
    /// </summary>
    /// <param name="items">The async enumerable of items to add.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async ValueTask AddRangeStreamingAsync(IAsyncEnumerable<T> items, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_initialized)
            throw new InvalidOperationException("BulkInserter must be initialized before adding items.");
        if (_completed)
            throw new InvalidOperationException("BulkInserter has been completed and cannot accept more items.");

        // If we don't have the direct path available, fall back to adding one by one
        if (!_useDirectPath || _extractors == null)
        {
            await foreach (var item in items.WithCancellation(cancellationToken))
            {
                await AddAsync(item, cancellationToken);
            }
            return;
        }

        // Use pooled array to accumulate rows without long-lived List<T> references
        var batchSize = _options.BatchSize;
        var batch = ArrayPool<T>.Shared.Rent(batchSize);

        try
        {
            var batchIndex = 0;

            await foreach (var item in items.WithCancellation(cancellationToken))
            {
                batch[batchIndex++] = item;

                if (batchIndex >= batchSize)
                {
                    // Send directly without copying to List<T>
                    await _connection.SendDataBlockDirectAsync(
                        _extractors,
                        batch,
                        batchIndex,
                        cancellationToken);

                    // Clear references immediately to allow GC of row objects
                    Array.Clear(batch, 0, batchIndex);
                    batchIndex = 0;
                }
            }

            // Flush any remaining rows
            if (batchIndex > 0)
            {
                await _connection.SendDataBlockDirectAsync(
                    _extractors,
                    batch,
                    batchIndex,
                    cancellationToken);
            }
        }
        finally
        {
            // Return pooled array with clearing to release object references
            ArrayPool<T>.Shared.Return(batch, clearArray: true);
        }
    }

    /// <summary>
    /// Flushes the current buffer to the server.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_initialized)
            throw new InvalidOperationException("BulkInserter must be initialized before flushing.");

        if (_buffer.Count == 0)
            return;

        using var activity = ClickHouseActivitySource.Source.StartActivity("clickhouse.bulk_insert.flush", ActivityKind.Client);
        if (activity != null)
        {
            activity.SetTag("db.system", "clickhouse");
            activity.SetTag("db.clickhouse.table", _tableName);
            activity.SetTag("db.clickhouse.rows", _buffer.Count);
        }

        try
        {
            if (_useDirectPath && _extractors != null)
            {
                // Fast path: direct-to-buffer writing (no boxing)
                await _connection.SendDataBlockDirectAsync(
                    _extractors,
                    _buffer,
                    _buffer.Count,
                    cancellationToken);
            }
            else
            {
                // Fallback path: extract column data from buffer (with boxing)
                var columnData = ExtractColumnData();

                // Send data block
                await _connection.SendDataBlockAsync(
                    _columnNames!,
                    _columnTypes!,
                    columnData,
                    _buffer.Count,
                    cancellationToken);
            }

            _totalRowsInserted += _buffer.Count;
            _connection.Logger.BulkInsertFlushed(_tableName, _buffer.Count);
        }
        catch (Exception ex)
        {
            ClickHouseActivitySource.SetError(activity, ex);
            throw;
        }

        _buffer.Clear();
    }

    /// <summary>
    /// Completes the bulk insert operation by flushing any remaining data and finalizing.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task CompleteAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_initialized)
            throw new InvalidOperationException("BulkInserter must be initialized before completing.");
        if (_completed)
            return;

        using var activity = ClickHouseActivitySource.StartBulkInsert(_tableName, _connection.Settings.Database, _connection.Settings.Telemetry);

        try
        {
            // Flush any remaining items
            await FlushAsync(cancellationToken);

            // Send empty block to signal end of data
            await _connection.SendEmptyBlockAsync(cancellationToken);

            // Wait for server confirmation
            await _connection.ReceiveEndOfStreamAsync(cancellationToken);

            activity?.SetTag("db.clickhouse.rows", _totalRowsInserted);

            _completed = true;
        }
        catch (ClickHouseServerException ex) when (_usedCachedSchema)
        {
            // Server-side rejection on the cached-schema path is almost always schema drift
            // (e.g. post-ALTER column removal, rename, or type change). Evict the entry so
            // the next inserter refreshes, and surface the error to the caller.
            _connection.SchemaCache.InvalidateTable(_tableName);
            ClickHouseActivitySource.SetError(activity, ex);
            throw;
        }
        catch (Exception ex)
        {
            ClickHouseActivitySource.SetError(activity, ex);
            throw;
        }
    }

    /// <summary>
    /// Disposes the bulk inserter. If not completed, sends the end signal to the server.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        if (_initialized && !_completed)
        {
            try
            {
                await CompleteAsync();
            }
            catch
            {
                // Ignore errors during dispose cleanup
            }
        }

        // Return pooled arrays
        ReturnPooledArrays();
        _buffer.Clear();
    }

    private List<PropertyMapping> GetPropertyMappings()
    {
        // Use reflection-based mapping
        var properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead)
            .Select(p =>
            {
                // Check deprecated ColumnAttribute
#pragma warning disable CS0618 // Type or member is obsolete
                var legacyAttr = p.GetCustomAttribute<ColumnAttribute>();
                if (legacyAttr?.Ignore == true)
                    return null;
#pragma warning restore CS0618

                // Check new ClickHouseColumnAttribute
                var newAttr = p.GetCustomAttribute<ClickHouseColumnAttribute>();

                // Prefer new attribute, fall back to legacy
                var columnName = newAttr?.Name ?? legacyAttr?.Name ?? p.Name;
                var order = newAttr?.Order ?? legacyAttr?.Order ?? int.MaxValue;

                return new PropertyMapping
                {
                    Property = p,
                    ColumnName = columnName,
                    Order = order
                };
            })
            .Where(m => m != null)
            .Cast<PropertyMapping>()
            .OrderBy(m => m.Order)
            .ThenBy(m => m.ColumnName, StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (properties.Count == 0)
            throw new InvalidOperationException($"Type {typeof(T).Name} has no readable properties for bulk insert.");

        return properties;
    }

    private void MapPropertiesToSchema(
        List<PropertyMapping> propertyMappings,
        string[] schemaColumnNames,
        string[] schemaColumnTypes)
    {
        // Build lookup from schema
        var schemaColumns = new Dictionary<string, (int Index, string Type)>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < schemaColumnNames.Length; i++)
        {
            schemaColumns[schemaColumnNames[i]] = (i, schemaColumnTypes[i]);
        }

        // Match properties to schema columns
        var matchedNames = new List<string>();
        var matchedTypes = new List<string>();
        var matchedGetters = new List<Func<T, object?>>();
        var matchedExtractors = new List<IColumnExtractor<T>>();
        var canUseDirectPath = true;

        foreach (var mapping in propertyMappings)
        {
            if (!schemaColumns.TryGetValue(mapping.ColumnName, out var schemaInfo))
            {
                var propertyInfo = mapping.Property != null ? $"from property '{mapping.Property.Name}' " : "";
                throw new InvalidOperationException(
                    $"Column '{mapping.ColumnName}' {propertyInfo}not found in table schema. " +
                    $"Available columns: {string.Join(", ", schemaColumnNames)}");
            }

            matchedNames.Add(mapping.ColumnName);
            matchedTypes.Add(schemaInfo.Type);

            matchedGetters.Add(CreateGetter(mapping.Property!));

            // Try to create a typed extractor for direct-to-buffer writing
            try
            {
                var extractor = ColumnExtractorFactory.Create<T>(
                    mapping.Property!,
                    mapping.ColumnName,
                    schemaInfo.Type);
                matchedExtractors.Add(extractor);
            }
            catch (NotSupportedException)
            {
                // Type not supported for direct path (e.g., arrays, maps, tuples)
                canUseDirectPath = false;
            }
        }

        _columnNames = matchedNames.ToArray();
        _columnTypes = matchedTypes.ToArray();
        _getters = matchedGetters.ToArray();

        if (canUseDirectPath && matchedExtractors.Count == matchedNames.Count)
        {
            _extractors = matchedExtractors.ToArray();
            _useDirectPath = true;
        }
    }

    private static Func<T, object?> CreateGetter(PropertyInfo property)
    {
        // Create a compiled expression for fast property access
        var parameter = Expression.Parameter(typeof(T), "obj");
        var propertyAccess = Expression.Property(parameter, property);
        var convert = Expression.Convert(propertyAccess, typeof(object));
        var lambda = Expression.Lambda<Func<T, object?>>(convert, parameter);
        return lambda.Compile();
    }

    private object?[][] ExtractColumnData()
    {
        var columnCount = _columnNames!.Length;
        var rowCount = _buffer.Count;

        // Use pooled arrays to reduce GC pressure
        EnsurePooledArrays(columnCount, rowCount);

        // Extract values row by row using reflection-based getters
        for (int row = 0; row < rowCount; row++)
        {
            var item = _buffer[row];
            for (int col = 0; col < columnCount; col++)
            {
                _pooledColumnData![col][row] = _getters![col](item);
            }
        }

        return _pooledColumnData!;
    }

    private void EnsurePooledArrays(int columnCount, int rowCount)
    {
        // Check if we need to (re)allocate
        if (_pooledColumnData == null ||
            _pooledColumnData.Length < columnCount ||
            _pooledArraySize < rowCount)
        {
            // Return existing arrays to pool
            ReturnPooledArrays();

            // Rent new arrays from ArrayPool
            // Use BatchSize as the minimum to avoid frequent re-rentals
            var arraySize = Math.Max(rowCount, _options.BatchSize);
            _pooledColumnData = new object?[columnCount][];

            for (int col = 0; col < columnCount; col++)
            {
                _pooledColumnData[col] = ArrayPool<object?>.Shared.Rent(arraySize);
            }

            _pooledArraySize = arraySize;
        }
        else
        {
            // Clear only the portion we'll use (important for nullable correctness)
            for (int col = 0; col < columnCount; col++)
            {
                Array.Clear(_pooledColumnData[col], 0, rowCount);
            }
        }
    }

    private void ReturnPooledArrays()
    {
        if (_pooledColumnData != null)
        {
            for (int col = 0; col < _pooledColumnData.Length; col++)
            {
                if (_pooledColumnData[col] != null)
                {
                    ArrayPool<object?>.Shared.Return(_pooledColumnData[col], clearArray: true);
                }
            }
            _pooledColumnData = null;
            _pooledArraySize = 0;
        }
    }

    private sealed class PropertyMapping
    {
        public PropertyInfo? Property { get; init; }
        public required string ColumnName { get; init; }
        public int Order { get; init; }
    }
}
