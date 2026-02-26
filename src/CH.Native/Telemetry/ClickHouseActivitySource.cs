using System.Diagnostics;
using CH.Native.Exceptions;
using CH.Native.Protocol.Messages;

namespace CH.Native.Telemetry;

/// <summary>
/// ActivitySource for CH.Native distributed tracing.
/// </summary>
public static class ClickHouseActivitySource
{
    /// <summary>
    /// The name of the ActivitySource.
    /// </summary>
    public const string Name = "CH.Native";

    /// <summary>
    /// The version of the ActivitySource.
    /// </summary>
    public const string Version = "1.0.0";

    /// <summary>
    /// The shared ActivitySource instance.
    /// </summary>
    public static readonly ActivitySource Source = new(Name, Version);

    /// <summary>
    /// Starts a new Activity for a ClickHouse query.
    /// </summary>
    /// <param name="sql">The SQL statement being executed.</param>
    /// <param name="queryId">The query ID for correlation.</param>
    /// <param name="database">The database name.</param>
    /// <param name="settings">Telemetry settings to check if tracing is enabled.</param>
    /// <returns>A new Activity, or null if tracing is disabled or not sampled.</returns>
    public static Activity? StartQuery(
        string sql,
        string? queryId = null,
        string? database = null,
        TelemetrySettings? settings = null)
    {
        if (settings?.EnableTracing == false)
            return null;

        var activity = Source.StartActivity("clickhouse.query", ActivityKind.Client);
        if (activity == null)
            return null;

        // OpenTelemetry semantic conventions for database spans
        activity.SetTag("db.system", "clickhouse");

        if (database != null)
            activity.SetTag("db.name", database);

        if (settings?.IncludeSqlInTraces != false)
            activity.SetTag("db.statement", SqlSanitizer.Sanitize(sql));

        if (queryId != null)
            activity.SetTag("db.clickhouse.query_id", queryId);

        return activity;
    }

    /// <summary>
    /// Starts a new Activity for establishing a ClickHouse connection.
    /// </summary>
    /// <param name="host">The server hostname.</param>
    /// <param name="port">The server port.</param>
    /// <param name="settings">Telemetry settings to check if tracing is enabled.</param>
    /// <returns>A new Activity, or null if tracing is disabled or not sampled.</returns>
    public static Activity? StartConnect(
        string host,
        int port,
        TelemetrySettings? settings = null)
    {
        if (settings?.EnableTracing == false)
            return null;

        var activity = Source.StartActivity("clickhouse.connect", ActivityKind.Client);
        if (activity == null)
            return null;

        activity.SetTag("db.system", "clickhouse");
        activity.SetTag("server.address", host);
        activity.SetTag("server.port", port);

        return activity;
    }

    /// <summary>
    /// Starts a new Activity for a ClickHouse bulk insert operation.
    /// </summary>
    /// <param name="tableName">The table being inserted into.</param>
    /// <param name="database">The database name.</param>
    /// <param name="settings">Telemetry settings to check if tracing is enabled.</param>
    /// <returns>A new Activity, or null if tracing is disabled or not sampled.</returns>
    public static Activity? StartBulkInsert(
        string tableName,
        string? database = null,
        TelemetrySettings? settings = null)
    {
        if (settings?.EnableTracing == false)
            return null;

        var activity = Source.StartActivity("clickhouse.bulk_insert", ActivityKind.Client);
        if (activity == null)
            return null;

        activity.SetTag("db.system", "clickhouse");

        if (database != null)
            activity.SetTag("db.name", database);

        activity.SetTag("db.clickhouse.table", tableName);

        return activity;
    }

    /// <summary>
    /// Sets server information tags on an Activity after a successful connection.
    /// </summary>
    /// <param name="activity">The Activity to update.</param>
    /// <param name="serverInfo">The ServerHello response containing server information.</param>
    public static void SetServerInfo(Activity? activity, ServerHello serverInfo)
    {
        if (activity == null)
            return;

        activity.SetTag("db.clickhouse.server_name", serverInfo.ServerName);
        activity.SetTag("db.clickhouse.server_version",
            $"{serverInfo.VersionMajor}.{serverInfo.VersionMinor}");

        if (serverInfo.Timezone != null)
            activity.SetTag("db.clickhouse.timezone", serverInfo.Timezone);
    }

    /// <summary>
    /// Sets query result metrics on an Activity.
    /// </summary>
    /// <param name="activity">The Activity to update.</param>
    /// <param name="rowsRead">The number of rows read.</param>
    /// <param name="bytesRead">The number of bytes read.</param>
    public static void SetQueryResults(Activity? activity, long rowsRead, long bytesRead)
    {
        if (activity == null)
            return;

        activity.SetTag("db.clickhouse.rows_read", rowsRead);
        activity.SetTag("db.clickhouse.bytes_read", bytesRead);
    }

    /// <summary>
    /// Records an error on an Activity.
    /// </summary>
    /// <param name="activity">The Activity to update.</param>
    /// <param name="ex">The exception that occurred.</param>
    public static void SetError(Activity? activity, Exception ex)
    {
        if (activity == null)
            return;

        activity.SetStatus(ActivityStatusCode.Error, ex.Message);
        activity.SetTag("error.type", ex.GetType().FullName);
        activity.SetTag("error.message", ex.Message);

        if (ex is ClickHouseServerException serverEx)
        {
            activity.SetTag("db.clickhouse.error_code", serverEx.ErrorCode);
        }
    }
}
