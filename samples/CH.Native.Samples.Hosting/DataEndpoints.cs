using CH.Native.Connection;
using Http = Microsoft.AspNetCore.Http.Results;

namespace CH.Native.Samples.Hosting;

/// <summary>
/// Endpoints that exercise the non-auth surface area of the DI integration:
/// resolving DataSources (default + keyed), running queries, bulk inserting
/// rows from the pool, and inspecting pool / health state.
/// </summary>
internal static class DataEndpoints
{
    public static WebApplication MapDataEndpoints(this WebApplication app)
    {
        app.MapGet("/", () =>
            "CH.Native Hosting sample — try /auth/{password|jwt|ssh|cert} (optionally ?role=admin_role), " +
            "/events/count, /replica/server, POST /events/bulk, /diag/pool, /ping/{key}, /health, /health/ready.");

        app.MapGet("/events/count", async (ClickHouseDataSource ds, CancellationToken ct) =>
        {
            await using var conn = await ds.OpenConnectionAsync(ct);
            // system.numbers is unbounded, so the inner LIMIT is required.
            var count = await conn.ExecuteScalarAsync<ulong>(
                "SELECT count() FROM (SELECT * FROM system.numbers LIMIT 10)",
                cancellationToken: ct);
            return Http.Ok(new { count });
        });

        app.MapGet("/replica/server", async (
            [FromKeyedServices("replica")] ClickHouseDataSource replica,
            CancellationToken ct) =>
        {
            await using var conn = await replica.OpenConnectionAsync(ct);
            var version = await conn.ExecuteScalarAsync<string>("SELECT version()", cancellationToken: ct);
            return Http.Ok(new { version });
        });

        app.MapPost("/events/bulk", async (
            ClickHouseDataSource ds,
            List<EventRow> rows,
            CancellationToken ct) =>
        {
            await using (var setup = await ds.OpenConnectionAsync(ct))
            {
                await setup.ExecuteNonQueryAsync(
                    "CREATE TABLE IF NOT EXISTS sample_events (event_id UUID, ts DateTime, payload String) ENGINE = Memory",
                    cancellationToken: ct);
            }

            await using var inserter = await ds.CreateBulkInserterAsync<EventRow>("sample_events", cancellationToken: ct);
            await inserter.InitAsync(ct);
            var total = 0;
            foreach (var row in rows)
            {
                await inserter.AddAsync(row, ct);
                total++;
            }
            await inserter.CompleteAsync(ct);
            return Http.Accepted(value: new { inserted = total });
        });

        app.MapGet("/diag/pool", (ClickHouseDataSource ds) => Http.Ok(ds.GetStatistics()));

        app.MapGet("/ping/{key}", async (string key, IServiceProvider sp, CancellationToken ct) =>
        {
            var ds = sp.GetRequiredKeyedService<ClickHouseDataSource>(key);
            return Http.Ok(new { key, healthy = await ds.PingAsync(ct) });
        });

        return app;
    }
}
