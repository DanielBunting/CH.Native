using CH.Native.Connection;
using Dapper;
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
            "/events/count, /events/dapper, /events/dapper-typed, /replica/server, POST /events/bulk, " +
            "/diag/pool, /ping/{key}, /health, /health/ready.");

        app.MapGet("/events/count", async (ClickHouseDataSource ds, CancellationToken ct) =>
        {
            await using var conn = await ds.OpenConnectionAsync(ct);
            // numbers(N) is the bounded table *function* — unlike the system.numbers
            // table it needs no SELECT grant, so the default DataSource's demo_user
            // (default role NONE — see the RBAC notes in README) can read it without
            // activating a role.
            var count = await conn.ExecuteScalarAsync<ulong>(
                "SELECT count() FROM numbers(10)",
                cancellationToken: ct);
            return Http.Ok(new { count });
        });

        // Dapper-via-DI: the connection rented from the DataSource IS a DbConnection
        // (since ClickHouseConnection now derives from DbConnection), so Dapper's
        // IDbConnection-bound extension methods bind directly. Pool, credential
        // providers, keyed services — all the things you get from DI continue to
        // work; you just call .QueryAsync<T> / .ExecuteScalarAsync<T> on the rent.
        app.MapGet("/events/dapper", async (ClickHouseDataSource ds, CancellationToken ct) =>
        {
            await using var conn = await ds.OpenConnectionAsync(ct);
            var n = await conn.ExecuteScalarAsync<ulong>(new CommandDefinition(
                "SELECT count() FROM numbers(10)",
                cancellationToken: ct));
            return Http.Ok(new { count = n, via = "dapper" });
        });

        // Dapper QueryAsync<T> against a system table — exercises column→property
        // mapping and parameter binding on a pooled, DI-resolved connection.
        // The same call shape works for the keyed DataSources above; see
        // /replica/server for the keyed-injection pattern.
        //
        // Parameter names avoid `limit` / `offset` — ClickHouse 26.x's parser
        // misinterprets `{limit:Type}` / `{offset:Type}` as the start of a
        // LIMIT/OFFSET clause and rejects the query with CANNOT_PARSE_QUOTED_STRING.
        // CH.Native fails fast with a clear error if you try to use either name.
        app.MapGet("/events/dapper-typed", async (ClickHouseDataSource ds, CancellationToken ct) =>
        {
            await using var conn = await ds.OpenConnectionAsync(ct);
            var rows = await conn.QueryAsync<NumberRow>(new CommandDefinition(
                "SELECT toUInt64(number) AS value FROM numbers(@max_rows) WHERE number >= @min_number",
                new { max_rows = 10, min_number = 3 },
                cancellationToken: ct));
            return Http.Ok(new { values = rows });
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
            // CREATE TABLE + INSERT need privileges demo_user only holds via admin_role,
            // which is default-role NONE (see the RBAC notes in README). Activate it for
            // this request and run both the DDL and the bulk insert on that same
            // connection. ChangeRolesAsync pins a sticky role override, so the pool
            // discards this connection on return — the documented cost of per-request RBAC.
            await using var conn = await ds.OpenConnectionAsync(ct);
            await conn.ChangeRolesAsync(new[] { "admin_role" }, ct);

            await conn.ExecuteNonQueryAsync(
                "CREATE TABLE IF NOT EXISTS sample_events (event_id UUID, ts DateTime, payload String) ENGINE = Memory",
                cancellationToken: ct);

            await using var inserter = conn.CreateBulkInserter<EventRow>("sample_events");
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
            // The default AddClickHouse(...) registers a non-keyed DataSource, so the
            // "default" key resolves through GetRequiredService; named registrations
            // (primary/replica/mtls/ssh/adhoc) resolve through the keyed container.
            var ds = key is "default"
                ? sp.GetRequiredService<ClickHouseDataSource>()
                : sp.GetRequiredKeyedService<ClickHouseDataSource>(key);
            return Http.Ok(new { key, healthy = await ds.PingAsync(ct) });
        });

        return app;
    }

    // Dapper maps by column name; MatchNamesWithUnderscores is set by
    // ClickHouseDapperIntegration.Register() at startup so `value` maps to `Value`.
    private sealed class NumberRow
    {
        public ulong Value { get; set; }
    }
}
