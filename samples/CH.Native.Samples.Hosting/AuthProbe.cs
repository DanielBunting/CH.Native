using CH.Native.Connection;
using CH.Native.Exceptions;
using Http = Microsoft.AspNetCore.Http.Results;

namespace CH.Native.Samples.Hosting;

/// <summary>
/// Shared probe used by every <c>/auth/{method}</c> endpoint. Opens a pooled
/// <see cref="ClickHouseDataSource"/> connection, optionally activates roles
/// for the request, then runs <c>SELECT currentUser/version/currentRoles</c>
/// plus a grant-gated CREATE/DROP probe so the response makes the user's
/// effective grants visible.
/// </summary>
/// <remarks>
/// Per-request role activation goes through <c>ChangeRolesAsync</c>, which pins
/// a sticky role override on the connection. The pool's <c>CanBePooled</c>
/// returns false for connections with that override, so a request that
/// activates roles costs one fresh physical connection — the documented
/// trade-off for per-request RBAC against a pooled DataSource.
/// </remarks>
internal static class AuthProbe
{
    public static async Task<IResult> RunAsync(
        string method,
        ClickHouseDataSource ds,
        string? roleParam,
        CancellationToken ct)
    {
        ClickHouseConnection conn;
        try
        {
            conn = await ds.OpenConnectionAsync(ct);
        }
        catch (Exception ex)
        {
            // Surface handshake failures as 200 with an `error` field so callers
            // can curl every endpoint without bash error-handling. JWT against
            // OSS lands here, as do mtls/ssh before docker setup has run.
            return Http.Ok(new { method, error = ex.GetType().Name, message = ex.Message });
        }

        await using (conn)
        {
            if (!string.IsNullOrEmpty(roleParam))
            {
                var roles = roleParam.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                await conn.ChangeRolesAsync(roles, ct);
            }

            var user = await conn.ExecuteScalarAsync<string>("SELECT currentUser()", cancellationToken: ct);
            var version = await conn.ExecuteScalarAsync<string>("SELECT version()", cancellationToken: ct);
            var activeRoles = await conn.ExecuteScalarAsync<string>(
                "SELECT arrayStringConcat(currentRoles(), ',')",
                cancellationToken: ct);

            string probe;
            string? hint = null;
            try
            {
                await conn.ExecuteNonQueryAsync(
                    "CREATE TABLE IF NOT EXISTS sample_rbac_probe (x UInt8) ENGINE=Memory",
                    cancellationToken: ct);
                await conn.ExecuteNonQueryAsync(
                    "DROP TABLE IF EXISTS sample_rbac_probe",
                    cancellationToken: ct);
                probe = "OK";
            }
            catch (ClickHouseServerException ex) when (ex.ErrorCode == 497)
            {
                probe = "ACCESS_DENIED";
                hint = "pass ?role=admin_role to activate the privileged role";
            }

            return Http.Ok(new
            {
                method,
                user,
                version,
                activeRoles = string.IsNullOrEmpty(activeRoles) ? "(none)" : activeRoles,
                rbacProbe = probe,
                hint,
            });
        }
    }
}
