using CH.Native.Connection;

namespace CH.Native.DependencyInjection;

/// <summary>
/// Fail-fast validator for <see cref="ClickHouseConnectionOptions"/>. Invoked
/// at the time an <c>AddClickHouse(IConfiguration)</c> registration captures
/// the bound POCO snapshot — pre-fix misconfigured pool sizes and
/// auth/credential pairings only surfaced at the first
/// <c>OpenConnectionAsync</c> as opaque <c>ArgumentOutOfRangeException</c>s.
/// </summary>
public static class ClickHouseConnectionOptionsValidator
{
    /// <summary>
    /// Throws <see cref="ArgumentException"/> with a consolidated message
    /// listing every validation failure, or returns silently if the options
    /// are well-formed.
    /// </summary>
    public static void ValidateOrThrow(ClickHouseConnectionOptions options, string? sectionPath = null)
    {
        var failures = new List<string>();

        // Pool bounds — empty default is fine; mis-configured pool is not.
        var pool = options.Pool;
        if (pool is not null)
        {
            if (pool.MaxPoolSize is < 1)
                failures.Add($"Pool.MaxPoolSize must be >= 1 (was {pool.MaxPoolSize}).");
            if (pool.MinPoolSize is < 0)
                failures.Add($"Pool.MinPoolSize must be >= 0 (was {pool.MinPoolSize}).");
            if (pool.MaxPoolSize is int max && pool.MinPoolSize is int min && min > max)
                failures.Add($"Pool.MinPoolSize ({min}) must be <= Pool.MaxPoolSize ({max}).");
            if (pool.ConnectionWaitTimeout is { } wait && wait < TimeSpan.Zero)
                failures.Add($"Pool.ConnectionWaitTimeout must be non-negative (was {wait}).");
            if (pool.ConnectionLifetime is { } life && life < TimeSpan.Zero)
                failures.Add($"Pool.ConnectionLifetime must be non-negative (was {life}).");
            if (pool.ConnectionIdleTimeout is { } idle && idle < TimeSpan.Zero)
                failures.Add($"Pool.ConnectionIdleTimeout must be non-negative (was {idle}).");
        }

        // Auth method ↔ credential pairing — when the connection string is
        // present we trust that path entirely (it has its own validator).
        if (string.IsNullOrEmpty(options.ConnectionString))
        {
            switch (options.AuthMethod)
            {
                case ClickHouseAuthMethod.Jwt:
                    if (string.IsNullOrEmpty(options.JwtToken))
                        failures.Add("AuthMethod=Jwt requires JwtToken (or an IClickHouseJwtProvider registration).");
                    break;
                case ClickHouseAuthMethod.SshKey:
                    if (string.IsNullOrEmpty(options.SshPrivateKeyPath))
                        failures.Add("AuthMethod=SshKey requires SshPrivateKeyPath (or an IClickHouseSshKeyProvider registration).");
                    break;
            }
        }

        // Port bounds.
        if (options.Port is { } port && (port < 1 || port > 65535))
            failures.Add($"Port must be in [1, 65535] (was {port}).");
        if (options.TlsPort is { } tlsPort && (tlsPort < 1 || tlsPort > 65535))
            failures.Add($"TlsPort must be in [1, 65535] (was {tlsPort}).");

        if (failures.Count == 0) return;

        var prefix = string.IsNullOrEmpty(sectionPath)
            ? "ClickHouse connection options validation failed: "
            : $"ClickHouse connection options ('{sectionPath}') validation failed: ";
        throw new ArgumentException(prefix + string.Join(" ", failures), nameof(options));
    }
}
