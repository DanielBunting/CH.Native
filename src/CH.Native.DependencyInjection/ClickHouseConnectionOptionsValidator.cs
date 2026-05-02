using CH.Native.Connection;

namespace CH.Native.DependencyInjection;

/// <summary>
/// Validator for <see cref="ClickHouseConnectionOptions"/>. Validation is split
/// across two methods so that auth/credential pairing can be deferred until
/// chained <c>WithJwtProvider&lt;&gt;()</c> / <c>WithSshKeyProvider&lt;&gt;()</c>
/// registrations have run on the builder returned from <c>AddClickHouse</c>.
/// <para>
/// <see cref="ValidateOrThrow"/> runs at registration time and pins shape
/// errors (pool bounds, port ranges) immediately. <see cref="ValidateAuthCredentialsOrThrow"/>
/// runs at first DataSource resolution and pairs <see cref="ClickHouseConnectionOptions.AuthMethod"/>
/// against the chained provider state on the builder.
/// </para>
/// </summary>
public static class ClickHouseConnectionOptionsValidator
{
    /// <summary>
    /// Throws <see cref="ArgumentException"/> with a consolidated message
    /// listing every shape failure (pool bounds, port ranges), or returns
    /// silently if the options are well-formed. Auth/credential pairing is
    /// deferred — see <see cref="ValidateAuthCredentialsOrThrow"/>.
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

        // Port bounds.
        if (options.Port is { } port && (port < 1 || port > 65535))
            failures.Add($"Port must be in [1, 65535] (was {port}).");
        if (options.TlsPort is { } tlsPort && (tlsPort < 1 || tlsPort > 65535))
            failures.Add($"TlsPort must be in [1, 65535] (was {tlsPort}).");

        ThrowIfAny(failures, sectionPath);
    }

    /// <summary>
    /// Validates that <see cref="ClickHouseConnectionOptions.AuthMethod"/> has
    /// a credential source — either a static value on the POCO or a chained
    /// provider registration on the builder. Called from the DataSource
    /// singleton factory once the user's <c>WithJwtProvider&lt;&gt;()</c> /
    /// <c>WithSshKeyProvider&lt;&gt;()</c> calls have populated the builder.
    /// <para>
    /// Skipped entirely when <see cref="ClickHouseConnectionOptions.ConnectionString"/>
    /// is non-empty — the connection-string parser owns auth pairing in that path.
    /// </para>
    /// </summary>
    internal static void ValidateAuthCredentialsOrThrow(
        ClickHouseConnectionOptions options,
        ClickHouseDataSourceBuilder builder,
        string? sectionPath = null)
    {
        if (!string.IsNullOrEmpty(options.ConnectionString)) return;

        var failures = new List<string>();

        switch (options.AuthMethod)
        {
            case ClickHouseAuthMethod.Jwt:
                if (string.IsNullOrEmpty(options.JwtToken) && builder.JwtProviderFactory is null)
                    failures.Add("AuthMethod=Jwt requires JwtToken (or an IClickHouseJwtProvider registration).");
                break;
            case ClickHouseAuthMethod.SshKey:
                if (string.IsNullOrEmpty(options.SshPrivateKeyPath) && builder.SshKeyProviderFactory is null)
                    failures.Add("AuthMethod=SshKey requires SshPrivateKeyPath (or an IClickHouseSshKeyProvider registration).");
                break;
        }

        ThrowIfAny(failures, sectionPath);
    }

    private static void ThrowIfAny(List<string> failures, string? sectionPath)
    {
        if (failures.Count == 0) return;

        var prefix = string.IsNullOrEmpty(sectionPath)
            ? "ClickHouse connection options validation failed: "
            : $"ClickHouse connection options ('{sectionPath}') validation failed: ";
        throw new ArgumentException(prefix + string.Join(" ", failures), "options");
    }
}
