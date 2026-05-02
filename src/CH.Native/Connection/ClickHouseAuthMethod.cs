namespace CH.Native.Connection;

/// <summary>
/// Authentication method used during the ClickHouse native protocol handshake.
/// </summary>
public enum ClickHouseAuthMethod
{
    /// <summary>
    /// Plain username + password. Server may route this to any password-based auth
    /// backend it has configured (plaintext, sha256, double-sha1, bcrypt, scram,
    /// LDAP, Kerberos, HTTP) — all indistinguishable on the wire.
    /// </summary>
    Password = 0,

    /// <summary>
    /// JWT bearer token. Client emits the magic marker " JWT AUTHENTICATION "
    /// in the username slot and the token in the password slot. In OSS builds
    /// the server rejects this with "JWT is available only in ClickHouse Cloud";
    /// use with ClickHouse Cloud or a Cloud-compatible build.
    /// </summary>
    /// <remarks>
    /// <para>
    /// SECURITY: the token travels in the password slot of the handshake — i.e.
    /// in cleartext at the application layer. Always pair JWT auth with TLS
    /// (<c>WithTls()</c>) to encrypt the wire; without TLS, the token is
    /// trivially recoverable from packet captures. The settings object retains
    /// the token in memory for the connection's lifetime — treat it like any
    /// other long-lived secret.
    /// </para>
    /// <para>
    /// LIFETIME: <c>IClickHouseJwtProvider.GetTokenAsync</c> is invoked once
    /// per physical connection, not per query. Connections are reused for up
    /// to <c>ClickHouseDataSourceOptions.ConnectionLifetime</c> (default
    /// 30 min). Set <c>ConnectionLifetime</c> at or below your JWT's
    /// time-to-live so a stale token doesn't survive into a query — if it
    /// does, the server's authentication-failed response now surfaces as
    /// <c>ClickHouseAuthenticationException</c> (R6) and short-circuits the
    /// retry policy, but the user still sees the failure.
    /// </para>
    /// </remarks>
    Jwt = 1,

    /// <summary>
    /// SSH key challenge/response. Requires server protocol revision &gt;= 54466
    /// (ClickHouse 23.9+). Client emits marker " SSH KEY AUTHENTICATION " followed
    /// by the configured username and signs a server-issued challenge with an
    /// SSH private key (RSA, Ed25519, or ECDSA).
    /// </summary>
    /// <remarks>
    /// The private-key bytes live in the immutable settings object for the
    /// connection's lifetime. .NET does not provide a guaranteed-zero memory
    /// primitive for managed byte arrays, so the client cannot fully wipe the
    /// key from memory on dispose. If your threat model includes post-process
    /// memory inspection, prefer the file-path overload + an OS-level secret
    /// store, or rotate keys frequently.
    /// </remarks>
    SshKey = 2,

    /// <summary>
    /// TLS client certificate (mTLS). The certificate supplied via
    /// <see cref="ClickHouseConnectionSettingsBuilder.WithTlsClientCertificate(System.Security.Cryptography.X509Certificates.X509Certificate2)"/>
    /// is presented during the TLS handshake; the server matches the certificate
    /// common name against the user's configured <c>ssl_certificates</c> entry.
    /// Requires TLS to be enabled.
    /// </summary>
    TlsClientCertificate = 3
}
