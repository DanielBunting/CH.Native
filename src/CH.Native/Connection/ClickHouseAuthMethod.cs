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
    Jwt = 1,

    /// <summary>
    /// SSH key challenge/response. Requires server protocol revision &gt;= 54466
    /// (ClickHouse 23.9+). Client emits marker " SSH KEY AUTHENTICATION " followed
    /// by the configured username and signs a server-issued challenge with an
    /// SSH private key (RSA, Ed25519, or ECDSA).
    /// </summary>
    SshKey = 2,

    /// <summary>
    /// TLS client certificate (mTLS). The certificate supplied via
    /// <see cref="ClickHouseConnectionSettingsBuilder.WithTlsClientCertificate"/>
    /// is presented during the TLS handshake; the server matches the certificate
    /// common name against the user's configured <c>ssl_certificates</c> entry.
    /// Requires TLS to be enabled.
    /// </summary>
    TlsClientCertificate = 3
}
