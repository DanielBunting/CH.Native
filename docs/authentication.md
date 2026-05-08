# Authentication

CH.Native supports four authentication methods, all on the native binary handshake:

| Method | Builder | Connection-string | Notes |
|---|---|---|---|
| **Password** | `WithPassword` / `WithCredentials` | `Password=…` | Default. Server may route to plaintext / sha256 / bcrypt / LDAP / Kerberos — indistinguishable on the wire. |
| **JWT** | `WithJwt` / `WithBearerToken` | `Jwt=…` (or `Token`, `BearerToken`, `Bearer`) | ClickHouse Cloud only. OSS server rejects with `JWT is available only in ClickHouse Cloud`. |
| **SSH key** | `WithSshKey(bytes, passphrase?)` / `WithSshKeyPath(path, passphrase?)` | `SshKeyPath=…;SshKeyPassphrase=…` | Requires server protocol revision ≥ 54466 (ClickHouse 23.9+) and an `<ssh_keys>` user entry. |
| **mTLS** | `WithTls` + `WithTlsClientCertificate(cert)` + `WithAuthMethod(TlsClientCertificate)` | TLS keys + cert path | Server matches the cert's CN against the user's `<ssl_certificates>` entry. |

The four methods are mutually exclusive — calling more than one of `WithPassword`, `WithJwt`, `WithSshKey*`, or `WithAuthMethod(TlsClientCertificate)` on the same builder throws `InvalidOperationException` at `Build()`.

## Password

The default. No special configuration:

```csharp
var settings = ClickHouseConnectionSettings.CreateBuilder()
    .WithHost("clickhouse.internal")
    .WithCredentials("app_user", Environment.GetEnvironmentVariable("CH_PASSWORD")!)
    .Build();
```

Or via connection string:

```
Host=clickhouse.internal;Username=app_user;Password=…
```

## JWT (ClickHouse Cloud)

```csharp
var settings = ClickHouseConnectionSettings.CreateBuilder()
    .WithHost("xxx.clickhouse.cloud")
    .WithTls()                        // *** required — see security note below ***
    .WithJwt(token)
    .Build();
```

`WithBearerToken(token)` is a synonym matching `clickhouse-cs`'s vocabulary; the value transmitted is identical.

**Security:** the JWT travels in the password slot of the handshake — i.e. cleartext at the application layer. **Always pair JWT with TLS** (`WithTls()`). Without TLS the token is trivially recoverable from packet captures.

For rotating tokens, use `IClickHouseJwtProvider` from `CH.Native.DependencyInjection` rather than holding the token in settings — see [Dependency Injection](dependency-injection.md#rotating-credentials).

## SSH key

```csharp
var settings = ClickHouseConnectionSettings.CreateBuilder()
    .WithHost("clickhouse.internal")
    .WithUsername("ssh_user")
    .WithSshKeyPath("/etc/secrets/ch.key", passphrase: null)
    .Build();
```

In-memory variant for keys loaded from a vault:

```csharp
var settings = ClickHouseConnectionSettings.CreateBuilder()
    .WithHost("clickhouse.internal")
    .WithUsername("ssh_user")
    .WithSshKey(privateKeyBytes, passphrase: "…")
    .Build();
```

Supported key formats: PEM and OpenSSH; algorithms RSA, Ed25519, and ECDSA. The server must have an `<ssh_keys><ssh_key><base64_key>…</base64_key></ssh_key></ssh_keys>` entry under the user in `users.xml` / `users.d/*.xml`.

**Lifetime note:** the private-key bytes live in the immutable settings object for the connection's lifetime. .NET cannot guarantee zeroing managed byte arrays on dispose. If your threat model includes post-process memory inspection, prefer the file-path overload (paired with an OS-level secret store) or rotate keys frequently.

## mTLS (TLS client certificate)

```csharp
using var clientCert = new X509Certificate2("/etc/secrets/client.pfx", "pfx-password");

var settings = ClickHouseConnectionSettings.CreateBuilder()
    .WithHost("clickhouse.internal")
    .WithTls()
    .WithTlsPort(9440)
    .WithUsername("cert_user")
    .WithTlsClientCertificate(clientCert)
    .WithAuthMethod(ClickHouseAuthMethod.TlsClientCertificate)
    .Build();
```

Or load straight from the file:

```csharp
.WithTlsClientCertificate("/etc/secrets/client.pfx", "pfx-password")
```

The certificate **must** include a private key — TLS requires the client to sign the handshake. A public-only certificate is rejected with `ArgumentException` at builder time.

The explicit `WithAuthMethod(ClickHouseAuthMethod.TlsClientCertificate)` is what tells the server to use cert-based authentication (as opposed to a password-based user who happens to be connecting over mTLS). Skip it and the server will fall back to password auth.

## Role activation

Default ClickHouse roles can be activated for every query on the connection:

```csharp
.WithRoles("analyst")                       // single role
.WithRoles("analyst", "admin_role")        // multiple
.WithRoles(Array.Empty<string>())          // explicit SET ROLE NONE
```

Or in the connection string:

```
…;Roles=analyst,admin_role
```

Leave unset (or pass `null`) to keep the server's login-time defaults. Each query starts with the configured roles applied — this is the right place to grant elevated permissions for a specific connection without changing the user's defaults globally.

### Per-request role activation (`ChangeRolesAsync`)

`ClickHouseConnection.ChangeRolesAsync` flips the active role set on an open connection — useful for per-request RBAC where the caller's role isn't known at pool-rent time:

```csharp
await using var conn = await dataSource.OpenConnectionAsync(ct);
await conn.ChangeRolesAsync(new[] { "analyst" }, ct);
// queries on this connection now run with `analyst` active
```

Pass `null` to revert to the connection's configured defaults; pass an empty list for an explicit `SET ROLE NONE`.

**Pool trade-off:** when a rented connection has had `ChangeRolesAsync` called against it, the pool considers it poisoned and discards it on return rather than handing it to another caller (`CanBePooled` returns `false`). That's the documented price for using a pooled `ClickHouseDataSource` to serve per-request roles — see [Connection Pooling — Connection poison detection](connection-pooling.md#connection-poison-detection). For workloads where the role set is stable across requests, `WithRoles(...)` at connection-build time is cheaper.

## Rotating credentials in DI apps

For long-running services where credentials rotate (JWTs, mTLS certs from Key Vault, SSH keys from Vault), don't hold the secret in `ClickHouseConnectionSettings`. Use the provider interfaces from `CH.Native.DependencyInjection`:

- `IClickHouseJwtProvider` — `ValueTask<string> GetTokenAsync(CancellationToken)`
- `IClickHouseSshKeyProvider` — `ValueTask<SshKeyMaterial> GetKeyAsync(CancellationToken)`
- `IClickHouseCertificateProvider` — `ValueTask<X509Certificate2> GetCertificateAsync(CancellationToken)`
- `IClickHousePasswordProvider` — `ValueTask<string> GetPasswordAsync(CancellationToken)`

Providers are invoked once per **physical** connection (cold start, post-eviction rent), not per query. The `ConnectionLifetime` pool setting bounds how stale a credential can be — set it shorter than your token TTL. See [Dependency Injection — rotating credentials](dependency-injection.md#rotating-credentials) for the full pattern.

## See also

- [Configuration](configuration.md) — connection-string keys and builder methods
- [Dependency Injection](dependency-injection.md) — provider interfaces and DI wiring
- [Connection Pooling](connection-pooling.md) — `ConnectionLifetime` and rotation timing
- `samples/CH.Native.Samples.Hosting` — ASP.NET sample with endpoint probes for all four auth methods (password / JWT / SSH / mTLS) plus a per-request role-activation demo. The bundled `docker/setup.sh` + `docker compose up` provisions roles, certs, and SSH keys against a local ClickHouse so every endpoint actually handshakes.
