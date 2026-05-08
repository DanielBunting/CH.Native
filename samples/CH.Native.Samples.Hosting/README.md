# CH.Native.Samples.Hosting

End-to-end ASP.NET sample that combines authentication and dependency injection.
Replaces the previous `CH.Native.Samples.Authentication` and
`CH.Native.Samples.DependencyInjection` projects, running everything against a
single docker overlay so the keyed `mtls` / `ssh` DataSources actually handshake.

## One-time setup

```bash
cd docker
./setup.sh              # generates ./docker/generated/{certs,keys,users.d,initdb,config.d}
docker compose up -d    # boots clickhouse-server:24.3 with those overlays mounted
cd ..
```

`setup.sh` provisions a self-signed CA, a server cert for `localhost`, a client
cert with `CN=cert_user`, and an RSA SSH keypair for `ssh_user`. It also writes
the SQL that creates `demo_user` / `ssh_user` / `cert_user`, the `analyst` and
`admin_role` roles, and pins them with default-role NONE. Nothing is committed
— `./docker/generated/` is gitignored.

## Run

```bash
dotnet run --project samples/CH.Native.Samples.Hosting
```

Then exercise the endpoints:

| Endpoint | Demonstrates |
|----------|--------------|
| `GET /` | Endpoint index |
| `GET /auth/password` | Default DataSource — static password from `appsettings.json:ClickHouse` |
| `GET /auth/password?role=admin_role` | Per-request role activation via `ChangeRolesAsync` (CREATE/DROP probe = OK) |
| `GET /auth/password?role=analyst` | Same, but `analyst` only has SELECT — CREATE/DROP probe = ACCESS_DENIED |
| `GET /auth/jwt` | Keyed `primary` + `IClickHouseJwtProvider` (OSS rejects JWT; demonstrates wire shape) |
| `GET /auth/ssh?role=admin_role` | Keyed `ssh` + `IClickHouseSshKeyProvider` reading docker SSH key |
| `GET /auth/cert?role=analyst` | Keyed `mtls` + `IClickHouseCertificateProvider` reading docker `client.pfx` |
| `GET /events/count` | `ClickHouseDataSource.OpenConnectionAsync` + scalar query |
| `GET /replica/server` | Resolving a keyed DataSource (`replica`) inline |
| `POST /events/bulk` (JSON `[{ "id": "...", "timestamp": "...", "payload": "..." }]`) | `BulkInserter<EventRow>` rented from the pool |
| `GET /diag/pool` | `ClickHouseDataSource.GetStatistics()` |
| `GET /ping/{key}` | `PingAsync()` against any keyed DataSource (`default`, `primary`, `replica`, `mtls`, `ssh`, `adhoc`) |
| `GET /health` | All health checks |
| `GET /health/ready` | Health checks tagged `ready` (i.e. `primary` + `replica`) |

## Roles (RBAC on top of auth)

`setup.sh`'s `initdb/10_auth_and_roles.sql` provisions two roles with
default role NONE:

| Role         | Grants                            |
|--------------|-----------------------------------|
| `analyst`    | `SELECT ON *.*`                   |
| `admin_role` | `CREATE, DROP, INSERT, SELECT, ALTER ON *.*` |

Without `?role=`, demo users connect with no active roles, so the grant-gated
CREATE/DROP probe at the bottom of every `/auth/*` response returns
`ACCESS_DENIED`. Pass `?role=admin_role` (or `?role=analyst,admin_role`) to
activate roles for the request:

```bash
curl localhost:5xxx/auth/password
# → { "method":"password", "user":"demo_user", "activeRoles":"(none)", "rbacProbe":"ACCESS_DENIED", "hint":"pass ?role=admin_role …" }

curl 'localhost:5xxx/auth/password?role=admin_role'
# → { "method":"password", "user":"demo_user", "activeRoles":"admin_role", "rbacProbe":"OK" }
```

The role activation goes through `ClickHouseConnection.ChangeRolesAsync`, which
issues `SET ROLE …` on the connection and pins the override. The pool's
`CanBePooled` check returns false for connections with a sticky role override,
so a request that activates roles costs one fresh physical connection — the
documented trade-off for per-request role activation against a pooled
`ClickHouseDataSource`.

## Teardown

```bash
cd docker
docker compose down
rm -rf generated
```

## How auth registrations look in code

```csharp
// Static password (default DataSource — also: replica)
builder.Services.AddClickHouse(builder.Configuration.GetSection("ClickHouse"));

// Rotating JWT
builder.Services
    .AddClickHouse("primary", builder.Configuration.GetSection("ClickHouse:Primary"))
    .WithJwtProvider<DemoJwtProvider>();

// SSH key
builder.Services
    .AddClickHouse("ssh", builder.Configuration.GetSection("ClickHouse:Ssh"))
    .WithSshKeyProvider<DemoSshKeyProvider>();

// mTLS client certificate
builder.Services
    .AddClickHouse("mtls", builder.Configuration.GetSection("ClickHouse:Mtls"))
    .WithCertificateProvider<DemoCertificateProvider>();

// Programmatic, no IConfiguration
builder.Services.AddClickHouse("adhoc", b => b
    .WithHost("localhost").WithDatabase("default").WithUsername("default"));
```

The four `IClickHouse{Jwt,Certificate,SshKey,Password}Provider` interfaces are
the integration points for rotating credentials — each is invoked once per
physical connection (gated by `Pool.ConnectionLifetime`). Real apps plug in
Azure Identity, AWS Secrets Manager, HashiCorp Vault, the Windows cert store,
etc.; the `Demo*Provider` classes here read from the docker overlay so the
sample handshakes end-to-end.
