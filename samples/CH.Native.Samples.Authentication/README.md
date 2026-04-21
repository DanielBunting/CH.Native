# CH.Native.Samples.Authentication

Demonstrates every auth method CH.Native supports, end-to-end against a local
ClickHouse 24.3 container:

| Method                   | Server-side user config (`users.d/auth_users.xml`)       |
|--------------------------|----------------------------------------------------------|
| Password                 | built-in `default` user (empty password)                 |
| JWT                      | *(OSS rejects `<jwt>` at parse time; Cloud-only)*        |
| SSH key                  | `<ssh_user>` with `<ssh_keys><base64_key>…`              |
| TLS client certificate   | `<cert_user>` with `<ssl_certificates><common_name>…`    |

## One-time setup

```bash
cd docker
./setup.sh              # generates ./docker/generated/{certs,keys,users.d,config.d}
docker compose up -d    # boots clickhouse-server:24.3 with those overlays mounted
cd ..
```

The setup script generates a self-signed CA, a server cert for `localhost`, a
client cert with `CN=cert_user`, and an RSA SSH keypair for `ssh_user`. Nothing
is committed — `./docker/generated/` is gitignored.

## Run each auth method

```bash
# Password (demo_user is provisioned by the setup script)
dotnet run -- password demo_user demo

# SSH key (server has the pubkey configured for ssh_user)
dotnet run -- ssh ssh_user docker/generated/keys/ssh_user

# TLS client certificate (server matches CN=cert_user against ssl_certificates)
dotnet run -- cert cert_user docker/generated/certs/client.pfx testpass --insecure

# JWT: expected to fail on OSS; ClickHouse treats the marker as a user name
# lookup and surfaces "Authentication failed: password is incorrect, or there
# is no user with such name". The round-trip proves the client wire format.
dotnet run -- jwt eyJhbGciOiJIUzI1NiJ9.e30.fake-signature
```

## Roles (RBAC on top of auth)

The setup script's `initdb/10_roles.sql` provisions two roles, granted to every
demo user, with **default role = NONE**:

| Role         | Grants                    |
|--------------|---------------------------|
| `analyst`    | `SELECT ON *.*`           |
| `admin_role` | `ALL ON *.* WITH GRANT OPTION` |

Because the default role is NONE, a bare connection has zero active privileges.
Pass `--role NAME` (or a comma list) to activate roles for the session:

```bash
# No role active — the grant-gated probe (CREATE/DROP TABLE) fails
dotnet run -- password demo_user demo
# → [password] connected as 'demo_user' roles=[(none)] to ClickHouse ...
# → [password]   CREATE/DROP probe = ACCESS_DENIED (activate admin_role via --role admin_role ...)

# Activate analyst — analyst has SELECT only, still can't CREATE
dotnet run -- password demo_user demo --role analyst
# → [password] connected as 'demo_user' roles=[analyst] to ClickHouse ...
# → [password]   CREATE/DROP probe = ACCESS_DENIED ...

# Activate admin_role — CREATE + DROP succeed
dotnet run -- password demo_user demo --role admin_role
# → [password] connected as 'demo_user' roles=[admin_role] to ClickHouse ...
# → [password]   CREATE/DROP probe = OK (privileged)

# Multiple roles
dotnet run -- ssh ssh_user docker/generated/keys/ssh_user --role analyst,admin_role

# Works for every auth method
dotnet run -- cert cert_user docker/generated/certs/client.pfx testpass --insecure --role admin_role
```

Each run also emits a `db.clickhouse.roles` tag on the query `Activity` (sorted
comma-joined), so any OpenTelemetry / Application Insights / Jaeger exporter
wired to the `CH.Native` ActivitySource sees the effective role set.

Under the hood, `WithRoles(...)` on `ClickHouseConnectionSettings` causes the
first query on the connection to be preceded by a `SET ROLE \`analyst\`, ...`
session command. The role state is cached per connection — subsequent queries
with the same set are no-ops. Per-command overrides are available via
`ClickHouseDbCommand.Roles` and `BulkInsertOptions.Roles`.

Each command opens a connection, runs `SELECT currentUser()` and `SELECT
version()`, and prints the principal the server resolved.

## Teardown

```bash
cd docker
docker compose down
rm -rf generated
```

## How each auth method looks in code

See [`Program.cs`](Program.cs). The pattern is the same for all four: build a
`ClickHouseConnectionSettings` via the fluent builder, then open a
`ClickHouseConnection`. The four methods differ only in which builder calls are
used:

```csharp
// Password
.WithUsername("default").WithPassword("")

// JWT
.WithJwt(token)

// SSH key
.WithUsername("ssh_user").WithSshKeyPath("/path/to/privkey", passphrase: null)

// TLS client certificate
.WithTls().WithTlsPort(9440).WithAllowInsecureTls()
.WithUsername("cert_user")
.WithTlsClientCertificate(new X509Certificate2("client.pfx", "testpass"))
.WithAuthMethod(ClickHouseAuthMethod.TlsClientCertificate)
```

The builder enforces mutual exclusion — calling `WithPassword` + `WithJwt`
throws at `Build()` time. The `WithAuthMethod(TlsClientCertificate)` call is
only required when you want the server to run its cert-auth path; a cert
attached for transport reasons alone doesn't need it.
