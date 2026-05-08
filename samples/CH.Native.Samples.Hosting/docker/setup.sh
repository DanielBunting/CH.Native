#!/usr/bin/env bash
# Generates TLS certs, an SSH RSA keypair, and users.d / config.d overlays
# for the docker-compose ClickHouse server. Idempotent — safe to re-run.
set -euo pipefail

cd "$(dirname "$0")"
rm -rf generated
mkdir -p generated/certs generated/keys generated/users.d generated/config.d generated/initdb
cd generated

echo "==> Generating self-signed CA"
openssl req -x509 -newkey rsa:2048 -sha256 -days 3650 -nodes \
    -subj "/CN=ch-auth-sample-ca" \
    -keyout certs/ca.key -out certs/ca.crt 2>/dev/null

echo "==> Generating server cert (CN=localhost)"
openssl req -newkey rsa:2048 -sha256 -nodes \
    -subj "/CN=localhost" \
    -keyout certs/server.key -out certs/server.csr 2>/dev/null
openssl x509 -req -in certs/server.csr \
    -CA certs/ca.crt -CAkey certs/ca.key -CAcreateserial \
    -days 3650 -out certs/server.crt \
    -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1") 2>/dev/null
rm certs/server.csr

echo "==> Generating client cert (CN=cert_user)"
openssl req -newkey rsa:2048 -sha256 -nodes \
    -subj "/CN=cert_user" \
    -keyout certs/client.key -out certs/client.csr 2>/dev/null
openssl x509 -req -in certs/client.csr \
    -CA certs/ca.crt -CAkey certs/ca.key -CAcreateserial \
    -days 3650 -out certs/client.crt 2>/dev/null
openssl pkcs12 -export \
    -out certs/client.pfx \
    -inkey certs/client.key -in certs/client.crt \
    -passout pass:testpass 2>/dev/null
rm certs/client.csr certs/ca.srl

echo "==> Generating SSH RSA keypair for ssh_user"
ssh-keygen -t rsa -b 2048 -f keys/ssh_user -N "" -C "ssh_user" -q

SSH_PUBKEY=$(awk '{print $2}' keys/ssh_user.pub)

echo "==> Writing users.d/auth_users.xml"
cat > users.d/auth_users.xml <<EOF
<clickhouse>
  <users>
    <!-- 'default' gets access_management so the initdb SQL can
         CREATE USER / CREATE ROLE / GRANT. All three auth-demo users are
         provisioned via SQL DDL below (initdb/10_auth_and_roles.sql) so they
         live in local_directory storage and can be re-granted roles. -->
    <default>
      <access_management>1</access_management>
      <named_collection_control>1</named_collection_control>
    </default>
  </users>
</clickhouse>
EOF

echo "==> Writing initdb/10_auth_and_roles.sql"
cat > initdb/10_auth_and_roles.sql <<EOF
-- Runs once on first boot, invoked by the image entrypoint as 'default' (which
-- has access_management=1 via users.d/auth_users.xml).
--
-- All three demo users are created here rather than in users.d so they live in
-- the mutable local_directory access storage and can receive role grants.
-- The SSH public key is pinned at setup-time.

CREATE USER IF NOT EXISTS demo_user IDENTIFIED WITH plaintext_password BY 'demo';
CREATE USER IF NOT EXISTS ssh_user  IDENTIFIED WITH ssh_key BY KEY '${SSH_PUBKEY}' TYPE 'ssh-rsa';
CREATE USER IF NOT EXISTS cert_user IDENTIFIED WITH ssl_certificate CN 'cert_user';

CREATE ROLE IF NOT EXISTS analyst;
CREATE ROLE IF NOT EXISTS admin_role;

GRANT SELECT ON *.* TO analyst;
-- Can't GRANT ALL WITH GRANT OPTION from a user without it; keep admin_role
-- powerful-but-not-re-grantable for the sample.
GRANT CREATE, DROP, INSERT, SELECT, ALTER ON *.* TO admin_role;

GRANT analyst, admin_role TO demo_user, ssh_user, cert_user;

SET DEFAULT ROLE NONE TO demo_user, ssh_user, cert_user;
EOF

echo "==> Writing config.d/tls.xml"
cat > config.d/tls.xml <<'EOF'
<clickhouse>
  <!-- Bind to all interfaces so the host-forwarded ports reach us.
       Default config listens only on 127.0.0.1 / ::1 inside the container.
       We use 0.0.0.0 (IPv4 any) rather than :: because Docker bridges often
       lack IPv6 and ClickHouse fails to bind the interserver port otherwise. -->
  <listen_host>0.0.0.0</listen_host>
  <tcp_port_secure>9440</tcp_port_secure>
  <openSSL>
    <server>
      <certificateFile>/etc/clickhouse-server/certs/server.crt</certificateFile>
      <privateKeyFile>/etc/clickhouse-server/certs/server.key</privateKeyFile>
      <caConfig>/etc/clickhouse-server/certs/ca.crt</caConfig>
      <verificationMode>relaxed</verificationMode>
      <loadDefaultCAFile>false</loadDefaultCAFile>
    </server>
  </openSSL>
</clickhouse>
EOF

# Permissions ClickHouse expects on mounted private keys.
chmod 600 certs/server.key certs/client.key certs/ca.key keys/ssh_user

cat <<EOF

Setup complete. Artifacts written to: $(pwd)

Next:
  docker compose up -d
  # (from the sample root, i.e. one directory up)
  dotnet run -- password demo_user demo
  dotnet run -- ssh      ssh_user  docker/generated/keys/ssh_user
  dotnet run -- cert     cert_user docker/generated/certs/client.pfx testpass --insecure
  dotnet run -- jwt      eyJhbGciOiJIUzI1NiJ9.e30.fake    # expected failure (Cloud-only)
EOF
