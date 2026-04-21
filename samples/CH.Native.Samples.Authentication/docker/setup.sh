#!/usr/bin/env bash
# Generates TLS certs, an SSH RSA keypair, and users.d / config.d overlays
# for the docker-compose ClickHouse server. Idempotent — safe to re-run.
set -euo pipefail

cd "$(dirname "$0")"
rm -rf generated
mkdir -p generated/certs generated/keys generated/users.d generated/config.d
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
    <!-- Password demo: image's entrypoint locks 'default' to localhost-only
         when CLICKHOUSE_USER isn't set, so we provision our own user. -->
    <demo_user>
      <password>demo</password>
      <networks><ip>::/0</ip></networks>
      <profile>default</profile>
      <quota>default</quota>
    </demo_user>
    <ssh_user>
      <ssh_keys>
        <ssh_key>
          <type>ssh-rsa</type>
          <base64_key>${SSH_PUBKEY}</base64_key>
        </ssh_key>
      </ssh_keys>
      <networks><ip>::/0</ip></networks>
      <profile>default</profile>
      <quota>default</quota>
    </ssh_user>
    <cert_user>
      <ssl_certificates>
        <common_name>cert_user</common_name>
      </ssl_certificates>
      <networks><ip>::/0</ip></networks>
      <profile>default</profile>
      <quota>default</quota>
    </cert_user>
  </users>
</clickhouse>
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
