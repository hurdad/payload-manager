#!/usr/bin/env bash
#
# Generate a development CA, a server certificate, an HS256 signing key and a
# signed token, for running payload-manager with TLS and authentication locally
# or in CI.
#
# THESE ARE DEVELOPMENT CREDENTIALS. The CA key sits unencrypted next to the
# certificate it signed and the token never expires in any meaningful sense.
# Everything lands in certs/, which .gitignore excludes. Do not deploy these.
#
# Idempotent: re-running reuses an existing CA (so clients that already trust it
# keep working) and only regenerates what is missing or expired. Pass --force to
# start over.
#
# Usage:
#   scripts/gen-dev-certs.sh [--force] [--dir certs] [--days 365]

set -euo pipefail

OUT_DIR="certs"
DAYS=365
FORCE=0

while [ $# -gt 0 ]; do
    case "$1" in
        --force) FORCE=1; shift ;;
        --dir)   OUT_DIR="$2"; shift 2 ;;
        --days)  DAYS="$2"; shift 2 ;;
        -h|--help) sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

command -v openssl >/dev/null || { echo "error: openssl not found" >&2; exit 1; }

if [ "$FORCE" -eq 1 ]; then
    rm -rf "$OUT_DIR"
fi
mkdir -p "$OUT_DIR"
# The CA key and the HS256 secret are private keys; keep the directory tight
# before anything is written into it.
chmod 700 "$OUT_DIR"

CA_KEY="$OUT_DIR/ca-key.pem"
CA_CRT="$OUT_DIR/ca.pem"
SRV_KEY="$OUT_DIR/server-key.pem"
SRV_CRT="$OUT_DIR/server.pem"
JWT_KEY="$OUT_DIR/jwt-hs256.key"
TOKEN="$OUT_DIR/dev-token.txt"

# ---------------------------------------------------------------------------
# CA
# ---------------------------------------------------------------------------

if [ -f "$CA_KEY" ] && [ -f "$CA_CRT" ]; then
    echo "==> reusing existing CA ($CA_CRT)"
else
    echo "==> generating CA"
    openssl req -x509 -newkey rsa:4096 -sha256 -nodes \
        -days $((DAYS * 2)) \
        -keyout "$CA_KEY" -out "$CA_CRT" \
        -subj "/CN=payload-manager development CA/O=payload-manager" \
        -addext "basicConstraints=critical,CA:TRUE,pathlen:0" \
        -addext "keyUsage=critical,keyCertSign,cRLSign" \
        2>/dev/null
fi

# ---------------------------------------------------------------------------
# Server certificate
#
# The SANs are the names the server is actually reached by: "localhost" for a
# bare-metal run, "payload-manager" because that is the compose service name
# every client container dials (docker-compose.postgres.yml and friends), and
# "payload-gateway" so the gateway's own certificate can be issued from here
# too. gRPC verifies the SAN, never the CN, so a certificate missing the name a
# client dials fails with an opaque handshake error.
# ---------------------------------------------------------------------------

regen_server=1
if [ -f "$SRV_CRT" ] && [ -f "$SRV_KEY" ]; then
    if openssl x509 -in "$SRV_CRT" -noout -checkend 86400 >/dev/null 2>&1; then
        regen_server=0
        echo "==> reusing existing server certificate ($SRV_CRT)"
    else
        echo "==> server certificate expires within 24h, regenerating"
    fi
fi

if [ "$regen_server" -eq 1 ]; then
    echo "==> generating server certificate"
    openssl req -newkey rsa:2048 -nodes \
        -keyout "$SRV_KEY" -out "$OUT_DIR/server.csr" \
        -subj "/CN=payload-manager/O=payload-manager" \
        2>/dev/null

    cat > "$OUT_DIR/server-ext.cnf" <<EXT
basicConstraints = CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = DNS:localhost,DNS:payload-manager,DNS:payload-gateway,IP:127.0.0.1,IP:::1
EXT

    openssl x509 -req -in "$OUT_DIR/server.csr" \
        -CA "$CA_CRT" -CAkey "$CA_KEY" -CAcreateserial \
        -out "$SRV_CRT" -days "$DAYS" -sha256 \
        -extfile "$OUT_DIR/server-ext.cnf" \
        2>/dev/null

    rm -f "$OUT_DIR/server.csr" "$OUT_DIR/server-ext.cnf"
fi

# ---------------------------------------------------------------------------
# HS256 signing key and a token signed with it
#
# Written without any shell helper so this script has no dependency beyond
# openssl and coreutils: base64url is plain base64 with the alphabet swapped and
# the padding stripped, and HS256 is one openssl dgst invocation.
# ---------------------------------------------------------------------------

b64url() { openssl base64 -A | tr '+/' '-_' | tr -d '='; }

if [ -f "$JWT_KEY" ]; then
    echo "==> reusing existing HS256 key ($JWT_KEY)"
else
    echo "==> generating HS256 signing key"
    openssl rand -base64 48 | tr -d '\n' > "$JWT_KEY"
fi
chmod 600 "$JWT_KEY"

echo "==> minting a development token"
# exp is DAYS out. iat is now. "sub" names the bearer so log lines and any
# future per-caller work have something to attribute to.
now=$(date +%s)
exp=$((now + DAYS * 86400))
header='{"alg":"HS256","typ":"JWT"}'
payload="{\"sub\":\"dev\",\"iss\":\"payload-manager-dev\",\"aud\":\"payload-manager\",\"iat\":${now},\"exp\":${exp}}"

h=$(printf '%s' "$header"  | b64url)
p=$(printf '%s' "$payload" | b64url)
sig=$(printf '%s' "$h.$p" | openssl dgst -sha256 -mac HMAC -macopt "key:$(cat "$JWT_KEY")" -binary | b64url)
printf '%s.%s.%s' "$h" "$p" "$sig" > "$TOKEN"

chmod 600 "$CA_KEY" "$SRV_KEY"
chmod 644 "$CA_CRT" "$SRV_CRT"
chmod 600 "$TOKEN"

cat <<SUMMARY

Development credentials written to $OUT_DIR/

  ca.pem           CA certificate  — give this to clients (PAYLOAD_MANAGER_TLS_CA)
  ca-key.pem       CA private key  — signs the above; never leaves this machine
  server.pem       server certificate (SANs: localhost, payload-manager, payload-gateway, 127.0.0.1, ::1)
  server-key.pem   server private key
  jwt-hs256.key    HS256 signing key — server validates tokens with this
  dev-token.txt    a token signed with it, valid $DAYS days

Point the service at them:

  server:
    bind_address: "0.0.0.0:50051"
    tls:
      cert_file: $OUT_DIR/server.pem
      key_file:  $OUT_DIR/server-key.pem
    auth:
      jwt_hs256_key_file: $OUT_DIR/jwt-hs256.key

And a client:

  export PAYLOAD_MANAGER_TLS_CA=$OUT_DIR/ca.pem
  export PAYLOAD_MANAGER_TOKEN=\$(cat $OUT_DIR/dev-token.txt)

SUMMARY
