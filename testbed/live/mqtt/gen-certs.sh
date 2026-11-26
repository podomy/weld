#!/usr/bin/env bash
# Copyright (C) 2026 Forge Universal LLC.
# SPDX-License-Identifier: BUSL-1.1
#
# Generate test-only TLS material for the mosquitto sidecar.
#
# Output lands in certs/ next to this script: a throwaway CA plus a
# server certificate valid for localhost and 127.0.0.1. These files
# are committed so the TLS gate runs without a setup step; they must
# never leave localhost testing. Regenerate with this script only.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CERTS="${SCRIPT_DIR}/certs"
mkdir -p "${CERTS}"
umask 077

openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "${CERTS}/ca-key.pem" -out "${CERTS}/ca.pem" \
  -days 3650 -subj "/CN=weld-test-ca" 2>/dev/null

openssl req -newkey rsa:2048 -nodes \
  -keyout "${CERTS}/server-key.pem" -out "${CERTS}/server.csr" \
  -subj "/CN=localhost" 2>/dev/null

cat > "${CERTS}/server.ext" <<'EOF'
subjectAltName = DNS:localhost, IP:127.0.0.1
EOF

openssl x509 -req -in "${CERTS}/server.csr" \
  -CA "${CERTS}/ca.pem" -CAkey "${CERTS}/ca-key.pem" -CAcreateserial \
  -out "${CERTS}/server.pem" -days 3650 \
  -extfile "${CERTS}/server.ext" 2>/dev/null

rm -f "${CERTS}/server.csr" "${CERTS}/server.ext" "${CERTS}/ca.srl"
chmod 600 "${CERTS}/server-key.pem" "${CERTS}/ca-key.pem"
chmod 644 "${CERTS}/server.pem" "${CERTS}/ca.pem"

echo "wrote ${CERTS}/ca.pem ${CERTS}/server.pem"
