#!/usr/bin/env bash
#
# Generate self-signed certificates for ClickHouse TLS testing
#
# This script creates:
# - CA certificate and key
# - Server certificate signed by CA
# - Client certificate for mutual TLS (optional)
# - DH parameters for strong encryption
#
# All certificates are valid for 10 years (testing only!)
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "==> Generating TLS certificates for ClickHouse testing..."
echo

# Certificate validity (days)
VALIDITY_DAYS=3650  # 10 years

# Common certificate information
COUNTRY="US"
STATE="California"
CITY="San Francisco"
ORG="ClickHouse Rust Client"
OU="Testing"

# ============================================================================
# 1. Generate CA (Certificate Authority)
# ============================================================================

echo "==> Step 1: Generating CA certificate..."

# Generate CA private key
openssl genrsa -out ca/ca-key.pem 4096

# Generate CA certificate
openssl req -new -x509 -days ${VALIDITY_DAYS} \
    -key ca/ca-key.pem \
    -out ca/ca-cert.pem \
    -subj "/C=${COUNTRY}/ST=${STATE}/L=${CITY}/O=${ORG}/OU=${OU}/CN=Test CA"

echo "✓ CA certificate generated: ca/ca-cert.pem"
echo

# ============================================================================
# 2. Generate Server Certificate
# ============================================================================

echo "==> Step 2: Generating server certificate..."

# Generate server private key
openssl genrsa -out server/server-key.pem 4096

# Generate server certificate signing request (CSR)
openssl req -new \
    -key server/server-key.pem \
    -out server/server.csr \
    -subj "/C=${COUNTRY}/ST=${STATE}/L=${CITY}/O=${ORG}/OU=${OU}/CN=localhost"

# Create config for Subject Alternative Names (SAN)
cat > server/san.cnf <<EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req

[req_distinguished_name]

[v3_req]
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = clickhouse-server-tls
DNS.3 = 127.0.0.1
IP.1 = 127.0.0.1
IP.2 = ::1
EOF

# Sign server certificate with CA
openssl x509 -req -days ${VALIDITY_DAYS} \
    -in server/server.csr \
    -CA ca/ca-cert.pem \
    -CAkey ca/ca-key.pem \
    -CAcreateserial \
    -out server/server-cert.pem \
    -extensions v3_req \
    -extfile server/san.cnf

# Clean up temporary files
rm server/server.csr server/san.cnf ca/ca-cert.srl 2>/dev/null || true

echo "✓ Server certificate generated: server/server-cert.pem"
echo

# ============================================================================
# 3. Generate DH Parameters
# ============================================================================

echo "==> Step 3: Generating Diffie-Hellman parameters (this may take a while)..."

# Generate DH parameters (2048 bits - faster for testing, 4096 for production)
openssl dhparam -out server/dhparam.pem 2048

echo "✓ DH parameters generated: server/dhparam.pem"
echo

# ============================================================================
# 4. Generate Client Certificate (for mutual TLS)
# ============================================================================

echo "==> Step 4: Generating client certificate (for mutual TLS)..."

# Generate client private key
openssl genrsa -out client/client-key.pem 4096

# Generate client certificate signing request (CSR)
openssl req -new \
    -key client/client-key.pem \
    -out client/client.csr \
    -subj "/C=${COUNTRY}/ST=${STATE}/L=${CITY}/O=${ORG}/OU=${OU}/CN=Test Client"

# Sign client certificate with CA
openssl x509 -req -days ${VALIDITY_DAYS} \
    -in client/client.csr \
    -CA ca/ca-cert.pem \
    -CAkey ca/ca-key.pem \
    -CAcreateserial \
    -out client/client-cert.pem

# Clean up temporary files
rm client/client.csr ca/ca-cert.srl 2>/dev/null || true

echo "✓ Client certificate generated: client/client-cert.pem"
echo

# ============================================================================
# 5. Set Permissions
# ============================================================================

echo "==> Step 5: Setting permissions..."

# Make keys readable only by owner
chmod 600 ca/ca-key.pem server/server-key.pem client/client-key.pem

# Make certificates readable by all
chmod 644 ca/ca-cert.pem server/server-cert.pem client/client-cert.pem server/dhparam.pem

echo "✓ Permissions set"
echo

# ============================================================================
# 6. Verify Certificates
# ============================================================================

echo "==> Step 6: Verifying certificates..."

# Verify server certificate
openssl verify -CAfile ca/ca-cert.pem server/server-cert.pem

# Verify client certificate
openssl verify -CAfile ca/ca-cert.pem client/client-cert.pem

echo

# ============================================================================
# Summary
# ============================================================================

echo "=========================================="
echo "✓ Certificate generation complete!"
echo "=========================================="
echo
echo "Generated files:"
echo "  CA:     ca/ca-cert.pem, ca/ca-key.pem"
echo "  Server: server/server-cert.pem, server/server-key.pem"
echo "  Client: client/client-cert.pem, client/client-key.pem"
echo "  DH:     server/dhparam.pem"
echo
echo "Certificate details:"
echo "  Validity: ${VALIDITY_DAYS} days (~10 years)"
echo "  Algorithm: RSA 4096-bit"
echo "  Server CN: localhost"
echo "  SANs: localhost, clickhouse-server-tls, 127.0.0.1, ::1"
echo
echo "Next steps:"
echo "  1. Start TLS-enabled ClickHouse: just start-db-tls"
echo "  2. Run TLS tests: just test-tls"
echo
