#!/bin/bash
# Generate self-signed TLS certificates for development
# For production, use certificates from a trusted CA (Let's Encrypt, etc.)

set -e

# Configuration
CERT_DIR="${1:-./certs}"
DAYS_VALID="${2:-365}"
COUNTRY="SG"
STATE="Singapore"
CITY="Singapore"
ORG="Sausheong Software Services"
COMMON_NAME="${3:-localhost}"

# Create certs directory if it doesn't exist
mkdir -p "$CERT_DIR"

echo "Generating self-signed TLS certificate..."
echo "Certificate directory: $CERT_DIR"
echo "Valid for: $DAYS_VALID days"
echo "Common Name: $COMMON_NAME"
echo ""

# Generate private key (RSA 4096-bit for strong security)
openssl genrsa -out "$CERT_DIR/server.key" 4096

# Generate certificate signing request (CSR)
openssl req -new -key "$CERT_DIR/server.key" -out "$CERT_DIR/server.csr" \
    -subj "/C=$COUNTRY/ST=$STATE/L=$CITY/O=$ORG/CN=$COMMON_NAME"

# Generate self-signed certificate
openssl x509 -req -days "$DAYS_VALID" \
    -in "$CERT_DIR/server.csr" \
    -signkey "$CERT_DIR/server.key" \
    -out "$CERT_DIR/server.crt" \
    -extfile <(printf "subjectAltName=DNS:localhost,DNS:*.localhost,IP:127.0.0.1,IP:::1")

# Set appropriate permissions
chmod 600 "$CERT_DIR/server.key"
chmod 644 "$CERT_DIR/server.crt"

# Clean up CSR
rm "$CERT_DIR/server.csr"

echo ""
echo "✅ Certificate generated successfully!"
echo ""
echo "Files created:"
echo "  - Private key: $CERT_DIR/server.key"
echo "  - Certificate: $CERT_DIR/server.crt"
echo ""
echo "To use with mindb-server, set these environment variables:"
echo "  export ENABLE_TLS=true"
echo "  export TLS_CERT_FILE=$CERT_DIR/server.crt"
echo "  export TLS_KEY_FILE=$CERT_DIR/server.key"
echo ""
echo "⚠️  WARNING: This is a self-signed certificate for development only!"
echo "   For production, use certificates from a trusted CA."
echo ""
echo "To view certificate details:"
echo "  openssl x509 -in $CERT_DIR/server.crt -text -noout"
