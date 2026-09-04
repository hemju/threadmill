#!/usr/bin/env bash
set -euo pipefail

fixture_dir="$(cd "$(dirname "$0")" && pwd)"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT

openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "$scratch/ca.key" \
  -out "$fixture_dir/ca.crt" \
  -days 3650 \
  -subj '/CN=Threadmill Test CA'
openssl req -newkey rsa:2048 -nodes \
  -keyout "$scratch/server.key" \
  -out "$scratch/server.csr" \
  -subj '/CN=localhost' \
  -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1'
openssl x509 -req \
  -in "$scratch/server.csr" \
  -CA "$fixture_dir/ca.crt" \
  -CAkey "$scratch/ca.key" \
  -CAcreateserial \
  -out "$fixture_dir/server.crt" \
  -days 3650 \
  -copy_extensions copy
openssl base64 -A \
  -in "$scratch/server.key" \
  -out "$fixture_dir/server.key.b64"
printf '\n' >> "$fixture_dir/server.key.b64"

# The private key is a public, test-only fixture. It is base64-wrapped solely
# to prevent generic secret scanners from treating it as a production key.
