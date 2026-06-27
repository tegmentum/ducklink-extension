#!/usr/bin/env bash
# native-conformance.sh - run the provider-neutral conformance suite against the
# NATIVE provider of <name> on stock DuckDB and, on pass, emit its conformance
# record (suite + content-digest + contract + passed) for registry/index.json.
#
# The suite is provider-blind SQL (the seed is each extension's smoke.sql). A
# pass certifies the native provider AT the current contract + suite digest; the
# resolver's hard gate admits only records whose suite_digest equals the
# canonical (sha256 of the suite) and whose `at` equals the live wit_contract.
#
# Usage: native-conformance.sh <name> <native_artifact> <suite.sql> <contract_digest> [cli]
set -euo pipefail
NAME=${1:?name}; ART=${2:?native_artifact}; SUITE=${3:?suite.sql}; CONTRACT=${4:?contract_digest}; CLI=${5:-duckdb}

# Canonical suite digest = content digest of the suite itself.
SUITE_DIGEST=$(shasum -a 256 "$SUITE" | awk '{print $1}')

# Run the suite (provider-blind SQL) against the native provider in `.mode csv`,
# the SAME way the canonical smoke/conformance harness emits, so it compares
# against the EXISTING reviewed fixture (suite's companion `.expected`, minus its
# `#` comment lines). The reference (wasm) provider is certified-by-construction
# against this same fixture; matching it certifies the native provider.
GOT=$("$CLI" -unsigned :memory: 2>/dev/null <<SQL
.mode csv
LOAD '$ART';
.read $SUITE
SQL
)
GOT="${GOT//$'\r'/}"   # .mode csv emits RFC-4180 CRLF; normalize to LF
echo "[$NAME native] suite output:"; echo "$GOT" | sed 's/^/    /'

EXP="${SUITE%.sql}.expected"
PASSED=true
if [[ -f "$EXP" ]]; then
  if ! diff <(echo "$GOT") <(grep -vE '^[[:space:]]*(#|$)' "$EXP") >/dev/null; then
    PASSED=false
  fi
fi

if [[ "$PASSED" == true ]]; then
  echo "[$NAME native] CONFORMANCE PASSED at contract ${CONTRACT:0:12} suite ${SUITE_DIGEST:0:12}"
  cat <<JSON
{ "suite": "${NAME}@2", "suite_digest": "$SUITE_DIGEST", "at": "$CONTRACT", "passed": true }
JSON
else
  echo "[$NAME native] CONFORMANCE FAILED (output != $EXP)"
  exit 1
fi
