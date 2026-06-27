#!/usr/bin/env bash
# native-conformance.sh - run the provider-neutral conformance suite against the
# NATIVE provider of <name> on stock DuckDB and, on pass, emit its conformance
# record (suite + content-digest + contract + passed) for registry/index.json.
#
# The suite is provider-blind SQL (extensions/<name>-component/conformance.sql).
# A pass certifies the native provider AT the current contract + suite digest;
# the resolver's hard gate admits only records whose suite_digest equals the
# canonical and whose `at` equals the live wit_contract.
#
# Usage: native-conformance.sh <name> <native_artifact> <conformance.sql> <contract_digest> [cli]
set -euo pipefail
NAME=${1:?name}; ART=${2:?native_artifact}; SUITE=${3:?conformance.sql}; CONTRACT=${4:?contract_digest}; CLI=${5:-duckdb}
EXP="${SUITE%.sql}.expected"

# Canonical suite digest -- build C's STRUCTURED scheme over conformance.{sql,expected}
# (byte-identical to resolver::compute_suite_digest / tooling/conformance.py),
# NOT a plain sha256, so the emitted record's suite_digest matches the canonical
# the resolver recomputes.
SUITE_DIGEST=$(python3 - "$SUITE" "$EXP" <<'PY'
import hashlib,sys
sql=open(sys.argv[1]).read(); exp=open(sys.argv[2]).read()
nsql="\n".join(l.rstrip() for l in sql.splitlines() if l.strip() and not l.lstrip().startswith("--"))
ne=[]
for l in exp.splitlines():
    s=l.rstrip()
    if not s.strip(): continue
    ls=s.lstrip()
    if ls=="#" or ls.startswith("# "): continue
    ne.append(s)
canon=b"duckdb:conformance-suite:1\n"+nsql.encode()+b"\n\x1e\n"+"\n".join(ne).encode()
print(hashlib.sha256(canon).hexdigest())
PY
)

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
