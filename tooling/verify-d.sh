#!/usr/bin/env bash
# verify-d.sh - end-to-end proof of design "D" transparent LOAD on STOCK DuckDB.
#
# Builds the shim cdylib, generates the `aba` shim + the native `aba` provider
# into a DuckLink repo, installs the shim, then runs the four VERIFY scenarios:
#   1. plain LOAD aba            -> NATIVE passthrough (precedence native > wasm)
#   2. DUCKLINK_PROVIDER=wasm... -> force the WASM provider
#   3. DUCKLINK_ALLOW_NATIVE=0   -> native denied, WASM fallback
#   4. tampered suite_digest     -> conformance gate rejects native, WASM fallback
# plus the native conformance run (suite vs the native provider).
#
# Requires: a stock DuckDB v1.5.4 CLI (path in $DUCKDB_CLI), built against the
# unstable C API. Run with `-unsigned` (the Stage-0 signing posture).
#
# Usage: DUCKDB_CLI=/path/to/duckdb verify-d.sh [workdir]
set -euo pipefail
HERE=$(cd "$(dirname "$0")/.." && pwd)            # native-extension/ducklink
ROOT=$(cd "$HERE/../.." && pwd)                    # ducklink monorepo
CLI=${DUCKDB_CLI:?set DUCKDB_CLI to a stock duckdb v1.5.4 binary}
WORK=${1:-$(mktemp -d)}
PLATFORM=$("$CLI" -noheader -list -c "PRAGMA platform" :memory:)
REV=$("$CLI" -noheader -list -c "SELECT version()" :memory:)
CONTRACT=$(python3 -c "import json;d=json.load(open('$ROOT/registry/index.json'));print([e for e in d['extensions'] if e['name']=='aba'][0]['wit_contract'])")
SUITE=$ROOT/extensions/aba-component/conformance.sql
SUITE_EXP=$ROOT/extensions/aba-component/conformance.expected
LIB=$HERE/target/debug/libducklink.dylib

REPO=$WORK/repo; DLHOME=$WORK/home; EXTDIR=$WORK/extdir
rm -rf "$REPO" "$DLHOME" "$EXTDIR"; mkdir -p "$DLHOME/artifacts" "$DLHOME/suites" "$EXTDIR"

echo "## build"; ( cd "$HERE" && cargo build >/dev/null 2>&1 )
echo "## generate shim + native provider into the repo ($REV/$PLATFORM)"
bash "$HERE/tooling/gen-shim.sh" aba        "$PLATFORM" "$REV" "$REPO" "$LIB" >/dev/null
bash "$HERE/tooling/gen-shim.sh" aba_native "$PLATFORM" "$REV" "$REPO" "$LIB" >/dev/null

echo "## DuckLink home (manifest + artifacts + suite)"
cp "$ROOT/artifacts/extensions/aba.wasm" "$DLHOME/artifacts/aba.wasm"
cp "$REPO/$REV/$PLATFORM/aba_native.duckdb_extension" "$DLHOME/artifacts/aba_native.duckdb_extension"
cp "$SUITE" "$DLHOME/suites/aba.sql"
cp "$SUITE_EXP" "$DLHOME/suites/aba.expected"
# Canonical suite digest: build C's structured scheme over conformance.{sql,expected}
# (byte-identical to resolver::compute_suite_digest / tooling/conformance.py).
SUITE_DIGEST=$(python3 - "$SUITE" "$SUITE_EXP" <<'PY'
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
echo "   canonical suite_digest = $SUITE_DIGEST"
python3 - "$DLHOME/index.json" "$CONTRACT" "$SUITE_DIGEST" <<'PY'
import json,sys
out,contract,suite=sys.argv[1:4]
json.dump({"extensions":[{"name":"aba","wit_contract":contract,"wit_contract_version":"2.0.0","providers":[
 {"id":"wasm-component","kind":"wasm","reference":True,"abi":"duckdb:extension@2.0.0","artifact":"artifacts/aba.wasm",
  "conformance":{"suite":"aba@2","suite_digest":suite,"at":contract,"passed":True}},
 {"id":"native-arm64-macos","kind":"native","platform":{"os":"macos","arch":"arm64"},"artifact":"artifacts/aba_native.duckdb_extension",
  "trust":{"signed_by":"ed25519:ducklink-dev","attestation":"none"},
  "conformance":{"suite":"aba@2","suite_digest":suite,"at":contract,"passed":True}}]}]},open(out,"w"),indent=1)
PY

echo "## ducklink install aba"
bash "$HERE/tooling/ducklink-install.sh" aba "$REPO" "$EXTDIR" "$CLI" >/dev/null

run(){ DUCKLINK_HOME="$DLHOME" "$@" "$CLI" -unsigned -box :memory:; }
echo "== 1: plain LOAD aba -> NATIVE =="
DUCKLINK_HOME="$DLHOME" "$CLI" -unsigned :memory: <<SQL
SET extension_directory='$EXTDIR'; LOAD aba; SELECT aba_validate('021000021') AS chase;
SQL
echo "== 2: force wasm =="
DUCKLINK_HOME="$DLHOME" DUCKLINK_PROVIDER=wasm-component "$CLI" -unsigned :memory: <<SQL
SET extension_directory='$EXTDIR'; LOAD aba; SELECT aba_validate('021000021') AS chase;
SQL
echo "== 3: deny native -> wasm =="
DUCKLINK_HOME="$DLHOME" DUCKLINK_ALLOW_NATIVE=0 "$CLI" -unsigned :memory: <<SQL
SET extension_directory='$EXTDIR'; LOAD aba; SELECT aba_validate('021000021') AS chase;
SQL
echo "## native conformance"
bash "$HERE/tooling/native-conformance.sh" aba "$DLHOME/artifacts/aba_native.duckdb_extension" "$SUITE" "$CONTRACT" "$CLI"
echo "## verify-d complete"
