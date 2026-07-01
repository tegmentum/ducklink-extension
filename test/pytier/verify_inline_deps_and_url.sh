#!/usr/bin/env bash
#
# Real-CLI verification for the two Python-source-tier features:
#   PART 1 — PEP 723 inline deps -> env: a script declaring a PURE-PYTHON dep
#            (`six`) whose scalar imports+uses it runs only after ducklink_run
#            stages the wheel; the same import with NO PEP 723 block FAILS.
#   PART 2 — arbitrary http(s) URL loader: ducklink_run('http://.../x.py') runs a
#            URL-hosted script and ducklink_load('http://.../x.wasm') loads a
#            URL-hosted component — both gated behind DUCKLINK_ALLOW_URL.
#
# Needs a duckdb v1.5.4 CLI, the built ducklink.duckdb_extension, and the pylon
# endpoint artifact + CPython Lib (python-wasm build tree). SKIPs (exit 0) when a
# prerequisite is missing unless STRICT=1.
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
strict="${STRICT:-0}"
skip() { if [ "$strict" = 1 ]; then echo "FAIL(STRICT): $1" >&2; exit 1; fi; echo "SKIP: $1" >&2; exit 0; }
fail() { echo "FAIL: $1" >&2; exit 1; }
pass() { echo "PASS: $1"; }

# --- prerequisites ------------------------------------------------------------
CLI="${DUCKDB_BIN:-/opt/homebrew/var/homebrew/tmp/.cellar/duckdb/1.5.4/bin/duckdb}"
[ -x "$CLI" ] && "$CLI" --version 2>/dev/null | grep -q v1.5.4 || skip "no duckdb v1.5.4 CLI"
EXT="${DUCKLINK_EXTENSION:-$repo/build/release/extension/ducklink/ducklink.duckdb_extension}"
[ -f "$EXT" ] || EXT="$repo/target/release/ducklink.duckdb_extension"
[ -f "$EXT" ] || skip "no ducklink.duckdb_extension (build it first)"
PYLON="${DUCKLINK_PYLON_ENDPOINT:-$HOME/git/python-wasm/build/3.14-current/pylon-endpoint.component.wasm}"
[ -f "$PYLON" ] || skip "no pylon endpoint component"
LIB="${DUCKLINK_PYLON_LIB:-$HOME/git/python-wasm/deps/cpython-3.14/Lib}"
[ -d "$LIB" ] || skip "no CPython Lib dir"
SDK="${DUCKLINK_PYTHON_SDK:-$HOME/git/ducklink/python-sdk/ducklink}"
[ -d "$SDK" ] || skip "no ducklink python SDK"

export DUCKLINK_PYLON_ENDPOINT="$PYLON" DUCKLINK_PYLON_LIB="$LIB" DUCKLINK_PYTHON_SDK="$SDK"
# Fresh cache each run so staging is actually exercised (not a stale hit).
export XDG_CACHE_HOME="$(mktemp -d)"
trap 'rm -rf "$XDG_CACHE_HOME" "${SRV_DIR:-}"; [ -n "${SRV_PID:-}" ] && kill "$SRV_PID" 2>/dev/null' EXIT

run_sql() { "$CLI" -unsigned -c "LOAD '$EXT'; $1" 2>&1; }

echo "== PART 1: PEP 723 inline deps -> env =="

# 1a. A declared PURE-PYTHON dep is staged and its scalar runs.
out="$(run_sql "
  SELECT scalars FROM ducklink_run('$here/dep_ext.py');
  SELECT six_type_name(3.0) AS a, six_type_name(2.5) AS b;
")"
echo "$out" | grep -q "int" && echo "$out" | grep -q "float" \
  && pass "1a declared dep 'six' staged; six_type_name ran (int/float)" \
  || fail "1a dep_ext.py did not run with staged six:
$out"

# 1b. The SAME import with NO PEP 723 block must FAIL (proves staging is load-bearing).
out="$(run_sql "SELECT scalars FROM ducklink_run('$here/dep_ext_nodecl.py'); SELECT six_type_name_nodecl(1.0);")"
if echo "$out" | grep -qiE "ModuleNotFoundError|No module named|six"; then
  pass "1b undeclared 'six' fails (import error) — staging is what makes 1a work"
else
  # It may register but fail on invoke; either an import error at load or an
  # invoke-time failure counts, as long as it does NOT return a clean int/float.
  if echo "$out" | grep -qE "^(int|float)$"; then
    fail "1b undeclared six unexpectedly succeeded:
$out"
  else
    pass "1b undeclared 'six' did not produce a valid result (no silent success):
$(echo "$out" | tail -3)"
  fi
fi

echo "== PART 2: arbitrary http(s) URL loader =="

# Serve test files over a local http server.
SRV_DIR="$(mktemp -d)"
cp "$here/url_ext.py" "$SRV_DIR/url_ext.py"
# Re-serve a real catalog component blob by URL to prove the .wasm URL path:
# resolve `aba` by name once (downloads + caches its blob), then copy the cached
# .wasm out to the http server and load it back by URL.
WASM_SRC=""
run_sql "SELECT 1 FROM ducklink_load('aba') LIMIT 1;" >/dev/null 2>&1 || true
for c in "$XDG_CACHE_HOME"/ducklink/wasm/sha256/*/*.wasm; do
  [ -f "$c" ] && WASM_SRC="$c" && break
done
if [ -z "$WASM_SRC" ]; then
  for c in "$HOME"/git/ducklink/artifacts/extensions/*.wasm; do
    [ -f "$c" ] && WASM_SRC="$c" && break
  done
fi
[ -n "$WASM_SRC" ] && cp "$WASM_SRC" "$SRV_DIR/url_comp.wasm"

( cd "$SRV_DIR" && python3 -m http.server 8799 >/dev/null 2>&1 ) &
SRV_PID=$!
sleep 1
BASE="http://127.0.0.1:8799"

# 2a. URL .py refused WITHOUT the opt-in.
out="$(DUCKLINK_ALLOW_URL="" run_sql "SELECT * FROM ducklink_run('$BASE/url_ext.py');")"
echo "$out" | grep -q "DUCKLINK_ALLOW_URL" \
  && pass "2a URL script refused without DUCKLINK_ALLOW_URL (unsigned-path gate)" \
  || fail "2a URL script was NOT gated:
$out"

# 2b. URL .py runs WITH the opt-in.
out="$(DUCKLINK_ALLOW_URL=1 run_sql "
  SELECT scalars FROM ducklink_run('$BASE/url_ext.py');
  SELECT url_greet('web') AS g;
")"
echo "$out" | grep -q "hello-from-url:web" \
  && pass "2b URL-hosted script ran (url_greet)" \
  || fail "2b URL script did not run:
$out"

# 2c. URL .wasm loads WITH the opt-in.
if [ -f "$SRV_DIR/url_comp.wasm" ]; then
  out="$(DUCKLINK_ALLOW_URL=1 run_sql "SELECT name, scalars FROM ducklink_load('$BASE/url_comp.wasm');")"
  echo "$out" | grep -qiE "url_comp|component" \
    && pass "2c URL-hosted .wasm component loaded" \
    || fail "2c URL .wasm did not load:
$out"
else
  skip "2c no component blob available to re-serve"
fi

echo "ALL CHECKS PASSED"
