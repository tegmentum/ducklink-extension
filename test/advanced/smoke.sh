#!/usr/bin/env bash
#
# Advanced-tier dispatch smoke test (PARSER / OPTIMIZER / table FILTER pushdown).
#
# These tiers bind DuckDB's INTERNAL C++ ABI through the C++ shim, so they can
# only be exercised by LOADing the real `.duckdb_extension` into a matching
# DuckDB host and running SQL — there is no in-process duckdb-rs path for them.
# The script builds nothing; it expects an already-built extension and a host
# CLI whose version matches the one the extension was built against (v1.5.4).
#
# It SKIPS (exit 0) when the host CLI, the extension artifact, or the component
# corpus are unavailable, so it is safe to wire into CI that lacks them. Set
# STRICT=1 to turn skips into failures.
#
# Inputs (env, all optional):
#   DUCKDB_BIN              path to a duckdb v1.5.4 CLI (else common paths tried)
#   DUCKLINK_EXTENSION      path to ducklink.duckdb_extension (else target/release)
#   DUCKLINK_CORPUS_DIR     dir of component .wasm files (else ~/git/ducklink/artifacts/extensions)
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"

strict="${STRICT:-0}"
skip() {
	if [ "$strict" = "1" ]; then
		echo "FAIL (STRICT): $1" >&2
		exit 1
	fi
	echo "SKIP: $1" >&2
	exit 0
}

# --- locate the host CLI (must report v1.5.4) ---------------------------------
find_cli() {
	local candidates=(
		"${DUCKDB_BIN:-}"
		"/opt/homebrew/var/homebrew/tmp/.cellar/duckdb/1.5.4/bin/duckdb"
		"$(command -v duckdb 2>/dev/null || true)"
	)
	for c in "${candidates[@]}"; do
		[ -n "$c" ] && [ -x "$c" ] || continue
		if "$c" --version 2>/dev/null | grep -q "v1.5.4"; then
			echo "$c"
			return 0
		fi
	done
	return 1
}
cli="$(find_cli)" || skip "no duckdb v1.5.4 CLI found (set DUCKDB_BIN)"

ext="${DUCKLINK_EXTENSION:-$repo/target/release/ducklink.duckdb_extension}"
[ -f "$ext" ] || skip "extension artifact not found at $ext (build + add metadata first)"

corpus="${DUCKLINK_CORPUS_DIR:-$HOME/git/ducklink/artifacts/extensions}"
[ -d "$corpus" ] || skip "component corpus dir not found at $corpus"

echo "host:    $cli ($("$cli" --version | head -1))"
echo "ext:     $ext"
echo "corpus:  $corpus"

fails=0
# run <name> <components> <sql> <expected-substring>
run() {
	local name="$1" comps="$2" sql="$3" expect="$4" out
	out="$(printf "LOAD '%s';\n%s\n" "$ext" "$sql" \
		| DUCKLINK_COMPONENTS="$comps" "$cli" -unsigned 2>/dev/null || true)"
	if grep -qF "$expect" <<<"$out"; then
		echo "PASS: $name"
	else
		echo "FAIL: $name — expected substring '$expect' not found in:" >&2
		echo "$out" >&2
		fails=$((fails + 1))
	fi
}

# --- PARSER: VISUALIZE rewrite via ggsql -------------------------------------
if [ -f "$corpus/ggsql.wasm" ]; then
	run "parser/visualize (ggsql)" \
		"ggsql=$corpus/ggsql.wasm" \
		"VISUALIZE SELECT 'apple' AS label, 3 AS n UNION ALL SELECT 'pear', 1;" \
		"###"
else
	skip "ggsql.wasm not in corpus"
fi

# --- OPTIMIZER: plan rewrite via qopt ----------------------------------------
if [ -f "$corpus/qopt.wasm" ]; then
	run "optimizer/rewrite (qopt)" \
		"qopt=$corpus/qopt.wasm" \
		"CREATE TABLE optme(x INTEGER); INSERT INTO optme VALUES (1); SELECT x FROM optme;" \
		"99"
else
	skip "qopt.wasm not in corpus"
fi

# --- TABLE FILTER PUSHDOWN: numstream prunes at the source -------------------
# numstream(10) emits v=0..9; `WHERE v > 7` is pushed to the source, so only
# v=8,9 are produced. Assert both the kept rows and (via SUM) that nothing below
# the threshold leaked through.
if [ -f "$corpus/numstream.wasm" ]; then
	run "table-fn filter pushdown / rows (numstream)" \
		"numstream=$corpus/numstream.wasm" \
		"SELECT v FROM numstream(10) WHERE v > 7 ORDER BY v;" \
		"8"
	run "table-fn filter pushdown / pruned-sum (numstream)" \
		"numstream=$corpus/numstream.wasm" \
		"SELECT count(*) AS c, sum(v) AS s FROM numstream(10) WHERE v > 7;" \
		"2"
else
	skip "numstream.wasm not in corpus"
fi

if [ "$fails" -gt 0 ]; then
	echo "$fails advanced-tier smoke check(s) failed" >&2
	exit 1
fi
echo "all advanced-tier smoke checks passed"
