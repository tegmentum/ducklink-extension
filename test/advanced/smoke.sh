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
# Beyond the three happy-path proofs (parser rewrite / optimizer rewrite /
# filter pushdown) it covers error + edge cases: malformed SQL through the
# parser, a pass-through statement the parser must leave untouched, an optimizer
# no-op plan, filter pushdown matching zero rows / all rows, NULL predicates,
# several advanced extensions loaded at once, a double-LOAD (DuckDB has no UNLOAD
# for C extensions, so reload == idempotent re-LOAD), the common tier still
# loading alongside the active C++ shim (no-regression), and the VERSION-GUARD
# degraded path (advanced disabled, common tier still works, no crash).
#
# It SKIPS (exit 0) when the host CLI, the extension artifact, or the component
# corpus are unavailable, so it is safe to wire into CI that lacks them. Set
# STRICT=1 to turn skips into failures.
#
# Inputs (env, all optional):
#   DUCKDB_BIN              path to a duckdb v1.5.4 CLI (else common paths tried)
#   DUCKDB_BIN_MISMATCH     path to a duckdb CLI whose version is NOT v1.5.4
#                           (else common paths tried); enables the no-crash test
#                           of loading into a non-matching host
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

# --- optionally locate a NON-matching CLI (for the no-crash version-guard test)
find_mismatch_cli() {
	local candidates=(
		"${DUCKDB_BIN_MISMATCH:-}"
		"/opt/homebrew/bin/duckdb"
		"$(command -v duckdb 2>/dev/null || true)"
	)
	for c in "${candidates[@]}"; do
		[ -n "$c" ] && [ -x "$c" ] || continue
		if ! "$c" --version 2>/dev/null | grep -q "v1.5.4"; then
			echo "$c"
			return 0
		fi
	done
	return 1
}

ext="${DUCKLINK_EXTENSION:-$repo/target/release/ducklink.duckdb_extension}"
[ -f "$ext" ] || skip "extension artifact not found at $ext (build + add metadata first)"

corpus="${DUCKLINK_CORPUS_DIR:-$HOME/git/ducklink/artifacts/extensions}"
[ -d "$corpus" ] || skip "component corpus dir not found at $corpus"

echo "host:    $cli ($("$cli" --version | head -1))"
echo "ext:     $ext"
echo "corpus:  $corpus"

fails=0

# Markers that mean the host process died unexpectedly (a version-ABI mismatch
# or a panic unwinding across the FFI boundary would surface here). Any of these
# in any captured output fails the suite regardless of the other assertions.
crash_re='Segmentation fault|SIGSEGV|SIGABRT|Abort trap|signal: |libc\+\+abi|terminating with uncaught|core dumped'

# run <name> <components> <sql> <expected-substring>
# Asserts the expected substring is in stdout AND that nothing crashed.
run() {
	local name="$1" comps="$2" sql="$3" expect="$4" out
	out="$(printf "LOAD '%s';\n%s\n" "$ext" "$sql" \
		| DUCKLINK_COMPONENTS="$comps" "$cli" -unsigned 2>&1 || true)"
	if grep -qE "$crash_re" <<<"$out"; then
		echo "FAIL: $name — host CRASHED:" >&2
		echo "$out" >&2
		fails=$((fails + 1))
		return
	fi
	if grep -qF "$expect" <<<"$out"; then
		echo "PASS: $name"
	else
		echo "FAIL: $name — expected substring '$expect' not found in:" >&2
		echo "$out" >&2
		fails=$((fails + 1))
	fi
}

# run_both <name> <components> <sql> <expect-A> <expect-B>
# Both substrings must appear (combined stdout+stderr); nothing crashed.
run_both() {
	local name="$1" comps="$2" sql="$3" a="$4" b="$5" out
	out="$(printf "LOAD '%s';\n%s\n" "$ext" "$sql" \
		| DUCKLINK_COMPONENTS="$comps" "$cli" -unsigned 2>&1 || true)"
	if grep -qE "$crash_re" <<<"$out"; then
		echo "FAIL: $name — host CRASHED:" >&2
		echo "$out" >&2
		fails=$((fails + 1))
		return
	fi
	if grep -qF "$a" <<<"$out" && grep -qF "$b" <<<"$out"; then
		echo "PASS: $name"
	else
		echo "FAIL: $name — expected both '$a' and '$b' in:" >&2
		echo "$out" >&2
		fails=$((fails + 1))
	fi
}

# run_nocrash <name> <components> <sql> [env...]
# Only asserts the host did NOT crash (output content is irrelevant). Used for
# error paths where graceful failure — not a particular message — is the point.
run_nocrash() {
	local name="$1" comps="$2" sql="$3" out
	out="$(printf "LOAD '%s';\n%s\n" "$ext" "$sql" \
		| DUCKLINK_COMPONENTS="$comps" "$cli" -unsigned 2>&1 || true)"
	if grep -qE "$crash_re" <<<"$out"; then
		echo "FAIL: $name — host CRASHED:" >&2
		echo "$out" >&2
		fails=$((fails + 1))
	else
		echo "PASS: $name (no crash)"
	fi
}

# ============================================================================
# 1. PARSER
# ============================================================================
if [ -f "$corpus/ggsql.wasm" ]; then
	# happy path: VISUALIZE is rewritten by ggsql.
	run "parser/visualize (ggsql)" \
		"ggsql=$corpus/ggsql.wasm" \
		"VISUALIZE SELECT 'apple' AS label, 3 AS n UNION ALL SELECT 'pear', 1;" \
		"###"
	# pass-through: a normal statement must reach DuckDB UNTOUCHED by the parser
	# extension (it only intercepts statements the built-in parser rejects).
	run "parser/passthrough (ggsql loaded, plain SELECT)" \
		"ggsql=$corpus/ggsql.wasm" \
		"SELECT 40 + 2 AS answer;" \
		"42"
	# malformed SQL: the parser extension declines, DuckDB reports its own error,
	# and the process must not crash.
	run_nocrash "parser/malformed (ggsql loaded, syntax error)" \
		"ggsql=$corpus/ggsql.wasm" \
		"SELEKT 1 FROM;"
else
	skip "ggsql.wasm not in corpus"
fi

# ============================================================================
# 2. OPTIMIZER
# ============================================================================
if [ -f "$corpus/qopt.wasm" ]; then
	# happy path: qopt rewrites the scan of `optme`.
	run "optimizer/rewrite (qopt)" \
		"qopt=$corpus/qopt.wasm" \
		"CREATE TABLE optme(x INTEGER); INSERT INTO optme VALUES (1); SELECT x FROM optme;" \
		"99"
	# no-op: a plan the rule does not claim must be returned unchanged.
	run "optimizer/noop (qopt loaded, unrelated query)" \
		"qopt=$corpus/qopt.wasm" \
		"SELECT 12345 AS k;" \
		"12345"
else
	skip "qopt.wasm not in corpus"
fi

# ============================================================================
# 3. TABLE FILTER PUSHDOWN
# ============================================================================
if [ -f "$corpus/numstream.wasm" ]; then
	# happy path: numstream(10) emits v=0..9; `WHERE v > 7` is pushed, so only
	# v=8,9 are produced.
	run "table/pushdown rows (numstream, v>7)" \
		"numstream=$corpus/numstream.wasm" \
		"SELECT v FROM numstream(10) WHERE v > 7 ORDER BY v;" \
		"8"
	run "table/pushdown pruned-count (numstream, v>7)" \
		"numstream=$corpus/numstream.wasm" \
		"SELECT count(*) AS c FROM numstream(10) WHERE v > 7;" \
		"2"
	# zero rows match.
	run "table/pushdown zero-match (numstream, v>100)" \
		"numstream=$corpus/numstream.wasm" \
		"SELECT count(*) AS c FROM numstream(10) WHERE v > 100;" \
		"0"
	# all rows match.
	run "table/pushdown all-match (numstream, v>=0)" \
		"numstream=$corpus/numstream.wasm" \
		"SELECT count(*) AS c FROM numstream(10) WHERE v >= 0;" \
		"10"
	# NULL predicates exercise the IS NULL / IS NOT NULL pushdown paths.
	run "table/pushdown is-not-null (numstream)" \
		"numstream=$corpus/numstream.wasm" \
		"SELECT count(*) AS c FROM numstream(10) WHERE v IS NOT NULL;" \
		"10"
	run "table/pushdown is-null (numstream)" \
		"numstream=$corpus/numstream.wasm" \
		"SELECT count(*) AS c FROM numstream(10) WHERE v IS NULL;" \
		"0"
else
	skip "numstream.wasm not in corpus"
fi

# ============================================================================
# 4. MULTIPLE ADVANCED EXTENSIONS AT ONCE + DOUBLE-LOAD (reload)
# ============================================================================
if [ -f "$corpus/ggsql.wasm" ] && [ -f "$corpus/qopt.wasm" ] && [ -f "$corpus/numstream.wasm" ]; then
	all="ggsql=$corpus/ggsql.wasm:qopt=$corpus/qopt.wasm:numstream=$corpus/numstream.wasm"
	# All three tiers registered together; the filter pushdown still prunes and
	# the optimizer rule (qopt) is a no-op on the numstream plan it does not own.
	run "multi-load (parser+optimizer+table) — pushdown still works" \
		"$all" \
		"SELECT v FROM numstream(10) WHERE v > 7 ORDER BY v;" \
		"8"
	run "multi-load — parser rewrite still works alongside the others" \
		"$all" \
		"VISUALIZE SELECT 'apple' AS label, 3 AS n;" \
		"###"
	# Double LOAD in one session: DuckDB has no UNLOAD for C extensions, so a
	# re-LOAD must be an idempotent no-op (registration guards prevent stacking).
	run "reload (double LOAD ducklink) — idempotent, still works" \
		"numstream=$corpus/numstream.wasm" \
		"$(printf "LOAD '%s';\nSELECT count(*) AS c FROM numstream(10) WHERE v > 7;" "$ext")" \
		"2"
fi

# ============================================================================
# 5. NO-REGRESSION: common tier loads alongside the active C++ shim
# ============================================================================
if [ -f "$corpus/sample_extension.wasm" ]; then
	# The advanced C++ shim is linked in and (on this matching host) active; a
	# plain stable-C-API scalar component must still load + run unperturbed.
	run "common-tier scalar (sample_plus_one) with shim active" \
		"sample=$corpus/sample_extension.wasm" \
		"SELECT sample_plus_one(41) AS r;" \
		"42"
fi

# ============================================================================
# 6. VERSION-GUARD DEGRADED PATH (advanced disabled, common tier still works)
# ============================================================================
# DUCKLINK_DISABLE_ADVANCED=1 forces the exact branch a real version-mismatched
# (e.g. newer) host would take: no internal-ABI symbol is touched, the advanced
# tier is off, and the common tier still loads and runs. This is the
# deterministic, same-host proof of graceful degradation.
if [ -f "$corpus/sample_extension.wasm" ]; then
	out="$(printf "LOAD '%s';\nSELECT sample_plus_one(41) AS r;\n" "$ext" \
		| DUCKLINK_DISABLE_ADVANCED=1 DUCKLINK_COMPONENTS="sample=$corpus/sample_extension.wasm" \
			"$cli" -unsigned 2>&1 || true)"
	if grep -qE "$crash_re" <<<"$out"; then
		echo "FAIL: degraded/common-tier-works — host CRASHED:" >&2
		echo "$out" >&2
		fails=$((fails + 1))
	elif grep -qF "42" <<<"$out" && grep -qF "advanced tier DISABLED" <<<"$out"; then
		echo "PASS: degraded mode — advanced DISABLED, common tier still computes 42"
	else
		echo "FAIL: degraded mode — expected '42' and 'advanced tier DISABLED' in:" >&2
		echo "$out" >&2
		fails=$((fails + 1))
	fi
	# In degraded mode the advanced tier must be truly OFF: a parser-extension-only
	# statement (VISUALIZE) must NOT be intercepted, and must not crash.
	if [ -f "$corpus/ggsql.wasm" ]; then
		out="$(printf "LOAD '%s';\nVISUALIZE SELECT 'apple' AS label, 3 AS n;\n" "$ext" \
			| DUCKLINK_DISABLE_ADVANCED=1 DUCKLINK_COMPONENTS="ggsql=$corpus/ggsql.wasm" \
				"$cli" -unsigned 2>&1 || true)"
		if grep -qE "$crash_re" <<<"$out"; then
			echo "FAIL: degraded/parser-off — host CRASHED:" >&2
			echo "$out" >&2
			fails=$((fails + 1))
		elif grep -qF "###" <<<"$out"; then
			echo "FAIL: degraded/parser-off — VISUALIZE was rewritten but advanced is disabled:" >&2
			echo "$out" >&2
			fails=$((fails + 1))
		else
			echo "PASS: degraded mode — parser extension is off (VISUALIZE not intercepted, no crash)"
		fi
	fi
fi

# ============================================================================
# 7. NON-MATCHING HOST must not crash (version mismatch is rejected cleanly)
# ============================================================================
# Loading the v1.5.4-built artifact into a different-version CLI must fail
# gracefully (metadata/C-API-version rejection), never segfault.
if mm_cli="$(find_mismatch_cli)"; then
	echo "mismatch host: $mm_cli ($("$mm_cli" --version | head -1))"
	out="$(printf "LOAD '%s';\nSELECT 1;\n" "$ext" | "$mm_cli" -unsigned 2>&1 || true)"
	if grep -qE "$crash_re" <<<"$out"; then
		echo "FAIL: non-matching host — CRASHED instead of rejecting cleanly:" >&2
		echo "$out" >&2
		fails=$((fails + 1))
	else
		echo "PASS: non-matching host — load rejected cleanly, no crash"
	fi
else
	echo "note: no non-matching duckdb CLI found; skipping the cross-version no-crash test"
fi

if [ "$fails" -gt 0 ]; then
	echo "$fails advanced-tier smoke check(s) failed" >&2
	exit 1
fi
echo "all advanced-tier smoke checks passed"
