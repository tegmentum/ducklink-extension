#!/usr/bin/env bash
# gen-shim.sh - emit a DuckLink shim (or native) .duckdb_extension into a repo.
#
# The DuckLink cdylib exports one `<name>_init_c_api` per managed name (added via
# the `ducklink_shim!` table in src/lib.rs) plus the native-provider entrypoints.
# This stamps a copy as `<name>.duckdb_extension` with the DuckDB metadata footer
# and lays it out at <repo>/<duckdb_version>/<platform>/ (the INSTALL FROM layout).
#
# Usage: gen-shim.sh <name> <platform> <duckdb_version> <repo_dir> <lib_path>
#   e.g. gen-shim.sh aba osx_arm64 v1.5.4 ./ducklink-repo target/debug/libducklink.dylib
set -euo pipefail
NAME=${1:?name}; PLATFORM=${2:?platform}; DV=${3:?duckdb_version}; REPO=${4:?repo_dir}; LIB=${5:?lib_path}
HERE=$(cd "$(dirname "$0")/.." && pwd)
APPEND="$HERE/extension-ci-tools/scripts/append_extension_metadata.py"
OUT="$REPO/$DV/$PLATFORM"
mkdir -p "$OUT"
RAW=$(mktemp -t "$NAME.raw.XXXX")
cp "$LIB" "$RAW"
python3 "$APPEND" -l "$RAW" -o "$OUT/$NAME.duckdb_extension" -n "$NAME" \
  --abi-type C_STRUCT_UNSTABLE -dv "$DV" -p "$PLATFORM" -ev v0.0.1
gzip -kf "$OUT/$NAME.duckdb_extension"
rm -f "$RAW"
echo "wrote $OUT/$NAME.duckdb_extension (+ .gz)"
