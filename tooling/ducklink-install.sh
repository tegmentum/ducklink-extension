#!/usr/bin/env bash
# ducklink-install.sh - pre-populate the local extension dir so plain
# `LOAD <name>` works on stock DuckDB (the `ducklink install <name>` step).
#
# Drives stock DuckDB's `INSTALL <name> FROM '<repo>'`, which fetches the shim
# from the DuckLink repo and copies it into the extension directory. Thereafter a
# plain `LOAD <name>` (any session) loads it - no manual INSTALL.
#
# Usage: ducklink-install.sh <name> <repo_dir> <extension_dir> [duckdb_cli]
set -euo pipefail
NAME=${1:?name}; REPO=${2:?repo_dir}; EXTDIR=${3:?extension_dir}; CLI=${4:-duckdb}
mkdir -p "$EXTDIR"
"$CLI" -unsigned :memory: <<SQL
SET extension_directory='$EXTDIR';
SET custom_extension_repository='$REPO';
INSTALL $NAME FROM '$REPO';
SQL
echo "ducklink: installed '$NAME' into $EXTDIR (plain 'LOAD $NAME' now works)"
