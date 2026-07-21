//! The conformance-suite runner for this repo's implementation of the
//! ducklink surface. Discovers every `conformance/scripts/*.sql`, runs
//! it against a freshly-loaded in-memory DuckDB, and diffs the output
//! against `conformance/expected/<name>.out`. Any script whose output
//! doesn't match its expected file is a failure.
//!
//! See `conformance/README.md` for the format and intent. Other hosts
//! (workspace `ducklink-host`, future ports) are expected to run the
//! same scripts through their own runners and produce identical
//! outputs.
//!
//! # Golden-file bootstrapping
//!
//! Set `DUCKLINK_CONFORMANCE_BLESS=1` when running to overwrite the
//! `expected/` files with the actual output. Use only after a
//! deliberate surface change — every commit that modifies expected
//! outputs should carry a matching STABILITY.md / CHANGELOG entry.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::ffi;
use duckdb::Connection;
use ducklink::engine::Engine2;
use ducklink::reg_duckdb::register_load_function;

fn conformance_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("conformance")
}

/// Format one query's rows as CSV (RFC 4180). The workspace ducklink
/// CLI produces this shape via `.mode csv`, so both hosts can emit
/// byte-identical text against the same `expected/*.out` files.
///
/// A value is quoted (and its internal quotes doubled) when it
/// contains a comma, a double-quote, a newline, or a carriage return.
/// Deliberately not DuckDB's default `.mode table` (box-drawing)
/// because that shape is unstable across CLI versions and can't be
/// hand-authored.
fn render_result(rows: Vec<Vec<String>>, columns: &[String]) -> String {
    let mut out = String::new();
    out.push_str(&csv_row(columns));
    out.push('\n');
    for row in rows {
        out.push_str(&csv_row(&row));
        out.push('\n');
    }
    out
}

/// Render one CSV row. Quote cells that contain a comma, quote,
/// newline, or CR; internal quotes double up per RFC 4180.
fn csv_row(cells: &[String]) -> String {
    cells
        .iter()
        .map(|c| csv_cell(c))
        .collect::<Vec<_>>()
        .join(",")
}

fn csv_cell(v: &str) -> String {
    let needs_quote = v.contains(',') || v.contains('"') || v.contains('\n') || v.contains('\r');
    if needs_quote {
        format!("\"{}\"", v.replace('"', "\"\""))
    } else {
        v.to_string()
    }
}

/// Run a single SQL script — every statement in turn — and collect
/// the rendered output of every SELECT-shaped statement (statements
/// that return columns).
fn run_script(con: &Connection, script: &str) -> Result<String, String> {
    let mut out = String::new();
    for raw in script.split(';') {
        let stmt = raw.trim();
        // Strip any leading `--` comment lines and blank lines. The
        // resulting body is the real SQL to send to the engine; if
        // there's nothing left, this "statement" was a pure comment
        // block between real ones.
        let body = stmt
            .lines()
            .skip_while(|l| {
                let t = l.trim_start();
                t.is_empty() || t.starts_with("--")
            })
            .collect::<Vec<_>>()
            .join("\n");
        let body = body.trim();
        if body.is_empty() {
            continue;
        }
        let mut prep = con
            .prepare(body)
            .map_err(|e| format!("prepare `{body}`: {e}"))?;
        // Try SELECT-shape first (populates column_names via query());
        // fall back to execute() for DDL/LOAD/SET statements that have
        // no result set.
        let query_res = prep.query([]);
        match query_res {
            Ok(mut r) => {
                let mut rows: Vec<Vec<String>> = Vec::new();
                let mut columns: Vec<String> = Vec::new();
                while let Some(row) = r.next().map_err(|e| format!("row: {e}"))? {
                    if columns.is_empty() {
                        // duckdb-rs exposes column_names on the Rows via the
                        // stmt handle, populated after the first row.
                        columns = row
                            .as_ref()
                            .column_names()
                            .into_iter()
                            .map(|s| s.to_string())
                            .collect();
                    }
                    let col_count = columns.len();
                    let mut cells = Vec::with_capacity(col_count);
                    for i in 0..col_count {
                        let s: Option<String> = row.get(i).unwrap_or(None);
                        cells.push(s.unwrap_or_else(|| "NULL".to_string()));
                    }
                    rows.push(cells);
                }
                // Zero-row result: still emit the header if we can derive
                // it from the statement. Otherwise the diff can't tell
                // "no rows because query returned empty" from "no rows
                // because query never ran".
                if columns.is_empty() {
                    columns = prep
                        .column_names()
                        .into_iter()
                        .map(|s| s.to_string())
                        .collect();
                }
                if !columns.is_empty() {
                    // No trailing blank line between statement blocks:
                    // DuckDB's `.mode csv` (which the workspace runner
                    // uses) emits `col,col\nval,val\n` with no
                    // separator, so this runner has to match to stay
                    // byte-compatible.
                    out.push_str(&render_result(rows, &columns));
                }
            }
            Err(_) => {
                // Retry as an execute — DDL / SET / LOAD land here.
                prep.execute([])
                    .map_err(|e| format!("execute `{body}`: {e}"))?;
            }
        }
    }
    Ok(out)
}

fn discover_scripts() -> Vec<PathBuf> {
    let scripts_dir = conformance_root().join("scripts");
    let mut scripts: Vec<PathBuf> = fs::read_dir(&scripts_dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", scripts_dir.display()))
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("sql"))
        .collect();
    scripts.sort();
    scripts
}

fn expected_path(script: &Path) -> PathBuf {
    let stem = script.file_stem().unwrap().to_string_lossy();
    conformance_root().join("expected").join(format!("{stem}.out"))
}

#[test]
fn conformance_suite() {
    let bless = std::env::var("DUCKLINK_CONFORMANCE_BLESS").is_ok();
    let scripts = discover_scripts();
    assert!(!scripts.is_empty(), "no conformance scripts found");

    let mut failures = Vec::new();

    for script_path in &scripts {
        let script = fs::read_to_string(script_path)
            .unwrap_or_else(|e| panic!("read {}: {e}", script_path.display()));
        // Open a fresh DuckDB, wire ducklink's `ducklink_load` +
        // discovery views + all SQL entry points into it, and drop
        // the `LOAD ducklink;` from the script text — the scripts
        // include it for portability across hosts, but this
        // in-process runner already has ducklink linked in.
        let (con, _db) = unsafe {
            let mut db: ffi::duckdb_database = std::ptr::null_mut();
            assert_eq!(
                ffi::duckdb_open(c":memory:".as_ptr(), &mut db),
                ffi::DuckDBSuccess
            );
            let con = Connection::open_from_raw(db.cast()).expect("open_from_raw");
            (con, db)
        };
        let engine = Arc::new(Engine2::new().expect("engine"));
        // Only the FIRST script (or all if the RUNTIME lock allows)
        // will bind the process-wide RUNTIME; subsequent scripts share
        // it. Failures here would be visible; a soft-error is fine.
        let _ = register_load_function(&con, _db, engine);

        let filtered: String = script
            .lines()
            .filter(|l| !l.trim_start().to_ascii_lowercase().starts_with("load ducklink"))
            .collect::<Vec<_>>()
            .join("\n");

        let actual = match run_script(&con, &filtered) {
            Ok(s) => s,
            Err(e) => {
                failures.push(format!("{}: {e}", script_path.display()));
                continue;
            }
        };

        let expected_p = expected_path(script_path);
        if bless {
            fs::write(&expected_p, &actual).expect("write expected");
            eprintln!("[bless] wrote {}", expected_p.display());
            continue;
        }
        let expected = fs::read_to_string(&expected_p).unwrap_or_default();
        if expected != actual {
            failures.push(format!(
                "{}:\n  expected ({}):\n{}\n  actual:\n{}",
                script_path.display(),
                expected_p.display(),
                expected,
                actual
            ));
        }
    }

    if !failures.is_empty() {
        panic!(
            "{} conformance failure(s):\n\n{}\n\n(Run with DUCKLINK_CONFORMANCE_BLESS=1 to rewrite expected outputs after a deliberate surface change.)",
            failures.len(),
            failures.join("\n---\n")
        );
    }
}
