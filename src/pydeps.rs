//! PEP 723 inline-dependency resolution + pure-Python wheel staging for the
//! `ducklink_run` Python source tier.
//!
//! A `ducklink_run('script.py')` script MAY declare inline dependencies in a
//! PEP 723 `# /// script` block:
//!
//! ```text
//! # /// script
//! # dependencies = ["six"]
//! # ///
//! ```
//!
//! Before the script is imported into the resident pylon interpreter, this module
//! parses that block, resolves each requirement to a PURE-PYTHON wheel on PyPI,
//! downloads + unzips it (content-addressed under the ducklink cache), and stages
//! the package tree into a `site-packages` dir the guest imports from. The pylon
//! dispatcher prepends that dir to `sys.path` (see `pylib/pylon_endpoint.py`), so
//! the script's `import <dep>` resolves inside the guest.
//!
//! ## Pure-Python only (native deps = Phase 5)
//!
//! pylon today installs PURE-PYTHON wheels onto `sys.path` (see
//! `python-wasm/docs/wheel-install.md`: `pip install --no-deps --target
//! /site-packages` of a `*-none-any` wheel, mounted + on `PYTHONPATH`). C-extension
//! / native wheels (a compiled `.so`/ABI tag) are NOT runnable in the wasm
//! interpreter yet — that is the Phase-5 native-dep pipeline. This resolver
//! therefore accepts ONLY wheels whose platform tag is `none-any` (a pure-Python
//! `py3-none-any` / `cp3x-none-any` wheel); a requirement that offers only a
//! native wheel FAILS with a clear message naming the Phase-5 boundary.
//!
//! ## Resolution model (MVP)
//!
//! This stages a `site-packages` dir directly. Pylon's content-addressed `env-id`
//! (uv-wasm, roadmap) is NOT yet wired into the ducklink host, so a staged
//! `site-packages` is the accepted MVP: each requirement's top-level pure-Python
//! wheel is fetched from PyPI's JSON API and unzipped. TRANSITIVE dependencies are
//! NOT resolved (no dep solver here) — a script needing a dependency's own
//! dependencies must list them all in its PEP 723 block, matching pylon's own
//! `pip install --no-deps` posture. Version specifiers are honored only loosely:
//! the newest release whose version satisfies a simple `==`/`>=` bound (or the
//! newest overall when unbounded) that has a pure-Python wheel is chosen.

use std::path::{Path, PathBuf};
use std::time::Duration;

/// A parsed requirement: the distribution name plus an optional exact/min version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Requirement {
    /// PyPI project name (normalized lower-case, `_`/`.` -> `-` for the API URL).
    pub name: String,
    /// `Some("==", "1.2.3")` / `Some(">=", "1.0")` when a simple bound was given.
    pub bound: Option<(String, String)>,
}

/// Parse a PEP 723 `# /// script` block's `dependencies` list from `source`.
///
/// Mirrors the reference PEP 723 extraction (the ducklink SDK's `pep723.py`):
/// find the single `# /// script` ... `# ///` block, strip the `# ` prefix from
/// each line, and read the `dependencies = [...]` array. Kept in Rust (rather than
/// a guest round-trip) so the resolve/stage happens in `bind` before the script is
/// ever imported. Returns an empty list when there is no block or no key.
pub fn parse_dependencies(source: &str) -> Result<Vec<Requirement>, String> {
    let Some(block) = read_script_block(source)? else {
        return Ok(Vec::new());
    };
    let deps = extract_dependencies_array(&block)?;
    deps.iter().map(|d| parse_requirement(d)).collect()
}

/// Extract the raw (un-prefixed) body of the single `# /// script` block, or
/// `None` if absent. Errors if the block appears more than once (per the spec).
fn read_script_block(source: &str) -> Result<Option<String>, String> {
    let lines: Vec<&str> = source.lines().collect();
    let mut blocks: Vec<String> = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        // Opening fence: a line that is exactly `# /// script`.
        if lines[i].trim_end() == "# /// script" {
            let mut body = String::new();
            let mut j = i + 1;
            let mut closed = false;
            while j < lines.len() {
                let l = lines[j];
                if l.trim_end() == "# ///" {
                    closed = true;
                    break;
                }
                // A block line must start with `#`. Strip `# ` (or a bare `#`).
                if let Some(rest) = l.strip_prefix("# ") {
                    body.push_str(rest);
                    body.push('\n');
                } else if l.trim_end() == "#" {
                    body.push('\n');
                } else {
                    // Non-comment line inside the fence: malformed; stop scanning
                    // this block (treat the fence as not a real PEP 723 block).
                    closed = false;
                    break;
                }
                j += 1;
            }
            if closed {
                blocks.push(body);
                i = j + 1;
                continue;
            }
        }
        i += 1;
    }
    match blocks.len() {
        0 => Ok(None),
        1 => Ok(Some(blocks.pop().unwrap())),
        n => Err(format!(
            "ducklink_run: {n} PEP 723 'script' metadata blocks found; the spec permits at most one"
        )),
    }
}

/// Pull the `dependencies = [ "a", "b>=1" ]` string array out of the block body.
/// A minimal TOML-array reader (the block is trusted, small, and the SDK writes a
/// plain string list) — good enough without a TOML dependency. Absent key -> [].
fn extract_dependencies_array(block: &str) -> Result<Vec<String>, String> {
    // Find `dependencies` `=` `[` ... `]`, possibly spanning lines.
    let Some(eq) = find_dependencies_assignment(block) else {
        return Ok(Vec::new());
    };
    let after = &block[eq..];
    let open = after
        .find('[')
        .ok_or_else(|| "ducklink_run: PEP 723 'dependencies' must be a list".to_string())?;
    let close = after[open..]
        .find(']')
        .ok_or_else(|| "ducklink_run: PEP 723 'dependencies' list is not closed".to_string())?
        + open;
    let inner = &after[open + 1..close];
    let mut out = Vec::new();
    for raw in inner.split(',') {
        let s = raw.trim().trim_matches(|c| c == '"' || c == '\'').trim();
        if !s.is_empty() {
            out.push(s.to_string());
        }
    }
    Ok(out)
}

/// Locate the byte offset of a top-level `dependencies =` assignment in the block.
fn find_dependencies_assignment(block: &str) -> Option<usize> {
    let mut offset = 0usize;
    for line in block.lines() {
        let trimmed = line.trim_start();
        if let Some(rest) = trimmed.strip_prefix("dependencies") {
            if rest.trim_start().starts_with('=') {
                // Offset of the assignment within the whole block.
                let line_start = offset + (line.len() - trimmed.len());
                return Some(line_start);
            }
        }
        offset += line.len() + 1; // +1 for the '\n' `lines()` stripped.
    }
    None
}

/// Parse one requirement string (`"six"`, `"six>=1.16"`, `"six==1.16.0"`) into a
/// [`Requirement`]. Extras / environment markers are stripped (best-effort MVP);
/// only `==` / `>=` bounds are recognized.
fn parse_requirement(req: &str) -> Result<Requirement, String> {
    // Drop an environment marker (`; python_version >= ...`) and extras (`[x]`):
    // `pkg[extra]>=1` -> `pkg>=1` (extras aren't resolved in the MVP).
    let req = req.split(';').next().unwrap_or(req).trim();
    let req = match (req.find('['), req.find(']')) {
        (Some(open), Some(close)) if close > open => {
            format!("{}{}", &req[..open], &req[close + 1..])
        }
        _ => req.to_string(),
    };
    let req = req.trim();
    for op in ["==", ">=", "~=", ">"] {
        if let Some(pos) = req.find(op) {
            let name = req[..pos].trim();
            let ver = req[pos + op.len()..].trim();
            if name.is_empty() {
                return Err(format!("ducklink_run: malformed requirement '{req}'"));
            }
            let op = if op == "~=" || op == ">" { ">=" } else { op };
            return Ok(Requirement {
                name: normalize_name(name),
                bound: Some((op.to_string(), ver.to_string())),
            });
        }
    }
    if req.is_empty() {
        return Err("ducklink_run: empty requirement in PEP 723 block".to_string());
    }
    Ok(Requirement {
        name: normalize_name(req),
        bound: None,
    })
}

/// Normalize a PyPI project name for the JSON API: lower-case, runs of
/// `-`/`_`/`.` collapsed to a single `-` (PEP 503).
fn normalize_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut prev_sep = false;
    for c in name.chars() {
        if c == '-' || c == '_' || c == '.' {
            if !prev_sep && !out.is_empty() {
                out.push('-');
            }
            prev_sep = true;
        } else {
            out.push(c.to_ascii_lowercase());
            prev_sep = false;
        }
    }
    out.trim_end_matches('-').to_string()
}

// ---------------------------------------------------------------------------
// PyPI resolution + pure-Python wheel staging
// ---------------------------------------------------------------------------

/// A pure-Python wheel chosen for a requirement: the release version + the wheel
/// download URL + its expected sha256 (from the PyPI index, for verification).
struct ChosenWheel {
    version: String,
    url: String,
    sha256: Option<String>,
    filename: String,
}

/// Resolve + stage every requirement's pure-Python wheel into `site_packages`.
/// Content-addressed under the ducklink cache so a repeat run is a cache hit.
/// Returns the list of `(name, version)` staged, for the summary/log. A native-
/// only requirement fails with a clear Phase-5-boundary message.
pub fn stage_dependencies(
    reqs: &[Requirement],
    site_packages: &Path,
) -> Result<Vec<(String, String)>, String> {
    std::fs::create_dir_all(site_packages)
        .map_err(|e| format!("ducklink_run: create site-packages {}: {e}", site_packages.display()))?;
    let mut staged = Vec::new();
    for req in reqs {
        let wheel = choose_pure_python_wheel(req)?;
        stage_wheel(&wheel, site_packages)?;
        crate::events::emit(
            "pydep_staged",
            Some(&req.name),
            format!("{} {}", req.name, wheel.version),
        );
        staged.push((req.name.clone(), wheel.version));
    }
    Ok(staged)
}

/// A blocking reqwest client mirroring the catalog fetch posture.
fn http_client() -> Result<reqwest::blocking::Client, String> {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .map_err(|e| format!("ducklink_run: http client init failed: {e}"))
}

/// Query the PyPI JSON API for `req` and pick the newest release that offers a
/// PURE-PYTHON (`*-none-any.whl`) wheel and satisfies the requirement's bound.
/// Fails clearly when the project has releases but NONE with a pure-Python wheel
/// (native-only -> Phase 5), and separately when the project is unknown.
fn choose_pure_python_wheel(req: &Requirement) -> Result<ChosenWheel, String> {
    let url = format!("https://pypi.org/pypi/{}/json", req.name);
    let client = http_client()?;
    let resp = client
        .get(&url)
        .send()
        .map_err(|e| format!("ducklink_run: PyPI query for '{}' failed: {e}", req.name))?;
    if resp.status().as_u16() == 404 {
        return Err(format!(
            "ducklink_run: PEP 723 dependency '{}' not found on PyPI",
            req.name
        ));
    }
    if !resp.status().is_success() {
        return Err(format!(
            "ducklink_run: PyPI query for '{}' returned HTTP {}",
            req.name,
            resp.status()
        ));
    }
    let raw = resp
        .bytes()
        .map_err(|e| format!("ducklink_run: read PyPI response for '{}': {e}", req.name))?;
    let body: serde_json::Value = serde_json::from_slice(&raw)
        .map_err(|e| format!("ducklink_run: parse PyPI response for '{}': {e}", req.name))?;

    let releases = body
        .get("releases")
        .and_then(|r| r.as_object())
        .ok_or_else(|| format!("ducklink_run: PyPI response for '{}' has no releases", req.name))?;

    // Candidate versions satisfying the bound, newest first.
    let mut versions: Vec<&String> = releases
        .keys()
        .filter(|v| version_satisfies(v, req.bound.as_ref()))
        .collect();
    versions.sort_by(|a, b| cmp_version(b, a)); // descending

    let mut saw_native_only = false;
    for v in &versions {
        let files = releases.get(*v).and_then(|f| f.as_array());
        let Some(files) = files else { continue };
        let mut had_wheel = false;
        for f in files {
            let fname = f.get("filename").and_then(|x| x.as_str()).unwrap_or("");
            if !fname.ends_with(".whl") {
                continue;
            }
            if f.get("yanked").and_then(|y| y.as_bool()).unwrap_or(false) {
                continue;
            }
            had_wheel = true;
            if is_pure_python_wheel(fname) {
                let dl = f
                    .get("url")
                    .and_then(|x| x.as_str())
                    .ok_or_else(|| format!("ducklink_run: PyPI wheel for '{}' has no url", req.name))?;
                let sha = f
                    .get("digests")
                    .and_then(|d| d.get("sha256"))
                    .and_then(|x| x.as_str())
                    .map(|s| s.to_string());
                return Ok(ChosenWheel {
                    version: (*v).clone(),
                    url: dl.to_string(),
                    sha256: sha,
                    filename: fname.to_string(),
                });
            }
        }
        if had_wheel {
            saw_native_only = true;
        }
    }

    if saw_native_only {
        Err(format!(
            "ducklink_run: PEP 723 dependency '{}' offers only NATIVE (C-extension) wheels — the \
             wasm Python tier runs PURE-PYTHON wheels only. Native/binary dependencies are the \
             Phase-5 native-dep boundary and are not yet supported.",
            req.name
        ))
    } else {
        Err(format!(
            "ducklink_run: PEP 723 dependency '{}' has no installable wheel satisfying {} (only \
             source distributions / no matching release). The wasm Python tier needs a pure-Python \
             wheel; building from an sdist is the Phase-5 native-dep boundary.",
            req.name,
            req.bound
                .as_ref()
                .map(|(op, v)| format!("{op}{v}"))
                .unwrap_or_else(|| "any version".to_string()),
        ))
    }
}

/// True for a pure-Python wheel: the platform tag is `any` and the ABI tag is
/// `none` (`<dist>-<ver>[-build]-<py>-none-any.whl`). A wheel with any other ABI/
/// platform tag carries compiled code (native) and is rejected.
fn is_pure_python_wheel(filename: &str) -> bool {
    let stem = match filename.strip_suffix(".whl") {
        Some(s) => s,
        None => return false,
    };
    // Tags are the LAST three dash-separated fields: <python>-<abi>-<platform>.
    let parts: Vec<&str> = stem.rsplitn(4, '-').collect(); // [platform, abi, python, rest]
    if parts.len() < 3 {
        return false;
    }
    let platform = parts[0];
    let abi = parts[1];
    abi == "none" && platform == "any"
}

/// Download + unzip a wheel's package tree into `site_packages`. Content-addressed
/// by the wheel's sha256 (the PyPI digest when present, else the bytes' hash):
/// the raw wheel is cached under `<cache>/wheels/sha256/<digest>/<filename>` and
/// its contents are extracted into `site_packages` (skipping the `*.dist-info`
/// RECORD metadata's noise is unnecessary — we extract everything, which is what
/// `pip --target` lands on `sys.path` too).
fn stage_wheel(wheel: &ChosenWheel, site_packages: &Path) -> Result<(), String> {
    let bytes = fetch_wheel_bytes(wheel)?;
    let cursor = std::io::Cursor::new(bytes);
    let mut zip = zip::ZipArchive::new(cursor)
        .map_err(|e| format!("ducklink_run: open wheel {}: {e}", wheel.filename))?;
    for i in 0..zip.len() {
        let mut entry = zip
            .by_index(i)
            .map_err(|e| format!("ducklink_run: read wheel entry: {e}"))?;
        // Guard against zip-slip: only extract entries whose sanitized path stays
        // within site_packages.
        let Some(rel) = entry.enclosed_name() else {
            continue;
        };
        let dest = site_packages.join(&rel);
        if entry.is_dir() {
            std::fs::create_dir_all(&dest)
                .map_err(|e| format!("ducklink_run: mkdir {}: {e}", dest.display()))?;
            continue;
        }
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("ducklink_run: mkdir {}: {e}", parent.display()))?;
        }
        let mut out = std::fs::File::create(&dest)
            .map_err(|e| format!("ducklink_run: create {}: {e}", dest.display()))?;
        std::io::copy(&mut entry, &mut out)
            .map_err(|e| format!("ducklink_run: extract {}: {e}", dest.display()))?;
    }
    Ok(())
}

/// Fetch the raw wheel bytes, using a content-addressed cache under the ducklink
/// cache root and verifying the PyPI sha256 (when the index gave one).
fn fetch_wheel_bytes(wheel: &ChosenWheel) -> Result<Vec<u8>, String> {
    let cache = wheel
        .sha256
        .as_ref()
        .and_then(|d| wheel_cache_path(d, &wheel.filename));
    if let Some(p) = &cache {
        if p.is_file() {
            if let Ok(b) = std::fs::read(p) {
                return Ok(b);
            }
        }
    }
    let client = http_client()?;
    let resp = client
        .get(&wheel.url)
        .send()
        .map_err(|e| format!("ducklink_run: download wheel {} failed: {e}", wheel.filename))?;
    if !resp.status().is_success() {
        return Err(format!(
            "ducklink_run: download of {} returned HTTP {}",
            wheel.url,
            resp.status()
        ));
    }
    let bytes = resp
        .bytes()
        .map_err(|e| format!("ducklink_run: reading wheel body failed: {e}"))?
        .to_vec();
    if let Some(want) = &wheel.sha256 {
        let got = sha256_hex(&bytes);
        if &got != want {
            return Err(format!(
                "ducklink_run: sha256 mismatch for wheel {}: PyPI says {want}, got {got}",
                wheel.filename
            ));
        }
        if let Some(p) = wheel_cache_path(want, &wheel.filename) {
            let _ = write_cache(&p, &bytes);
        }
    }
    Ok(bytes)
}

fn wheel_cache_path(digest: &str, filename: &str) -> Option<PathBuf> {
    Some(
        crate::catalog::cache_root()?
            .join("wheels")
            .join("sha256")
            .join(digest)
            .join(filename),
    )
}

fn write_cache(cache_path: &Path, bytes: &[u8]) -> Result<(), String> {
    let parent = cache_path.parent().ok_or("cache path has no parent")?;
    std::fs::create_dir_all(parent).map_err(|e| format!("create {}: {e}", parent.display()))?;
    let tmp = cache_path.with_extension("partial");
    std::fs::write(&tmp, bytes).map_err(|e| format!("write {}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, cache_path).map_err(|e| format!("finalise {}: {e}", cache_path.display()))
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    let d = h.finalize();
    let mut s = String::with_capacity(64);
    for b in d {
        use std::fmt::Write as _;
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Does `version` satisfy the (loose) bound? `==` requires an exact match; `>=`
/// requires `version >= bound`. No bound -> always true. Unparseable versions
/// compare lexically as a fallback.
fn version_satisfies(version: &str, bound: Option<&(String, String)>) -> bool {
    // Skip pre-releases when a bound is present-or-absent unless explicitly asked.
    if is_prerelease(version) {
        return false;
    }
    match bound {
        None => true,
        Some((op, want)) => match op.as_str() {
            "==" => cmp_version(version, want) == std::cmp::Ordering::Equal,
            ">=" => cmp_version(version, want) != std::cmp::Ordering::Less,
            _ => true,
        },
    }
}

/// A crude pre-release check: a version containing a/b/rc/dev is a pre-release.
fn is_prerelease(v: &str) -> bool {
    let l = v.to_ascii_lowercase();
    l.contains('a') || l.contains('b') || l.contains("rc") || l.contains("dev")
}

/// Compare two PEP 440-ish versions numerically field-by-field (`1.2.10` > `1.2.9`).
/// Non-numeric fields fall back to a lexical compare of the whole string.
fn cmp_version(a: &str, b: &str) -> std::cmp::Ordering {
    let pa: Option<Vec<u64>> = a.split('.').map(|p| p.parse::<u64>().ok()).collect();
    let pb: Option<Vec<u64>> = b.split('.').map(|p| p.parse::<u64>().ok()).collect();
    match (pa, pb) {
        (Some(va), Some(vb)) => {
            let n = va.len().max(vb.len());
            for i in 0..n {
                let x = va.get(i).copied().unwrap_or(0);
                let y = vb.get(i).copied().unwrap_or(0);
                match x.cmp(&y) {
                    std::cmp::Ordering::Equal => continue,
                    o => return o,
                }
            }
            std::cmp::Ordering::Equal
        }
        _ => a.cmp(b),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_block_no_deps() {
        assert_eq!(parse_dependencies("print('hi')\n").unwrap(), vec![]);
    }

    #[test]
    fn parses_pep723_block() {
        let src = "\
# /// script
# dependencies = [\"six\", \"packaging>=21.0\"]
# ///
import six
";
        let reqs = parse_dependencies(src).unwrap();
        assert_eq!(reqs.len(), 2);
        assert_eq!(reqs[0], Requirement { name: "six".into(), bound: None });
        assert_eq!(
            reqs[1],
            Requirement { name: "packaging".into(), bound: Some((">=".into(), "21.0".into())) }
        );
    }

    #[test]
    fn parses_exact_bound_and_normalizes_name() {
        let src = "\
# /// script
# dependencies = [\"My_Pkg.Name==1.2.3\"]
# ///
";
        let reqs = parse_dependencies(src).unwrap();
        assert_eq!(reqs[0].name, "my-pkg-name");
        assert_eq!(reqs[0].bound, Some(("==".into(), "1.2.3".into())));
    }

    #[test]
    fn rejects_two_blocks() {
        let src = "\
# /// script
# dependencies = [\"a\"]
# ///
# /// script
# dependencies = [\"b\"]
# ///
";
        assert!(parse_dependencies(src).is_err());
    }

    #[test]
    fn pure_python_wheel_detection() {
        assert!(is_pure_python_wheel("six-1.16.0-py2.py3-none-any.whl"));
        assert!(is_pure_python_wheel("packaging-23.2-py3-none-any.whl"));
        assert!(!is_pure_python_wheel(
            "numpy-1.26.0-cp312-cp312-manylinux_2_17_x86_64.whl"
        ));
        assert!(!is_pure_python_wheel(
            "cryptography-42.0-cp39-abi3-macosx_10_12_x86_64.whl"
        ));
        assert!(!is_pure_python_wheel("six-1.16.0.tar.gz"));
    }

    #[test]
    fn version_ordering() {
        assert_eq!(cmp_version("1.2.10", "1.2.9"), std::cmp::Ordering::Greater);
        assert_eq!(cmp_version("2.0", "1.9.9"), std::cmp::Ordering::Greater);
        assert_eq!(cmp_version("1.0.0", "1.0.0"), std::cmp::Ordering::Equal);
    }

    #[test]
    fn version_bounds() {
        assert!(version_satisfies("1.16.0", None));
        assert!(version_satisfies("1.16.0", Some(&("==".into(), "1.16.0".into()))));
        assert!(!version_satisfies("1.15.0", Some(&("==".into(), "1.16.0".into()))));
        assert!(version_satisfies("2.0.0", Some(&(">=".into(), "1.0".into()))));
        assert!(!version_satisfies("0.9.0", Some(&(">=".into(), "1.0".into()))));
        // Pre-releases are skipped.
        assert!(!version_satisfies("1.0.0rc1", None));
    }
}

/// Live PyPI proofs for the host-side resolve/stage pipeline. `#[ignore]`d
/// because they hit the network; run with:
///   `cargo test --no-default-features --features bundled -- --ignored live_`
#[cfg(test)]
mod live_tests {
    use super::*;

    #[test]
    #[ignore = "hits real PyPI"]
    fn live_stage_six_from_pypi() {
        // Proves the whole host-side Part-1 chain: resolve `six` on PyPI, detect
        // its pure-Python wheel, download, and unzip into a site-packages dir.
        let tmp = std::env::temp_dir().join(format!("dl-pydeps-live-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let reqs = vec![Requirement { name: "six".into(), bound: None }];
        let staged = stage_dependencies(&reqs, &tmp).expect("stage six");
        assert_eq!(staged.len(), 1);
        assert_eq!(staged[0].0, "six");
        // `six` ships a single top-level module `six.py`.
        assert!(
            tmp.join("six.py").is_file(),
            "six.py must be unzipped into site-packages"
        );
        let has_distinfo = std::fs::read_dir(&tmp)
            .unwrap()
            .any(|e| e.unwrap().file_name().to_string_lossy().contains("six"));
        assert!(has_distinfo);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    #[ignore = "hits real PyPI"]
    fn live_native_dep_is_rejected() {
        // numpy offers only native (C-extension) wheels -> Phase-5 boundary error.
        let reqs = vec![Requirement { name: "numpy".into(), bound: None }];
        let tmp = std::env::temp_dir().join(format!("dl-pydeps-native-{}", std::process::id()));
        let err = stage_dependencies(&reqs, &tmp).expect_err("numpy must be rejected");
        assert!(
            err.contains("NATIVE") || err.contains("Phase-5"),
            "error must name the native/Phase-5 boundary: {err}"
        );
    }
}
