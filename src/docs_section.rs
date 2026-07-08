//! Read a `duckdb.docs` wasm custom section from a component and parse it into
//! per-function documentation overrides.
//!
//! A component may bundle its own docs in a `duckdb.docs` custom section (UTF-8
//! JSON matching [`ComponentDocs`]) so its summary / description / example /
//! tags stay in sync with source-level doc comments even when the catalog is
//! stale. The load path calls [`parse_docs_from_wasm`] with the component's
//! on-disk `.wasm`; the parsed docs are cached on the loaded-component record
//! and merged into `ducklink.docs` at query time — per-function per-field
//! overrides, tags UNIONed.
//!
//! Non-fatal by construction: any error (missing file, unreadable bytes,
//! malformed wasm envelope, missing section, invalid JSON, wrong shape)
//! returns `None`. A verbose-mode diagnostic is emitted so a curious operator
//! can tell WHY the docs weren't picked up; a component that just doesn't
//! ship a section is silent (the common case).

use std::path::Path;

use serde::Deserialize;

use ducklink_runtime::verbose_log;

/// The parsed contents of a component's `duckdb.docs` custom section: the
/// per-function documentation overrides the component ships with itself.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct ComponentDocs {
    #[serde(default)]
    pub functions: Vec<ComponentDocEntry>,
}

/// One function's documentation overrides. Every field is optional; a field
/// carried by the component wins over the catalog value for the same function.
/// `tags` UNIONs with the catalog's tags rather than replacing them.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct ComponentDocEntry {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub summary: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub example: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
}

impl ComponentDocs {
    /// Look up the doc entry for `function_name`, if the component provided one.
    pub fn get(&self, function_name: &str) -> Option<&ComponentDocEntry> {
        self.functions
            .iter()
            .find(|e| e.name.as_deref() == Some(function_name))
    }
}

/// Read the component's `.wasm` file and, if it carries a `duckdb.docs` wasm
/// custom section, parse the JSON into a [`ComponentDocs`]. Every error path
/// is non-fatal: I/O failure, malformed wasm envelope, missing section,
/// invalid JSON, or wrong shape all return `None`. A verbose-mode diagnostic
/// is emitted for the error paths; a plain "no section present" is silent.
pub fn parse_docs_from_wasm(path: &Path) -> Option<ComponentDocs> {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) => {
            verbose_log!(
                "[ducklink:docs] could not read wasm for docs section from {}: {e}",
                path.display()
            );
            return None;
        }
    };
    parse_docs_from_bytes(&bytes, &path.display().to_string())
}

/// Parse the docs section out of already-loaded wasm bytes. Split out for
/// testability — the unit test drives this directly with a bytes-literal
/// wasm binary carrying a synthetic `duckdb.docs` section.
pub fn parse_docs_from_bytes(bytes: &[u8], source_label: &str) -> Option<ComponentDocs> {
    let section = match find_custom_section(bytes, "duckdb.docs") {
        Ok(Some(data)) => data,
        // A component without the section is the common case: silent.
        Ok(None) => return None,
        Err(e) => {
            verbose_log!(
                "[ducklink:docs] wasm custom-section scan failed for {source_label}: {e}"
            );
            return None;
        }
    };
    match serde_json::from_slice::<ComponentDocs>(section) {
        Ok(docs) => Some(docs),
        Err(e) => {
            verbose_log!(
                "[ducklink:docs] duckdb.docs section for {source_label} was not valid JSON: {e}"
            );
            None
        }
    }
}

/// Scan `bytes` (a wasm module OR component binary) for a custom section
/// whose name equals `target`. Returns `Some(payload_slice)` on a hit, `None`
/// if no such section is present, and `Err(msg)` only if the top-level
/// framing is malformed (which signals the binary itself is broken, not a
/// missing-docs case). For components, a two-pass scan descends once into
/// each nested core-module section, since a Rust guest's
/// `#[link_section = "duckdb.docs"]` typically lands inside the inner core
/// module rather than at the component envelope.
fn find_custom_section<'a>(bytes: &'a [u8], target: &str) -> Result<Option<&'a [u8]>, String> {
    if bytes.len() < 8 {
        return Err("wasm binary shorter than 8-byte header".to_string());
    }
    if &bytes[..4] != b"\0asm" {
        return Err("missing wasm magic (\\0asm)".to_string());
    }
    // Version word: 0x01000000 = core module; 0x0d000100 = component. The
    // section framing (id byte + leb128 size + payload) is identical in both
    // encodings; only the meaning of non-zero ids differs (see the nested
    // core-module recurse below).
    let is_component = matches!(&bytes[4..8], [0x0d, 0x00, 0x01, 0x00]);

    let mut i = 8usize;
    let mut nested_modules: Vec<&[u8]> = Vec::new();
    while i < bytes.len() {
        let id = bytes[i];
        i += 1;
        let (size, size_len) = read_uleb128(&bytes[i..])
            .ok_or_else(|| format!("malformed section-size leb128 at offset {i}"))?;
        i += size_len;
        let section_end = i
            .checked_add(size as usize)
            .ok_or_else(|| "section size overflows".to_string())?;
        if section_end > bytes.len() {
            return Err(format!(
                "section extends past EOF ({section_end} > {})",
                bytes.len()
            ));
        }
        let payload = &bytes[i..section_end];
        if id == 0 {
            // Custom section: <leb128 name_len><name bytes><payload...>.
            let (name_len, name_len_len) = read_uleb128(payload)
                .ok_or_else(|| "malformed custom-section name-length leb128".to_string())?;
            let name_end = name_len_len
                .checked_add(name_len as usize)
                .ok_or_else(|| "custom-section name overflows".to_string())?;
            if name_end > payload.len() {
                return Err("custom-section name extends past section end".to_string());
            }
            let name = &payload[name_len_len..name_end];
            if name == target.as_bytes() {
                return Ok(Some(&payload[name_end..]));
            }
        } else if is_component && id == 1 {
            // Component core-module section: payload is a nested wasm module
            // binary. Save for a second pass so we prefer a match at the
            // component envelope over one inside a nested module.
            nested_modules.push(payload);
        }
        i = section_end;
    }
    // Second pass: descend one level into each nested core module. Tolerant:
    // a nested payload that doesn't start with the wasm magic is skipped
    // rather than erroring the whole scan (defensive against unusual
    // encodings; a truly broken binary would fail component loading anyway).
    for module_bytes in nested_modules {
        if module_bytes.starts_with(b"\0asm") {
            if let Ok(Some(hit)) = find_custom_section(module_bytes, target) {
                return Ok(Some(hit));
            }
        }
    }
    Ok(None)
}

/// Decode one unsigned LEB128-encoded u32 from the start of `bytes`.
/// Returns `Some((value, bytes_consumed))` on success, `None` on truncation
/// or a value wider than u32 (custom-section sizes and name lengths in the
/// wasm binary format are u32).
fn read_uleb128(bytes: &[u8]) -> Option<(u32, usize)> {
    let mut result: u32 = 0;
    let mut shift: u32 = 0;
    for (i, &b) in bytes.iter().enumerate() {
        // A u32 leb128 is at most 5 bytes (5 * 7 = 35 bits of payload space,
        // with only 4 bits used in the 5th byte). Reject longer encodings.
        if i >= 5 {
            return None;
        }
        // On the 5th byte only the low 4 bits are valid (28 + 4 = 32 bits).
        if i == 4 && (b & 0x7F) > 0x0F {
            return None;
        }
        let low = (b & 0x7F) as u32;
        let shifted = low.checked_shl(shift)?;
        result |= shifted;
        if b & 0x80 == 0 {
            return Some((result, i + 1));
        }
        shift += 7;
    }
    // Ran off the end of the input without finding a terminating byte.
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encode a value as unsigned LEB128 for building test wasm binaries.
    fn encode_uleb128(mut value: u32) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let mut b = (value & 0x7F) as u8;
            value >>= 7;
            if value != 0 {
                b |= 0x80;
            }
            out.push(b);
            if value == 0 {
                break;
            }
        }
        out
    }

    /// Wrap `name` + `data` as a wasm custom-section payload (id 0). Section
    /// framing (id byte + leb128 size + payload) is added by `build_module`.
    fn custom_section_body(name: &str, data: &[u8]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend(encode_uleb128(name.len() as u32));
        body.extend(name.as_bytes());
        body.extend(data);
        body
    }

    /// Build a minimal wasm CORE MODULE (magic + version + one custom
    /// section). Enough for `find_custom_section` to parse.
    fn build_module(section_name: &str, section_data: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend(b"\0asm");
        out.extend(&[0x01, 0x00, 0x00, 0x00]);
        let body = custom_section_body(section_name, section_data);
        out.push(0); // custom section id
        out.extend(encode_uleb128(body.len() as u32));
        out.extend(&body);
        out
    }

    /// Build a minimal wasm COMPONENT that WRAPS a nested core module
    /// carrying the custom section. Exercises the "section inside a
    /// component's core-module payload" case (the layout a Rust guest
    /// compiled via cargo-component with `#[link_section]` produces).
    fn build_component_with_nested_module(section_name: &str, section_data: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend(b"\0asm");
        // Component version.
        out.extend(&[0x0d, 0x00, 0x01, 0x00]);
        let nested = build_module(section_name, section_data);
        out.push(1); // core-module section id
        out.extend(encode_uleb128(nested.len() as u32));
        out.extend(&nested);
        out
    }

    #[test]
    fn finds_custom_section_in_a_core_module() {
        let bytes = build_module("duckdb.docs", b"{\"functions\":[]}");
        let hit = find_custom_section(&bytes, "duckdb.docs").expect("scan ok");
        assert_eq!(hit, Some(&b"{\"functions\":[]}"[..]));
    }

    #[test]
    fn finds_custom_section_nested_in_a_component() {
        let bytes =
            build_component_with_nested_module("duckdb.docs", b"{\"functions\":[{\"name\":\"x\"}]}");
        let hit = find_custom_section(&bytes, "duckdb.docs").expect("scan ok");
        assert_eq!(hit, Some(&b"{\"functions\":[{\"name\":\"x\"}]}"[..]));
    }

    #[test]
    fn missing_section_returns_none() {
        let bytes = build_module("something.else", b"payload");
        assert_eq!(find_custom_section(&bytes, "duckdb.docs").unwrap(), None);
    }

    #[test]
    fn missing_magic_is_error() {
        let bytes = vec![0u8; 16];
        assert!(find_custom_section(&bytes, "duckdb.docs").is_err());
    }

    #[test]
    fn parses_docs_json_from_module_bytes() {
        let json = br#"{
            "functions": [
                {
                    "name": "aba_validate",
                    "summary": "Validate ABA routing numbers",
                    "description": "Long form.",
                    "example": "SELECT aba_validate('021000021');",
                    "tags": ["validator", "banking"]
                }
            ]
        }"#;
        let bytes = build_module("duckdb.docs", json);
        let docs = parse_docs_from_bytes(&bytes, "<test>").expect("parsed");
        assert_eq!(docs.functions.len(), 1);
        let e = docs.get("aba_validate").expect("entry");
        assert_eq!(e.summary.as_deref(), Some("Validate ABA routing numbers"));
        assert_eq!(e.example.as_deref(), Some("SELECT aba_validate('021000021');"));
        assert_eq!(e.tags, vec!["validator".to_string(), "banking".to_string()]);
    }

    #[test]
    fn invalid_json_is_non_fatal() {
        // Section present but the body isn't JSON.
        let bytes = build_module("duckdb.docs", b"not json at all");
        assert!(parse_docs_from_bytes(&bytes, "<test>").is_none());
    }

    #[test]
    fn no_section_returns_none_without_error() {
        let bytes = build_module("something.else", b"payload");
        assert!(parse_docs_from_bytes(&bytes, "<test>").is_none());
    }

    #[test]
    fn leb128_round_trip_matches_decoder() {
        for &value in &[0u32, 1, 127, 128, 12345, 1_000_000, u32::MAX] {
            let encoded = encode_uleb128(value);
            let (decoded, len) = read_uleb128(&encoded).expect("decode");
            assert_eq!(decoded, value);
            assert_eq!(len, encoded.len());
        }
    }

    #[test]
    fn leb128_rejects_oversized_input() {
        // 6-byte leb128 encoding (5 bytes with continuation set, then a 6th)
        // must be rejected as it exceeds u32 range.
        let over_long = [0x80, 0x80, 0x80, 0x80, 0x80, 0x01];
        assert!(read_uleb128(&over_long).is_none());
    }
}
