# Community-native provider audit

_Audit date: 2026-07-13_

_Inputs: `assets/catalog-snapshot.json` (200 ducklink extensions) vs.
`duckdb/community-extensions` (281 published extensions, as returned by
`gh api /repos/duckdb/community-extensions/contents/extensions`)._

## Exec summary

**Only 3 ducklink modules have a viable `community-native` provider today: `h3`,
`jsonata`, and `markdown`.** In each case every SQL name that ducklink advertises
in `exports[]` is registered by the community extension of the same name with a
compatible signature (or an implicit VARCHAR alias). Everything else is a mine
field.

The audit was scoped to the **30 ducklink modules whose name is identical to a
published community extension** — the highest-signal candidate set. Cross-module
matches (a ducklink module whose exports happen to line up with a
different-named community extension) were not exhaustively enumerated; a
targeted keyword pass over the community extension list did not surface any
plausible additional candidates for ducklink's non-name-matched modules (`isin`,
`iban`, `luhn`, `baseN`, `aba`, etc.), whose exports use financial-identifier
naming that no community extension mirrors. Time budget: this pass consumed
roughly half the allotted hour; the remaining 170 ducklink modules × 251
non-name-matched community extensions were not cross-checked and are not
expected to yield hits, but the search is not exhaustive.

The dominant failure mode across the 27 non-`certain` matches is **silent
prefix drift**: ducklink modules very often expose bare function names (e.g.
`sma`, `sha1`, `plot_sparkline`, `jinja_render`, `fuzz_ratio`, `normal_cdf`)
where the community publisher scoped their names with an extension prefix
(`t_sma`, `crypto_hash('sha1', …)`, `tp_sparkline`, `minijinja_render`,
`rapidfuzz_partial_ratio`, `dist_normal_cdf`). Auto-promoting any of these to
`community-native` would silently break user queries the moment ducklink
switched over.

Counts: **certain 3, partial 8, possible 19, none 0** (out of the 30
same-name matches). The 170 ducklink modules whose name does not appear in
`community-extensions` were not individually cross-referenced against every
community extension and are treated as `none` by default in this pass.

## Overlap table (30 same-name candidates)

| ducklink module | community extension (repo) | ducklink `exports[]` | community-side function names (exact matches only) | confidence |
| --- | --- | --- | --- | --- |
| `a5` | `a5` (query-farm/a5) | `a5_lonlat_to_cell`, `a5_cell_to_lat`, `a5_cell_to_lon`, `a5_cell_to_resolution`, `a5_cell_to_parent`, `a5_is_valid_cell`, `a5_cell_to_hex`, `a5_hex_to_cell` | `a5_lonlat_to_cell`, `a5_cell_to_parent`, `a5_is_valid_cell` (3 of 8) | partial |
| `bitfilters` | `bitfilters` (query-farm/bitfilters) | `xor_filter`, `xor_filter_contains` | — (community only registers width-suffixed `xor8_filter`, `xor16_filter`, `xor8_filter_contains`, `xor16_filter_contains`) | possible |
| `celestial` | `celestial` (lisa-sgs/duckdb-celestial) | `equatorial_to_galactic_l`, `equatorial_to_galactic_b`, `angular_separation`, `hms_to_deg`, `dms_to_deg` | — (community registers `spherical_angle`, `angular_separation_rad`, `angular_separation_deg`) | possible |
| `crypto` | `crypto` (query-farm/crypto) | `sha1`, `sha512`, `sha3_256`, `blake3`, `crc32` | — (community exposes generic `crypto_hash(algorithm, value)`, `crypto_hash_agg`, `crypto_hmac` — algorithms are a runtime argument, not distinct function names) | possible |
| `dns` | `dns` (tobilg/duckdb-dns) | `dns_lookup`, `dns_resolve_all` | `dns_lookup` (1 of 2; community's second-form is `dns_lookup_all`, not `dns_resolve_all`) | partial |
| `dplyr` | `dplyr` (mrchypark/libdplyr) | `dplyr` | — (community registers `dplyr_query`) | possible |
| `fit` | `fit` (antoriche/duckdb-fit-extension) | `read_fit` | — (community registers table functions `fit_records`, `fit_activities`, `fit_sessions` and scalar `fit_openssl_version`) | possible |
| `ggsql` | `ggsql` (posit-dev/ggsql-duckdb) | `VISUALIZE` | `VISUALIZE` / `VISUALISE` keyword handled by both (see semantic-divergence note below) | possible |
| `h3` | `h3` (isaacbrodsky/h3-duckdb) | `h3_latlng_to_cell`, `h3_cell_to_lat`, `h3_cell_to_lng`, `h3_cell_to_parent`, `h3_grid_distance`, `h3_is_valid_cell` | all 6 registered by community (verified against `src/h3_indexing.cpp` and neighbours) | **certain** |
| `hashfuncs` | `hashfuncs` (query-farm/hashfuncs) | `xxh32`, `xxh64`, `xxh3`, `murmur3` | `xxh32`, `xxh64` (2 of 4; community has `xxh3_64` / `xxh3_128` / `xxh3_128_hex` — no bare `xxh3` — and no `murmur3`) | partial |
| `ion` | `ion` (kestra-io/duckdb-ion) | `ion_to_json`, `ion_from_json`, `ion_get` | — (community registers scalar `to_ion`, table `read_ion`, copy target `ion_binary`) | possible |
| `json_schema` | `json_schema` (query-farm/json_schema) | `json_schema_valid`, `json_schema_errors` | — (community registers `json_schema_validate`, `json_schema_validate_schema`, `json_schema_patch`, `json_schema_update`) | possible |
| `jsonata` | `jsonata` (query-farm/jsonata) | `jsonata` | `jsonata` (`ScalarFunctionSet jsonata_function_set("jsonata")`, VARCHAR/JSON overload — VARCHAR arg is implicitly castable) | **certain** |
| `jwt` | `jwt` (GalvinGao/duckdb_jwt) | `jwt_header`, `jwt_payload` | — (community only registers `jwt_decode_payload`) | possible |
| `lindel` | `lindel` (query-farm/lindel) | `morton_encode`, `morton_decode_x`, `morton_decode_y`, `hilbert_encode`, `hilbert_decode_x`, `hilbert_decode_y` | `morton_encode`, `hilbert_encode` (2 of 6; community's decode returns a whole array via `morton_decode` / `hilbert_decode`, ducklink split it into `_x` / `_y`) | partial |
| `magic` | `magic` (carlopi/duckdb-magic) | `magic_mime`, `magic_extension`, `magic_matcher_type`, `is_image` | `magic_mime` (1 of 4; community's other names are `magic_type`, `magic_required_extensions`, `magic_archive_members`, `magic_capabilities`) | partial |
| `marisa` | `marisa` (query-farm/marisa) | `fst_contains`, `fst_prefix`, `fst_count` | — (community registers `marisa_lookup`, `marisa_common_prefix`, `marisa_predictive`, aggregate `marisa_trie` — ducklink uses generic `fst_*` names) | possible |
| `markdown` | `markdown` (teaguesterling/duckdb_markdown) | `md_to_html`, `md_to_text` | both registered by community (`markdown_scalar_functions.cpp`). Community param is the `markdown` VARCHAR alias with `RegisterCastFunction(VARCHAR, markdown_type, 0)` (implicit cost 0), so a caller passing plain VARCHAR still binds. | **certain** |
| `minijinja` | `minijinja` (query-farm/minijinja) | `jinja_render`, `jinja_valid` | — (community registers `minijinja_render`, `minijinja_render_with_context` — `minijinja_` prefix vs `jinja_`) | possible |
| `netquack` | `netquack` (hatamiarash7/duckdb-netquack) | `registrable_domain`, `public_suffix`, `subdomain`, `domain_label` | — (community registers `extract_domain`, `extract_subdomain`, `extract_tld`, `extract_path`, … — different verb + noun ordering) | possible |
| `prql` | `prql` (ywelsch/duckdb-prql) | `prql_to_sql`, `prql_is_valid` | — (community is a `ParserExtension` that intercepts full PRQL queries; it does **not** expose any scalar functions) | possible |
| `rapidfuzz` | `rapidfuzz` (query-farm/rapidfuzz) | `fuzz_ratio`, `damerau_levenshtein`, `indel`, `osa` | — (community registers `rapidfuzz_partial_ratio`, `rapidfuzz_token_set_ratio`, `rapidfuzz_token_sort_ratio`, `rapidfuzz_partial_token_set_ratio` — `rapidfuzz_` prefix) | possible |
| `stochastic` | `stochastic` (query-farm/stochastic) | `normal_cdf`, `normal_pdf`, `normal_quantile`, `binomial_pmf`, `poisson_pmf`, `exponential_cdf`, `beta_cdf` | — (community's `RegisterFunction<>` helper composes names as `dist_<distribution>_<op>`, e.g. `dist_normal_cdf`, `dist_beta_cdf` — the `dist_` prefix is mandatory) | possible |
| `talib` | `talib` (neuesql/atm_talib) | `sma`, `ema`, `rsi` | — (community's `TALIB_SCALAR_*` macros register `t_sma`, `t_ema`, `t_rsi` — mandatory `t_` prefix) | possible |
| `tera` | `tera` (query-farm/tera) | `tera_render`, `tera_valid` | `tera_render` (1 of 2; community does not register `tera_valid`) | partial |
| `textplot` | `textplot` (query-farm/textplot) | `plot_sparkline`, `plot_bars`, `qr_utf8` | — (community registers `tp_sparkline`, `tp_bar`, `tp_qr`, `tp_density` — `tp_` prefix, and `qr_utf8` has no community counterpart) | possible |
| `tsid` | `tsid` (quackscience/duckdb-extension-tsid) | `tsid_encode`, `tsid_decode`, `tsid_timestamp`, `tsid_from_timestamp` | — (community registers `tsid()` generator and `tsid_to_timestamp()` — different verbs) | possible |
| `urlpattern` | `urlpattern` (teaguesterling/duckdb_urlpattern) | `url_pattern_test`, `url_pattern_match` | — (community registers `urlpattern_test`, `urlpattern_extract`, `urlpattern_pathname`, … — no underscore between `url` and `pattern`) | possible |
| `warc` | `warc` (midwork-finds-jobs/duckdb_warc) | `read_warc` | — (community registers scalar `parse_warc`, ducklink expects table `read_warc`) | possible |
| `yaml` | `yaml` (teaguesterling/duckdb_yaml) | `yaml_to_json`, `json_to_yaml` | `yaml_to_json` (1 of 2; community's inverse direction is `from_yaml` / `to_yaml`, not `json_to_yaml`) | partial |

## Proposed catalog patches (certain matches only)

Each block below shows the current `providers[]` for a ducklink entry followed
by the proposed new value. Only the additive change is shown — no other fields
in the entry should move. The provider shape (`id`, `kind: "community-native"`,
`extension_name`) matches `docs/catalog-authoring.md` § 3b.

### `h3`

Current:

```json
[
  {
    "id": "wasm-component",
    "kind": "wasm",
    "reference": true,
    "abi": "duckdb:extension@2.2.0",
    "artifact": "artifacts/extensions/h3.wasm",
    "content_digest": "b1c79dd5c00dc0cea0ad2f34c20d7d77180a3d0a3967e68edc6d83cfb6a6957d",
    "conformance": {
      "suite": "h3@4",
      "suite_digest": "bdac7a0ab7cc22fe084b3064a14a558c6ad77ed08691726f94b24fc126fb63dc",
      "at": "a2ad9764ac971345d6a650b92edbda034b160980acf148d354126f7e6f92ba40",
      "passed": true
    },
    "status": "supported"
  }
]
```

Proposed (append one entry):

```json
[
  {
    "id": "wasm-component",
    "kind": "wasm",
    "reference": true,
    "abi": "duckdb:extension@2.2.0",
    "artifact": "artifacts/extensions/h3.wasm",
    "content_digest": "b1c79dd5c00dc0cea0ad2f34c20d7d77180a3d0a3967e68edc6d83cfb6a6957d",
    "conformance": {
      "suite": "h3@4",
      "suite_digest": "bdac7a0ab7cc22fe084b3064a14a558c6ad77ed08691726f94b24fc126fb63dc",
      "at": "a2ad9764ac971345d6a650b92edbda034b160980acf148d354126f7e6f92ba40",
      "passed": true
    },
    "status": "supported"
  },
  {
    "id": "cn-official",
    "kind": "community-native",
    "extension_name": "h3"
  }
]
```

### `jsonata`

Current: `providers: null` (no providers array yet — the entry only advertises a
WASM `artifact`).

Proposed (create the array):

```json
[
  {
    "id": "cn-official",
    "kind": "community-native",
    "extension_name": "jsonata"
  }
]
```

Note: the ducklink `jsonata` entry currently lacks the `wasm-component` provider
its peers carry. Whoever applies this patch should also add the corresponding
WASM provider (mirroring e.g. the `markdown` entry) so the community-native
route is an alternative, not the only route. If the intent is
community-native-only, this block is complete as-is.

### `markdown`

Current: `providers: null` (same situation as `jsonata`).

Proposed:

```json
[
  {
    "id": "cn-official",
    "kind": "community-native",
    "extension_name": "markdown"
  }
]
```

Same note as `jsonata`: add a matching `wasm-component` provider if the WASM
path is intended to remain available alongside the community-native path.

## Not adding, but flagged

These are the 27 same-name matches that did **not** clear the `certain` bar.
Every one of them would silently break user queries if promoted, so none are
being proposed.

**Partial (8) — some ducklink `exports[]` names are present in the community
extension, some are not.** Adding a `community-native` provider here would make
the covered names work but silently error on the uncovered ones.

- `a5` — 3 of 8 covered. Community collapses lat/lon into `a5_cell_to_lonlat`
  and hex conversion into `a5_u64_to_hex` / `a5_hex_to_u64`; ducklink's split
  scalars have no direct equivalent.
- `dns` — 1 of 2 covered. Community's second-form is `dns_lookup_all`, not
  `dns_resolve_all`.
- `hashfuncs` — 2 of 4 covered. Community's `xxh3` variants are all
  bit-width-suffixed (`xxh3_64`, `xxh3_128`) and there is no `murmur3` at all.
- `lindel` — 2 of 6 covered. Encodes match, but community's `morton_decode` /
  `hilbert_decode` return a whole coordinate array where ducklink split them
  into `_x` / `_y` scalars.
- `magic` — 1 of 4 covered. Community uses `magic_type` /
  `magic_required_extensions` / `magic_archive_members`; ducklink's
  `magic_extension`, `magic_matcher_type`, and `is_image` don't exist.
- `tera` — 1 of 2 covered. Community does not register `tera_valid`.
- `yaml` — 1 of 2 covered. Community's JSON→YAML direction is `to_yaml(json)`,
  not a `json_to_yaml` scalar.

**Possible (19) — module name is a match but no ducklink export is registered
verbatim by the community extension.** These are all-or-nothing prefix / verb
mismatches; forwarding would fail every call.

- `bitfilters` — community requires bit-width suffix (`xor8_filter` /
  `xor16_filter`), ducklink expects bare `xor_filter`.
- `celestial` — community `angular_separation_rad` / `angular_separation_deg`
  vs ducklink `angular_separation`; ducklink's `equatorial_to_galactic_*` and
  `hms_to_deg` / `dms_to_deg` have no counterparts.
- `crypto` — different **interface**, not just naming. Community exposes
  `crypto_hash(algorithm, value)` with the algorithm as a string argument;
  ducklink exposes algorithm-per-function (`sha1(...)`, `blake3(...)`, …).
- `dplyr` — community's function is `dplyr_query(...)`, ducklink calls it
  `dplyr(...)`.
- `fit` — different **shape**: community exposes table functions
  (`fit_records`, `fit_activities`, `fit_sessions`) plus `fit_openssl_version`;
  ducklink advertises a single `read_fit`.
- `ggsql` — surface parity (both parse a `VISUALIZE` statement) but **semantic
  divergence**: community `ggsql` renders vega-lite in an in-process HTTP
  server + browser tab; ducklink's description says it produces a text bar
  chart. The user's query returns fundamentally different output, so silently
  swapping providers is not the transparent routing the community-native
  contract requires.
- `ion` — community exposes `to_ion` scalar + `read_ion` table + `ion_binary`
  copy target; ducklink's `ion_to_json` / `ion_from_json` / `ion_get` are all
  independent, non-overlapping names.
- `json_schema` — off-by-one verb: community is `_validate`, ducklink is
  `_valid`. And ducklink's `json_schema_errors` has no counterpart.
- `jwt` — community only publishes `jwt_decode_payload`; ducklink asks for
  `jwt_header` and `jwt_payload` and their signatures differ.
- `marisa` — ducklink uses generic finite-state-transducer verbs
  (`fst_contains`, `fst_prefix`, `fst_count`) whereas community keeps the
  `marisa_` prefix (`marisa_lookup`, `marisa_common_prefix`,
  `marisa_predictive`).
- `minijinja` — mandatory `minijinja_` prefix in community vs `jinja_` in
  ducklink. Also, community's second function is `minijinja_render_with_context`,
  not `jinja_valid`.
- `netquack` — community verbs are `extract_*` (`extract_domain`,
  `extract_subdomain`, `extract_tld`, …); ducklink uses the noun-first form
  (`registrable_domain`, `public_suffix`, `subdomain`, `domain_label`).
- `prql` — community is a **`ParserExtension` only**: no scalar functions
  registered. Ducklink advertises `prql_to_sql` / `prql_is_valid` scalars,
  which cannot be served by a parser extension.
- `rapidfuzz` — mandatory `rapidfuzz_` prefix in community; ducklink drops it.
  None of ducklink's four exports appear verbatim in community source.
- `stochastic` — community's `RegisterFunction` template composes names as
  `dist_<distribution>_<op>` (e.g. `dist_normal_cdf`). Ducklink drops the
  `dist_` prefix and would fail every lookup.
- `talib` — community's `TALIB_SCALAR_*` macros prepend `t_`
  (`t_sma`, `t_ema`, `t_rsi`); ducklink uses the bare technical-analysis
  names.
- `textplot` — community prefix is `tp_` (`tp_sparkline`, `tp_bar`, `tp_qr`);
  ducklink uses `plot_` (plus a `qr_utf8` that has no community counterpart).
- `tsid` — completely different verbs: community is `tsid()` / 
  `tsid_to_timestamp()`; ducklink is `tsid_encode` / `tsid_decode` /
  `tsid_timestamp` / `tsid_from_timestamp`.
- `urlpattern` — one-character-off prefix drift: community `urlpattern_*` vs
  ducklink `url_pattern_*` (with the extra underscore).
- `warc` — community publishes a scalar `parse_warc`; ducklink advertises a
  table function `read_warc`. Different verb and different shape.

## What was not audited

- **Ducklink modules with no name-matching community extension** (170 of 200).
  A targeted spot-check for `isin`, `iban`, `luhn`, `base32/58`, `aba` and
  similar identifier-validation modules found no plausible cross-module match
  in the community-extensions list. The full cross-product (170 ducklink ×
  251 non-name-matched community extensions) was not enumerated.
- **Function-name matches across module names** (e.g. a ducklink module whose
  export overlaps with a community extension of a completely different name).
  Given ducklink's naming convention of prefixing exports with the module name
  and the community publishers' identical convention, cross-module hits are
  structurally unlikely, but the audit does not rule them out.
- **Signature compatibility beyond name matches.** The `certain` picks were
  spot-checked for argument types (`h3` and `markdown` use compatible or
  implicitly-castable inputs; `jsonata` accepts VARCHAR at bind time via the
  JSON alias). A downstream review before shipping should confirm each
  `certain` extension's signatures still match on the current tip of the
  community repo.
