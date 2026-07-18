# Native `.duckdb_extension` hosting plan

**Status:** design. Nothing is deployed. This plan describes how to extend the
already-working `ext.ducklink.dev` host to serve native `.duckdb_extension`
binaries, mirroring the WASM path the loader already fetches from.

**Load flow this plan serves.** `src/catalog.rs` (the loader) already knows how
to resolve a catalog entry to a native URL and fetch/verify it. Concretely:

```rust
// src/catalog.rs
const NATIVE_BLOB_BASE: &str = "https://ext.ducklink.dev/native/sha256";
// URL construction (line ~769):
let url = format!("{NATIVE_BLOB_BASE}/{digest}/{platform}/{name}.duckdb_extension");
```

A single missing piece: bytes need to be sitting at those URLs. Everything else
— resolver, cache layout, sha256 verify, event trace — is already merged and
exercised by `LOAD NATIVE 'name'`.

Related design docs:
- `docs/dual-build-native-and-wasm.md` — why we're publishing native at all.
- `docs/catalog-authoring.md` — the catalog schema whose `providers[]` names the
  blob's digest / platform / duckdb version.

---

## 1. Current hosting (what's already up)

`ext.ducklink.dev` is a **Cloudflare-fronted R2 custom domain** in front of the
R2 bucket `datalink-ext` (Cloudflare account `a633389b157fd8a9ec3d3a27cd375643`,
per `~/git/ducklink/deploy/r2/r2.config.json`). Verified live:

```
$ curl -sI https://ext.ducklink.dev/catalog.json
HTTP/2 200
server: cloudflare
content-type: application/json
cache-control: public, max-age=60, must-revalidate

$ curl -sI https://ext.ducklink.dev/wasm/sha256/<digest>/aba.wasm
HTTP/2 200
server: cloudflare
content-type: application/wasm
cache-control: public, max-age=31536000, immutable
```

The bucket is also exposed at `datalink-ext.tegmentum.ai` (a second Cloudflare
custom domain on the same R2 bucket). Both hostnames map to the same objects.

Layout in the bucket today (from `r2.config.json` `layout`):

```
wasm/sha256/<digest>/<name>.wasm          # WASM blobs, digest-keyed, immutable
ducklink/catalog.json                      # gen-4 catalog (short cache)
get/{plugin,standalone,browser}/...        # get.ducklink.dev download artifacts
```

Upload path already in the repo:

- `~/git/ducklink/deploy/r2/publish-artifacts.sh` — uploads via
  `aws s3api put-object --endpoint-url https://<acct>.r2.cloudflarestorage.com`,
  reads S3-compatible R2 credentials from `~/git/datalink/r2.env` or CI secrets.
- `~/git/ducklink/deploy/r2/apply-cors.sh` + `cors.json` — bucket-level CORS.

Nothing about the current setup precludes native — we just haven't uploaded any
objects under `native/` yet.

---

## 2. Proposed native extension: directory + object model

### 2.1 Object key

Mirror the WASM key exactly, one path segment deeper for `<platform>`:

```
native/sha256/<digest>/<platform>/<name>.duckdb_extension
```

- `<digest>` — hex sha256 of the raw `.duckdb_extension` bytes. Matches the
  `content_digest` on the `providers[]` entry in the catalog.
- `<platform>` — DuckDB's convention: `osx_arm64`, `osx_amd64`, `linux_amd64`,
  `linux_arm64`, `windows_amd64`.
- `<name>` — the catalog entry name (e.g. `aba`).

Public URL: `https://ext.ducklink.dev/native/sha256/<digest>/<platform>/<name>.duckdb_extension`.

**Why digest first, then platform** (matches the loader, not the doc-comment).
`src/catalog.rs` line 769 constructs `.../<digest>/<platform>/<name>...`. The
docstring at line 28-29 above the constant (`<platform>/<duckdb_version>/<digest>/...`)
is stale — the URL builder was updated but that comment was not. Either the
comment gets fixed or this doc formalises the current runtime behaviour; either
way, the layout above is what the loader actually fetches. **Human decision:**
fix the stale docstring, then keep the digest-first layout that's already shipping.

**Why no `<duckdb_version>` segment.** A native `.duckdb_extension` is bound to
a specific DuckDB version, but the digest is a function of the compiled bytes,
which already differ per DuckDB version. Two builds of the same source against
DuckDB v1.5.4 and v1.6.0 produce two different digests and therefore two
different object keys. No collision possible; no need for a version segment.
The `duckdb_version` still lives on the catalog `providers[]` entry — that's
where the selector reads it (`select_native_provider`).

### 2.2 Object headers

Set on upload (identical to WASM cache policy except content-type):

| Header | Value | Rationale |
|---|---|---|
| `content-type` | `application/octet-stream` | Native `.duckdb_extension` has no registered IANA type. DuckDB's own extension repo uses this for `.duckdb_extension.gz` and the same for uncompressed. Browsers won't try to render it; DuckDB reads it as bytes. |
| `cache-control` | `public, max-age=31536000, immutable` | Digest-keyed URL is immutable by construction. Same policy as WASM blobs today. |
| `content-encoding` | *(omit)* | We serve raw bytes; see 2.3. |

### 2.3 Compression: raw, not gzipped (recommendation)

**Recommendation: publish raw `.duckdb_extension` bytes, not `.gz`, for the
`LOAD NATIVE` path.** Reasoning:

1. The loader (`src/catalog.rs::download_native_blob`) verifies the downloaded
   bytes' sha256 against the catalog's `content_digest`. That digest is over
   the raw file. Serving gzipped bytes with `content-encoding: gzip` would
   work through reqwest (auto-decompression) but adds a foot-gun: any middlebox
   that strips the `content-encoding` header, or a fetch client that doesn't
   auto-decompress (browsers with `fetch({ decompress: false })`, WASM hosts
   using bare XHR), would see the compressed bytes and the digest check would
   fail.
2. Cloudflare's edge already does on-the-fly Brotli/gzip for
   `content-type: application/octet-stream` when the client sends
   `accept-encoding`. Wire savings without changing what's stored.
3. Native binaries are 400 KB - 2 MB. Storage is cents/year regardless.

The DuckDB core `INSTALL <name>` flow gzips + appends a 256-byte signature
footer. **We are not on that flow.** `LOAD NATIVE 'name'` is ducklink's own
resolver — no signature footer, no `.gz` suffix, raw bytes only. If a native
build is *also* published through `duckdb/community-extensions` (per
`docs/dual-build-native-and-wasm.md`) the community-extensions CI handles the
gzip/signature path for its own S3 bucket — that pipeline is out of scope here.

### 2.4 CORS

The existing CORS rule (`~/git/ducklink/deploy/r2/cors.json`) is bucket-wide
and already permits `GET`/`HEAD` with `range`/`if-match`/`if-none-match` from
the browser demo origin. Native binaries only ever get fetched by the ducklink
native host (a CLI, not a browser) — the browser demo path stays WASM. **No
CORS change is required** for the MVP.

If a future in-browser feature ever needs to expose native URLs (unlikely — a
browser can't load a `.duckdb_extension`), the existing `apply-cors.sh` already
covers the whole bucket. New paths inherit the same rule.

### 2.5 Access controls

Public read, no auth. Extensions are catalog-listed by name + digest anyway —
enumeration is not a threat, and the digest in the URL is exactly what the
loader verifies. Same posture as the WASM blobs today.

R2 write access is scoped to the `R2_ACCESS_KEY_ID`/`R2_SECRET_ACCESS_KEY`
credentials in `~/git/datalink/r2.env` (local) and the CI org secrets of the
same name (CI). Adding native does not change the write posture.

---

## 3. Upload procedure

### 3.1 MVP: extend the existing manual publish script

The lowest-friction path is to add a native block to
`~/git/ducklink/deploy/r2/publish-artifacts.sh`. That script already:

- reads R2 creds from CI secrets or `r2.env`,
- targets the `datalink-ext` bucket via `aws s3api put-object`,
- sets `cache-control: public, max-age=31536000, immutable` by default,
- knows how to detect `PLATFORM` (`osx_arm64`, etc.) from `uname`,
- writes to the same bucket that `ext.ducklink.dev` fronts.

**Proposed addition** (concept only, not a diff to land here). New env-driven
block, disabled by default, that mirrors the existing `plugin:` block but keys
by digest and skips the community-extensions naming convention:

```bash
# --- native (ducklink_load(kind => 'native')) -------------------------------
# NATIVE_FILES="aba=/path/to/aba.duckdb_extension creditcard=/path/to/..."
# NATIVE_PLATFORM="linux_amd64"   (default: detect)
for pair in $NATIVE_FILES; do
  name="${pair%%=*}"; file="${pair#*=}"
  digest="$(sha256_of "$file")"
  k="native/sha256/${digest}/${NATIVE_PLATFORM}/${name}.duckdb_extension"
  upload "$file" "$k" application/octet-stream
done
```

Local, one-time-per-platform invocation:

```bash
NATIVE_FILES="aba=$HOME/git/ducklink-corpus/target/release/libaba.duckdb_extension" \
  ~/git/ducklink/deploy/r2/publish-artifacts.sh
```

That is the recommended shape for **v1**: manual, opt-in, uses the same
credential + endpoint plumbing as the plugin/standalone/browser uploads that
already run today. There's no CI job yet, no signing pipeline, no matrix build
— the flagship-scope of the dual-build plan (1-2 native extensions, per
`docs/dual-build-native-and-wasm.md`) does not justify one yet.

### 3.2 v2 (when there's a matrix): a GitHub Actions job

Once there are 2+ native extensions × 5 platforms and rebuilds happen per
DuckDB release, wrap this in an Actions workflow:

- Matrix on `{platform, duckdb_version}` builds `libaba.duckdb_extension`.
- Job uploads artifact + updates the catalog's `providers[]` entry with the
  new digest.
- Same R2 credentials as the existing publish workflow (org secrets).

Not needed for the first native extension. Explicitly deferred.

### 3.3 Uploading the catalog update

Every native provider needs a matching `providers[]` entry in
`ext.ducklink.dev/catalog.json` (see `docs/catalog-authoring.md` for the
schema). The catalog build lives at `~/git/ducklink/tooling/gen-catalog.py`;
its output is uploaded by `publish-artifacts.sh` (`ducklink/catalog.json`
key). This is unchanged from the WASM flow — a native provider is just a new
`{kind:"native", platform, duckdb_version, content_digest}` entry alongside
the wasm providers.

---

## 4. Cost + scale estimate

R2 pricing (as of writing): storage $0.015/GB-month, class-B reads
$0.36/M, **egress free**. Class-A writes $4.50/M (upload path, not egress).

Assumptions for a realistic MVP year:

| Variable | Value |
|---|---|
| Extensions with a native build | 2 (dual-build doc caps at 1-2 flagship) |
| Platforms per extension | 5 (`osx_arm64`, `osx_amd64`, `linux_amd64`, `linux_arm64`, `windows_amd64`) |
| Versions kept live | 3 (current + 2 prior; digest-keyed so old ones stay resolvable) |
| Avg binary size | 1 MB (typical for a single scalar + tables + arrow deps) |
| Downloads/month | 1,000 (generous — most flagship native extensions in similar ecosystems see fewer) |

**Storage:** 2 × 5 × 3 × 1 MB = **30 MB → $0.00045/month**.

**Reads:** 1,000/mo × $0.36/M = **$0.00036/month**.

**Egress:** $0 on R2.

**Writes:** ~50 uploads/month × $4.50/M = $0.000225.

**Total: under $0.01/month.** At 100× scale (200 native builds, 100k downloads
per month) still under $0.10/month. Native hosting adds *effectively nothing*
to the existing R2 bill.

The scaling limit here is not cost, it's Cloudflare zone request quotas — the
public zone plan the `ducklink.dev` domain sits on already handles the WASM
traffic and will absorb the native traffic in the same envelope.

---

## 5. Explicit non-goals

- **No DuckDB-core `INSTALL` compatibility.** Native artifacts on
  `ext.ducklink.dev` are consumed by `LOAD NATIVE 'name'` (the ducklink
  runtime's own path), not `INSTALL <name>`. If a native build ALSO wants to
  ride the `INSTALL` flow, the vehicle is `duckdb/community-extensions` — a
  separate PR, a separate bucket, a separate build recipe. See
  `docs/dual-build-native-and-wasm.md` §"What ships".
- **No signature footer / signing key.** The 256-byte trailing signature block
  that DuckDB core validates on `LOAD` (`allow_unsigned_extensions=false`) is
  only meaningful for the `INSTALL/LOAD <name>` path. `LOAD NATIVE` verifies
  sha256 against the catalog `content_digest`, so a bit-for-bit tamper is
  already detected without a footer.
- **No new zone / domain.** Everything lives under `ext.ducklink.dev`.

---

## 6. What needs a human decision

Marked so nothing implicit slips through:

1. **Cloud provider — already decided (R2).** No decision to revisit. The
   plan reuses the existing `datalink-ext` R2 bucket and the existing
   `ext.ducklink.dev` custom domain — zero new infra.

2. **Upload trigger — needs a call.** Manual bash invocation of the extended
   `publish-artifacts.sh` is the MVP recommendation (§3.1). If someone wants a
   GitHub Actions matrix from day one, that changes §3 — probably worth it if
   the aba pilot ships on more than one platform on day one, probably not
   worth it if only `osx_arm64` ships first.

3. **Fix the stale docstring in `src/catalog.rs`.** Lines 28-29 say
   `<BASE>/<platform>/<duckdb_version>/<digest>/...` but line 769 constructs
   `<BASE>/<digest>/<platform>/...` — the code is the truth, the comment is
   wrong. Trivial cleanup; noting here so it doesn't get lost. This plan
   assumes the digest-first layout the code emits.

4. **First flagship extension.** The dual-build doc names `aba` as the pilot;
   this plan doesn't second-guess it. Once `aba` publishes for one platform
   using §3.1 the URL contract is fully exercised — everything else is
   fill-in-the-matrix work.

5. **Do we bother compressing on upload?** Recommendation is no (§2.3) —
   Cloudflare's edge does the wire compression, and gzipped-at-rest breaks the
   sha256 pre-verify assumption for any client that doesn't auto-decompress.
   If someone wants `-encoding: gzip` for storage-cost reasons (irrelevant at
   this scale), that reverses the recommendation. Default answer: no.

No other decisions are open. Everything else is implied by the existing WASM
setup, the loader code in `src/catalog.rs`, and the R2 config in
`~/git/ducklink/deploy/r2/`.

---

## 7. Acceptance checklist

For anyone landing this later, "native hosting works" means all of:

- [ ] `curl -sI https://ext.ducklink.dev/native/sha256/<digest>/<platform>/aba.duckdb_extension`
      returns `HTTP/2 200` with `content-type: application/octet-stream`
      and `cache-control: public, max-age=31536000, immutable`.
- [ ] `curl -s .../aba.duckdb_extension | shasum -a 256` matches `<digest>`.
- [ ] `ext.ducklink.dev/catalog.json` `aba` entry carries a
      `providers[]` member with `{kind:"native", platform, duckdb_version, content_digest:<digest>}`.
- [ ] Running `ducklink_load_native('aba')` (or the equivalent `LOAD NATIVE`
      surface) on a machine matching `<platform>` + `<duckdb_version>` fetches
      + sha256-verifies + caches at
      `~/.cache/ducklink/native/sha256/<digest>/aba.duckdb_extension`.
- [ ] The offline fallback (`tests/live_catalog_smoke.rs::offline_falls_back_to_bundled_snapshot`)
      still passes — the bundled snapshot need not include native providers
      for entries whose flagship path is still WASM.
