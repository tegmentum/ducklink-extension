# DuckDB upstream issue: `custom_trusted_extension_keys`

Draft for submission to https://github.com/duckdb/duckdb/discussions or /issues. Body below is ready to paste into a discussion post.

---

## Title

Proposal: `custom_trusted_extension_keys` — allow users to trust specific third-party extension signing keys

## Summary

DuckDB currently ships two built-in trusted extension signing keys (the DuckDB team key and the community-extensions key). Any extension signed with a different key must be loaded via `SET allow_unsigned_extensions = true;`, which trusts *any* unsigned extension for the rest of the database's lifetime.

This is a coarse choice that leaves third-party extension distributors two options: get into duckdb/community-extensions (governance bottleneck; not every project is a fit), or ask users to disable signature checking entirely.

Propose adding a `custom_trusted_extension_keys` setting that lets a user (or an installer they've already trusted) pin a *specific* public key. Extensions signed with that key load normally; unsigned extensions still don't.

This is strictly *more* granular than `allow_unsigned_extensions`, and the security posture is comparable to (arguably better than) the current implicit trust in the community-extensions maintainer set.

## Motivation — concrete use case

We (tegmentum) run [ducklink](https://github.com/tegmentum/ducklink-extension), a DuckDB extension that dynamically loads portable WebAssembly extension modules. For workloads where the WASM sandbox overhead matters (25-40× vs native DuckDB), we want to publish native `.duckdb_extension` builds of specific components (validators, encoders, etc.) that share source with the WASM versions.

Publishing each as a separate community extension isn't the right shape — the reason our catalog exists is to avoid the community-extensions governance funnel for hundreds of small capabilities. Serving native binaries from our own infrastructure works, but only if users flip `allow_unsigned_extensions=true` — which is a real security-posture change that most users don't want to make just to load a bank-routing-number validator.

With `custom_trusted_extension_keys`, users would `SET custom_trusted_extension_keys = '<tegmentum public key PEM>';` once (in a config file, environment variable, or startup script) and thereafter load any tegmentum-signed extension without touching the unsigned flag. Unsigned extensions from *other* sources would still fail signature check.

This isn't ducklink-specific. Any organization publishing signed DuckDB extensions today faces the same choice, and the answer is always the same suboptimal one.

## Proposed shape

New global setting `custom_trusted_extension_keys`:

- Type: `VARCHAR` (semicolon-separated list of PEM-encoded public keys, or a single PEM).
- Scope: `GLOBAL_ONLY` (same as `allow_community_extensions`).
- Default: empty string.

Extensions signed with any of the configured keys load without `allow_unsigned_extensions`. The core DuckDB and community keys remain trusted as they are today.

Nice-to-have follow-ups (out of scope for the initial issue):

- `TRUST EXTENSION KEY '<pem>'` DDL sugar mirroring `SET custom_trusted_extension_keys` for one-key-at-a-time addition.
- Env var (`DUCKDB_TRUSTED_EXTENSION_KEYS`) so container / CLI deployments can configure without a `SET`.

## Security discussion

**Q: Isn't this weaker than the current model?**

A: No — it's stronger than `allow_unsigned_extensions` (which is the current alternative), and comparable to `allow_community_extensions=true` (which is on by default and trusts every community-extensions author).

Users who run `INSTALL foo FROM community` today implicitly trust every author community-extensions has vetted. A pinned custom key is a *narrower* trust statement: "I trust this one specific author's extensions." That's better security posture than the community model, not worse.

**Q: What if a malicious extension author tricks users into pinning their key?**

A: Same social-engineering problem exists for the community trust model. Users have to decide what to trust based on the reputation of the source (author, distribution URL, community). The mechanism doesn't change the trust decision; it makes the trust decision explicit and granular.

**Q: Should keys be verifiable against a chain of trust?**

A: For the initial version, no — raw public keys, same as the built-in ones. If PKI-style chains become useful later, the setting could accept certificates. Keeping the initial shape simple avoids re-litigating cryptography choices during PR review.

## Implementation sketch

Small — 2 files, ~60-100 lines of real change plus tests:

- `src/main/extension/extension_helper.cpp::GetPublicKeys()` currently hard-codes `community_public_keys[]` and gates on `allow_community_extensions`. Add a parallel block that reads `db.config.options.custom_trusted_extension_keys`, parses the semicolon-separated PEM values, and appends to the returned key vector.
- `src/main/settings/*` + `src/include/duckdb/main/settings.hpp` — register the setting.
- Existing signature-verification path (OpenSSL, `EVP_DigestVerify*`) is agnostic to key source; just extending the key pool is sufficient. No change to verification logic.

Tests:

- Add a fixture extension signed with a test key. Load with the trust set → succeed. Load without → fail with the existing "invalid signature" error.
- Confirm that setting `custom_trusted_extension_keys = ''` (default) leaves current behaviour completely unchanged.
- Confirm interaction with `allow_unsigned_extensions=true` (unsigned still loads regardless, expected).

Happy to prototype this in a fork and open a PR once there's directional buy-in.

## What we're asking

Before we invest in a PR:

1. Is the proposed direction agreeable to the DuckDB team?
2. Any preference on shape — single-value `VARCHAR` PEM, list, or a keys-file path?
3. Any preference on setting name?
4. Prefer we file this as a formal issue and follow the RFC/PR flow, or continue the design discussion here first?

Happy to iterate. Cross-links to prior discussion or existing issues would be useful if this has been considered before.

---

_Filed by: tegmentum (ducklink authors), 2026-07-09._
