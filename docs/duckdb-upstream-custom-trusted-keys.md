# DuckDB upstream issue: `custom_trusted_extension_keys`

Draft for submission to https://github.com/duckdb/duckdb/discussions or /issues. Body below is ready to paste into a discussion post.

---

## Title

Proposal: `custom_trusted_extension_keys` — minimal opt-in for trusting a specific third-party extension signing key

## The ask, in one sentence

Add a single `VARCHAR` setting that lets an operator pin one or more third-party extension signing public keys, so a signed third-party extension loads without the caller having to flip `allow_unsigned_extensions=true` (which today is the *only* opt-out and disables signature checking for the entire process).

## Relationship to existing work

This proposal deliberately overlaps with the RFC in [#23388 — *Trusted custom extension repositories (per-origin signing keys)*](https://github.com/duckdb/duckdb/discussions/23388) and its draft PR [#23387](https://github.com/duckdb/duckdb/pull/23387). We support the direction of #23388 and think it lands in the right end-state. But that RFC is a substantial piece of work — new `extension_repository` secret type, `.well-known` discovery, `duckdb_register_extension_repo()`, origin-scoped verification, TOFU pinning, ~22 files touched — and the maintainer feedback there notes the trust-surface expansion and complexity as real concerns worth thinking hard about.

This proposal is deliberately the **minimum-viable subset** of that idea: no secrets, no discovery, no origin scoping, no changes to `.info` sidecars. Just extending the pool of trusted keys that `GetPublicKeys()` already returns. It is essentially the "manual key registration only (no discovery)" path that #23388 keeps as an alternative for power users (§10 there). If maintainers are wary of the trust-surface expansion in the full RFC, this is a strictly smaller change that unblocks the same downstream use case, and can serve as the pathfinder — with #23388 layered on top later if the team decides to.

Related prior context we found:

- [#9709 — Unsecured extension file downloads (MITM)](https://github.com/duckdb/duckdb/issues/9709) — closed with the conclusion that *signing*, not TLS, is the integrity guarantee. This proposal reuses that principle unchanged.
- The [Community Extensions announcement](https://duckdb.org/2024/07/05/community-extensions) explicitly acknowledges that `allow_unsigned_extensions` is "problematic in itself" and that centralization is the current answer. This proposal is the smallest possible decentralized escape valve.

## Motivation — concrete use case

We (tegmentum) run [ducklink](https://github.com/tegmentum/ducklink-extension), a DuckDB extension that dynamically loads portable WebAssembly extension modules. For workloads where the WASM sandbox overhead matters (25–40× vs native DuckDB), we want to publish native `.duckdb_extension` builds of specific components (validators, encoders, etc.) that share source with the WASM versions.

Publishing each as a separate community extension isn't the right shape — the whole point of our catalog is to avoid the community-extensions governance funnel for hundreds of small capabilities. Serving native binaries from our own infrastructure works, but only if users flip `allow_unsigned_extensions=true` — a real security-posture change most users don't want to make just to load a bank-routing-number validator.

With `custom_trusted_extension_keys`, an operator would `SET custom_trusted_extension_keys = '<tegmentum public key PEM>';` once (in a config file, environment variable, or startup script) and thereafter load any tegmentum-signed extension without touching the unsigned flag. Unsigned extensions from *other* sources would still fail signature check.

The same pain point applies to any organization that has a signed extension it wants to distribute outside `duckdb/community-extensions`. Query.farm's public write-up (and their operational reasons for maintaining their own [Haybarn](https://github.com/Query-farm-haybarn) distribution — cited by @rustyconover in #23388) is another data point that the "either central community repo or `allow_unsigned_extensions`" binary is a real friction.

## Proposed shape

New global setting `custom_trusted_extension_keys`:

- Type: `VARCHAR` (semicolon-separated list of PEM-encoded public keys; a single PEM is also valid).
- Scope: `SettingScopeTarget::GLOBAL_ONLY`, matching `AllowCommunityExtensionsSetting`.
- Default: empty string (behaviour byte-for-byte unchanged from today).
- `OnSet` callback: **one-way** — the setting can only be set while `db` is null (i.e., before database construction) or when only *adding* keys. Once set, keys cannot be removed at runtime. This mirrors the one-way semantics of `allow_unsigned_extensions` / `allow_community_extensions` that @carlopi highlighted in #23388.

Extensions signed with any of the configured keys load normally. The core DuckDB and community keys remain trusted exactly as they are today; the setting is strictly additive.

### Naming — options and tradeoffs

| Name | Pros | Cons |
|---|---|---|
| `custom_trusted_extension_keys` | mirrors `custom_extension_repository`; clear "trusted keys" | slightly long |
| `additional_trusted_extension_keys` | very explicit that it's additive | longer still |
| `trusted_extension_signing_keys` | includes "signing"; explicit | may conflict with future PKI-style extension |
| `allow_custom_extension_keys` (bool) + `custom_extension_keys` (varchar) | matches `allow_community_extensions` + `custom_extension_repository` split | two settings instead of one; more surface |

Weak preference for `custom_trusted_extension_keys` — the shortest name that's still unambiguous and consistent with the `custom_extension_repository` precedent (`src/common/settings.json:229`).

### Nice-to-have follow-ups (out of scope for the initial ask)

- `TRUST EXTENSION KEY '<pem>'` DDL sugar mirroring `SET custom_trusted_extension_keys` for one-key-at-a-time addition.
- Env var (`DUCKDB_TRUSTED_EXTENSION_KEYS`) so container / CLI deployments can configure without a `SET`.
- File-path form (`custom_trusted_extension_keys_file = '/etc/duckdb/trusted_keys.pem'`) for larger key sets.

## Security discussion

**Q: Isn't this weaker than the current model?**

A: No — strictly stronger than `allow_unsigned_extensions` (the current alternative), and comparable to `allow_community_extensions=true` (on by default, trusts every community-extensions author). Users who run `INSTALL foo FROM community` today implicitly trust every author community-extensions has vetted. A pinned custom key is a *narrower* trust statement: "I trust this one specific author's extensions." That's tighter than the community model, not looser.

**Q: Doesn't this widen the trust surface the way #23388 does?**

A: No — that's the specific reason this proposal is minimal. #23388 widens the trust surface because trust is tied to *installation provenance* recorded in the `.info` sidecar, which becomes trust-relevant metadata. This proposal doesn't touch `.info` at all: `GetPublicKeys()` just returns a bigger set, and the existing verification path (`CheckKnownSignatures` → `MbedTlsWrapper::IsValidSha256Signature`) is unchanged. The trust surface added is exactly one config value, no new files, no new secrets.

**Q: What if a malicious extension author tricks users into pinning their key?**

A: Same social-engineering problem exists for the community trust model — a user has to decide what to trust based on the reputation of the source. The mechanism doesn't change the trust decision; it makes the trust decision explicit and granular. And unlike `allow_unsigned_extensions`, a pinned key can only validate extensions actually signed by that key; a tampered download still fails.

**Q: Should keys be verifiable against a chain of trust / rotate / expire?**

A: For the initial version, no — raw public keys, same shape as the built-in ones baked into `public_keys[]` and `community_public_keys[]`. Rotation is a `SET` away. If PKI-style chains or transparency logs become useful later, they layer on top; keeping the initial shape simple avoids re-litigating cryptography choices during PR review. #23388's discovery + TOFU pin path is the natural next step if the team wants an automated flow later.

**Q: Interaction with `allow_unsigned_extensions`?**

A: Independent axes. When `allow_unsigned_extensions=true`, signature checking is off and this setting is moot (as expected). When it's false (the default), a custom-trusted-key extension loads; unsigned or wrong-key extensions still fail. Same posture the community-key path already has.

## Implementation sketch

Small — 3–4 files, ~50–80 LOC of real change plus tests. Verified against the v1.5.x baseline in `duckdb/duckdb`:

- `src/main/extension/extension_helper.cpp:853` — `ExtensionHelper::GetPublicKeys(bool allow_community_extensions)` today concatenates `public_keys[]` and (conditionally) `community_public_keys[]`. Extend the signature to also accept the semicolon-parsed list from `db.config.options.custom_trusted_extension_keys`, and append.
- `src/main/extension/extension_load.cpp:315` — `CheckKnownSignatures` and its three callers (`CheckExtensionSignature`, both `CheckExtensionBufferSignature` overloads) forward the extra key list.
- `src/main/extension/extension_load.cpp:485-500` — `TryInitialLoad` reads the new setting via `Settings::Get<CustomTrustedExtensionKeysSetting>(db)` alongside the existing `AllowCommunityExtensionsSetting` read, and passes it into `CheckExtensionSignature`.
- `src/include/duckdb/main/settings.hpp` — new `CustomTrustedExtensionKeysSetting` struct, mirroring `CustomExtensionRepositorySetting` (line 397) for VARCHAR / `GLOBAL_ONLY`, and mirroring `AllowCommunityExtensionsSetting::OnSet` (line 152) for one-way semantics.
- `src/main/settings/custom_settings.cpp` — implement the `OnSet` callback.
- `src/common/settings.json` — add the JSON entry (parallel to `custom_extension_repository` at line 229).
- `src/main/config.cpp` — register in `DUCKDB_SETTING_CALLBACK(...)` block (mirrors line 73).

Existing OpenSSL / mbedtls verification path (`duckdb_mbedtls::MbedTlsWrapper::IsValidSha256Signature`) is agnostic to key source; just extending the key pool is sufficient. No change to verification logic.

Tests:

- Fixture extension signed with a test RSA key. Load with the trust set → succeed. Load without → fail with the existing `UNSIGNED_EXTENSION` error.
- Confirm `custom_trusted_extension_keys = ''` (default) leaves current behaviour completely unchanged (byte-for-byte identical `GetPublicKeys()` output).
- Confirm interaction with `allow_unsigned_extensions=true` (unsigned still loads regardless, expected).
- Confirm malformed PEM produces a clear parse error at `SET` time, not at load time.

Happy to prototype this in a fork and open a PR once there's directional buy-in — the change is small enough that a review could start from the code rather than the design.

## Companion prototype sketch

We've drafted a slightly more detailed diff-style implementation outline as a separate document, kept out of this discussion for readability: [`docs/duckdb-upstream-custom-trusted-keys-prototype.md`](./duckdb-upstream-custom-trusted-keys-prototype.md) in the ducklink-extension repo. It covers the exact `settings.hpp` struct, the `GetPublicKeys` refactor, the `extension_load.cpp` call-site edits, and the test-fixture layout. Purpose: show the DuckDB team we've thought through implementation, not just spec'd it.

## What we're asking

Before we invest in a PR:

1. Is the *minimal* direction here agreeable, given that #23388 is the fuller version and has some concerns still under discussion? Concretely: would maintainers rather (a) land a minimal `custom_trusted_extension_keys` first and layer #23388 on later, (b) hold this for #23388 to settle, or (c) reject the direction entirely?
2. Any preference on shape — single-value `VARCHAR` PEM, semicolon-separated list, or a keys-file path? (Weakly prefer semicolon-separated inline PEM to match how `custom_extension_repository` is a plain string.)
3. Any preference on setting name? (Weakly prefer `custom_trusted_extension_keys`.)
4. Prefer we file this as a formal issue and follow the RFC/PR flow, or continue the design discussion here first?

If #23388's direction is preferred and the maintainers have capacity to review it in the near term, we're happy to withdraw this and support that PR instead — the two proposals aren't in tension. We're raising this one because it's a small enough change to prototype and merge on a shorter timeline than #23388, and it gets us out of the `allow_unsigned_extensions` bind in the meantime.

Cross-links to any prior discussion we missed would be useful.

---

_Filed by: tegmentum (ducklink authors), 2026-07-09._
