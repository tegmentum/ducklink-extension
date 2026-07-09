# Prototype sketch: `custom_trusted_extension_keys`

Companion to [`duckdb-upstream-custom-trusted-keys.md`](./duckdb-upstream-custom-trusted-keys.md). Not a real diff — a diff-shaped outline of what the actual PR against `duckdb/duckdb` (v1.5.x baseline) would touch. Purpose: demonstrate that the "small — 3–4 files, ~50–80 LOC" claim in the discussion post is grounded in the actual code paths, and give a maintainer a concrete artifact to poke at before we spend PR time on it.

All file paths are relative to `duckdb/duckdb`.

---

## 1. Setting registration

### `src/include/duckdb/main/settings.hpp`

Add a new struct following the `CustomExtensionRepositorySetting` template (line 397) and the `AllowCommunityExtensionsSetting` one-way `OnSet` pattern (line 144). Bump `SettingIndex` to the next free slot.

```cpp
struct CustomTrustedExtensionKeysSetting {
    using RETURN_TYPE = string;
    static constexpr const char *Name = "custom_trusted_extension_keys";
    static constexpr const char *Description =
        "Semicolon-separated PEM-encoded RSA public keys that are trusted as "
        "extension signing keys, in addition to the core DuckDB and community keys";
    static constexpr const char *InputType = "VARCHAR";
    static constexpr const char *DefaultValue = "";
    static constexpr SettingScopeTarget Scope = SettingScopeTarget::GLOBAL_ONLY;
    static constexpr idx_t SettingIndex = /* next free */;
    static void OnSet(SettingCallbackInfo &info, Value &input);
};
```

### `src/main/settings/custom_settings.cpp`

Implement the `OnSet` callback with one-way semantics — new keys can be added at runtime, existing keys cannot be removed. This matches @carlopi's guidance in duckdb#23388 that opt-in trust settings must be one-way to prevent runtime downgrade.

```cpp
//===----------------------------------------------------------------------===//
// Custom Trusted Extension Keys
//===----------------------------------------------------------------------===//
void CustomTrustedExtensionKeysSetting::OnSet(SettingCallbackInfo &info, Value &input) {
    auto new_value = input.ToString();
    auto old_value = info.db ? Settings::Get<CustomTrustedExtensionKeysSetting>(*info.db) : "";

    // Basic validation: parse each PEM chunk once so we fail fast at SET time,
    // not at LOAD time when the error is confusing.
    auto new_keys = ParsePemList(new_value);   // helper defined in extension_helper.cpp
    for (auto &pem : new_keys) {
        if (!duckdb_mbedtls::MbedTlsWrapper::IsValidPublicKey(pem)) {
            throw InvalidInputException(
                "custom_trusted_extension_keys: invalid PEM public key");
        }
    }

    // One-way: refuse to drop any previously-trusted key.
    if (info.db) {
        auto old_keys = ParsePemList(old_value);
        std::set<string> new_set(new_keys.begin(), new_keys.end());
        for (auto &k : old_keys) {
            if (!new_set.count(k)) {
                throw InvalidInputException(
                    "custom_trusted_extension_keys can only be extended at "
                    "runtime, not narrowed; restart the database to remove keys");
            }
        }
    }
}
```

### `src/common/settings.json`

Add the JSON entry next to `custom_extension_repository` (line 229):

```json
{
    "name": "custom_trusted_extension_keys",
    "description": "Semicolon-separated PEM-encoded RSA public keys trusted as extension signing keys",
    "type": "VARCHAR",
    "scope": "global",
    "default_value": "",
    "on_callbacks": ["set"]
}
```

### `src/main/config.cpp`

Add to the `DUCKDB_SETTING_CALLBACK(...)` block near `AllowCommunityExtensionsSetting` (line 73):

```cpp
DUCKDB_SETTING_CALLBACK(CustomTrustedExtensionKeysSetting),
```

---

## 2. Key-loading changes

### `src/main/extension/extension_helper.cpp`

Add a small PEM-list parser near the existing key arrays (around line 851, right before `GetPublicKeys`), and thread the extra key list through `GetPublicKeys`.

```cpp
// Parse "PEM1;PEM2;..." — tolerant of whitespace and empty segments.
vector<string> ExtensionHelper::ParsePemList(const string &raw) {
    vector<string> out;
    idx_t start = 0;
    for (idx_t i = 0; i <= raw.size(); i++) {
        if (i == raw.size() || raw[i] == ';') {
            auto chunk = raw.substr(start, i - start);
            StringUtil::Trim(chunk);
            if (!chunk.empty()) {
                out.push_back(std::move(chunk));
            }
            start = i + 1;
        }
    }
    return out;
}

const vector<string> ExtensionHelper::GetPublicKeys(
    bool allow_community_extensions,
    const vector<string> &custom_trusted_keys) {
    vector<string> keys;
    for (idx_t i = 0; public_keys[i]; i++) {
        keys.emplace_back(public_keys[i]);
    }
    if (allow_community_extensions) {
        for (idx_t i = 0; community_public_keys[i]; i++) {
            keys.emplace_back(community_public_keys[i]);
        }
    }
    for (const auto &pem : custom_trusted_keys) {
        keys.push_back(pem);
    }
    return keys;
}
```

### `src/include/duckdb/main/extension_helper.hpp`

Update the `GetPublicKeys` declaration (line 162) and expose the parser helper:

```cpp
static const vector<string> GetPublicKeys(
    bool allow_community_extension = false,
    const vector<string> &custom_trusted_keys = {});
static vector<string> ParsePemList(const string &raw);
```

Keep the default arg on `custom_trusted_keys` so callers outside `extension_load.cpp` that don't have access to `DBConfig` don't have to be touched.

### `src/main/extension/extension_load.cpp`

Thread the extra key list through the three verification wrappers (lines 315, 326, 344, 357). Each currently takes `const bool allow_community_extensions`; each grows a `const vector<string> &custom_trusted_keys` param that is forwarded to `GetPublicKeys` via `CheckKnownSignatures`.

At the single call site inside `TryInitialLoad` (line 485):

```cpp
if (!Settings::Get<AllowUnsignedExtensionsSetting>(db)) {
    bool signature_valid;
    if (parsed_metadata.AppearsValid()) {
        bool allow_community_extensions = Settings::Get<AllowCommunityExtensionsSetting>(db);
        auto custom_trusted_raw       = Settings::Get<CustomTrustedExtensionKeysSetting>(db);
        auto custom_trusted_keys      = ExtensionHelper::ParsePemList(custom_trusted_raw);
        signature_valid = CheckExtensionSignature(*handle, parsed_metadata,
                                                  allow_community_extensions,
                                                  custom_trusted_keys);
    } else {
        signature_valid = false;
    }
    ...
}
```

The parser runs once per load; the setting is read only when the extension has valid metadata, so the parse cost is trivial.

---

## 3. Tests

### Fixture

Add `test/extension/custom_trusted_key/` with:

- `test_signing_key.pem` — RSA private key generated with `openssl genrsa 2048` (checked in, deliberately a test key).
- `test_signing_key.pub.pem` — matching public key (paste into the `SET` in the test).
- `sign_extension.py` — helper to append a signature block to a test extension binary, reusing the existing `scripts/extension-upload-single.py` signing shape.
- A trivial `.duckdb_extension` binary built from a stub extension and signed with the key above.

### `test/sql/extension/custom_trusted_extension_keys.test`

sqllogictest coverage. The load-succeeds path assumes the fixture binary has been produced by the extension build harness.

```
# name: test/sql/extension/custom_trusted_extension_keys.test
# description: custom_trusted_extension_keys lets a specific 3rd-party key load a signed extension

require-env TEST_EXT_TRUSTED_KEY_PEM
require-env TEST_EXT_TRUSTED_SIGNED_PATH

# Default behaviour unchanged: without the key, load fails
statement error
LOAD '${TEST_EXT_TRUSTED_SIGNED_PATH}'
----
Extension is not signed

# With the key set, load succeeds
statement ok
SET custom_trusted_extension_keys = '${TEST_EXT_TRUSTED_KEY_PEM}'

statement ok
LOAD '${TEST_EXT_TRUSTED_SIGNED_PATH}'

# One-way: removing keys at runtime is rejected
statement error
SET custom_trusted_extension_keys = ''
----
custom_trusted_extension_keys can only be extended at runtime

# Malformed PEM rejected at SET time
statement error
SET custom_trusted_extension_keys = 'not a pem key'
----
invalid PEM public key
```

Plus a small C++ test in `test/api/extension_signing/` that verifies `GetPublicKeys("", {})` returns exactly the same vector as `GetPublicKeys(false)` did before the change, guarding the "byte-for-byte unchanged by default" property.

### Interaction test

```
# custom_trusted_extension_keys is orthogonal to allow_unsigned_extensions
statement ok
SET allow_unsigned_extensions = true

# Unsigned still loads (allow_unsigned wins)
statement ok
LOAD '${TEST_EXT_UNSIGNED_PATH}'
```

---

## 4. Rough LOC accounting

| File | LOC change |
|---|---|
| `src/include/duckdb/main/settings.hpp` | +12 |
| `src/main/settings/custom_settings.cpp` | +30 |
| `src/common/settings.json` | +8 |
| `src/main/config.cpp` | +1 |
| `src/main/extension/extension_helper.cpp` | +25 (+parser, +GetPublicKeys change) |
| `src/include/duckdb/main/extension_helper.hpp` | +3 |
| `src/main/extension/extension_load.cpp` | +5 (call-site plumbing) |
| **Total src** | **~85 LOC** |
| Tests + fixture (excluding the signed binary) | ~120 LOC |

Well under the 200-LOC threshold that typically distinguishes a "small self-contained change" from something that needs its own design cycle.

---

## 5. What this does *not* do

Explicitly out of scope for this prototype — the fuller RFC #23388 is the right home for these:

- No origin scoping: a custom-trusted key trusts extensions from *any* URL, not just a specific repository. This is a stronger trust statement than #23388's per-origin pinning, and the docs should call it out.
- No `.well-known` discovery, no `duckdb_register_extension_repo()`, no `extension_repository` secret type, no `.info` sidecar changes.
- No rotation UI (rotation = another `SET`).
- No `duckdb_extension_repositories()` observability table (though `duckdb_settings()` shows the raw PEM list — probably good enough for a v1).

Anyone who needs the richer per-origin model should push on #23388.
