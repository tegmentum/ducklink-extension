"""ducklink's pylon-endpoint dispatcher — the ducklink-owned Python side of the
``compose:dynlink/endpoint`` provider.

This module implements the reactor <-> dispatcher contract of the generic
pylon-endpoint reactor (``bindings/pylon-endpoint/shim.c`` in python-wasm),
WITHOUT living in pylon's repo. The reactor imports a module named
``pylon_endpoint`` from its ``/app`` preopen at runtime (the ``.py`` is NOT baked
into the component) and caches its ``handle`` callable. Because the HOST controls
what is mounted at ``/app``, ducklink ships THIS dispatcher there — so a plain,
generic pylon endpoint (carrying zero ducklink code) serves the ducklink Python
source tier.

Reactor <-> dispatcher contract (what ``shim.c`` requires of us):

* the module is importable as ``pylon_endpoint`` from ``/app``;
* it exposes a callable ``handle(method: str, payload: bytes) -> bytes``;
* ``handle`` returns the response bytes (msgpack is ducklink's application
  encoding), or raises on failure — the reactor converts any raised exception
  into the WIT ``error`` variant (``error-code = invalid-input``) so the boundary
  never traps. (A ``METHODS`` registry is the conventional shape; the reactor
  only binds ``handle``.)

Methods (ducklink Python source tier — Phase-1 MVP, msgpack over the WIT
boundary; arrow-columnar batching is a Phase-2 follow-on):

  method            payload (msgpack)                -> result (msgpack)
  ------            ------------------------------   ----------------------------
  ping              <ignored>                        -> {"python": "<version>"}
  echo              <any bytes>                       -> <same bytes, verbatim>
  runtime.load      {"module": "<script>"}            -> <int fn count>
  runtime.manifest  <ignored>                         -> [<manifest entry>, ...]
  offload           {"entry":"mod:fn","args":[..]}    -> <fn return value>
  offload_arrow     {"entry":"mod:fn","arrow":<ipc>}  -> <arrow ipc (col 'result')>

  runtime.load     importlib-import a user script so its ``@ducklink`` decorators
                   fire and populate ducklink's registry; returns the function
                   count registered so far (the host reads the full manifest via
                   ``runtime.manifest``).
  runtime.manifest returns ``ducklink.runtime.manifest()`` — the JSON-able
                   ``list[dict]`` describing every registered function.
  offload          resolves the manifest ``entry`` (``"module:callable"``), calls
                   it with ``args``/``kwargs``, and returns its value. This is the
                   per-row fallback path (one WIT crossing per row).
  offload_arrow    the ARROW-COLUMNAR scalar dispatch: ONE crossing per DuckDB
                   DataChunk. The host serializes the chunk's argument columns
                   (named ``arg0``, ``arg1``, ...) into a single Arrow IPC stream;
                   this decodes them, resolves ``entry``, applies it row-wise with
                   DuckDB NULL semantics (any NULL arg -> NULL result, the fn is
                   NOT called), and encodes the result column (named ``result``)
                   back into an Arrow IPC stream. The batch/vectorization lives
                   HERE (ducklink-owned) so the WIT interface stays generic.

``ping``/``echo`` are generic health checks that need no ducklink import, so the
dispatcher is smoke-testable without a user script staged.
"""

from __future__ import annotations

import importlib as _importlib
import sys
from functools import reduce as _reduce

import _msgpack


class EndpointError(Exception):
    """Structured failure surfaced to the host as the WIT ``error`` variant."""


class RAW(bytes):
    """Marker: handle() returns these bytes verbatim (no msgpack re-encode)."""


# --- generic health checks -------------------------------------------------


def _m_echo(payload: bytes):
    # Return the raw request bytes verbatim. Bypasses msgpack entirely so the
    # provider is testable without a codec on either side.
    return RAW(payload)


def _m_ping(payload: bytes):
    return {"python": sys.version.split()[0]}


# --- ducklink Python source tier: load -> manifest -> offload --------------
#
# The generic offload envelope the ducklink host drives to run a user-authored
# Python source extension inside this resident interpreter. Only bytes cross the
# WIT boundary (compose:dynlink/endpoint); msgpack is the application encoding.


def _resolve_entry(entry: str):
    """Resolve an ``"module:callable"`` entry to the callable object.

    ``callable`` may be a dotted attribute path (``pkg.mod:Cls.method``).
    """
    module_name, sep, attr_path = entry.partition(":")
    if not sep or not module_name or not attr_path:
        raise EndpointError("entry %r must be 'module:callable'" % (entry,))
    obj = _importlib.import_module(module_name)
    return _reduce(getattr, attr_path.split("."), obj)


# PEP 723 inline dependencies the host resolves+stages are unzipped into
# ``/app/site-packages`` (under the same ``/app`` preopen this dispatcher and the
# user script live in). Prepend it to ``sys.path`` so a script's ``import <dep>``
# resolves against a staged pure-Python wheel. Idempotent: added once, ahead of
# ``/app`` so a staged dependency shadows nothing but is found before the stdlib.
_SITE_PACKAGES = "/app/site-packages"


def _ensure_site_packages_on_path():
    if _SITE_PACKAGES not in sys.path:
        sys.path.insert(0, _SITE_PACKAGES)
        # A newly-created dir may not be reflected in importer caches yet.
        _importlib.invalidate_caches()


def _m_runtime_load(payload: bytes):
    """Import the user script so its ``@ducklink`` decorators register.

    Returns the number of functions registered so far (the host reads the full
    manifest separately via ``runtime.manifest``).
    """
    req = _msgpack.unpackb(payload)
    if not isinstance(req, dict) or "module" not in req:
        raise EndpointError("runtime.load payload must be a msgpack map with 'module'")
    module = req["module"]
    if not isinstance(module, str) or not module:
        raise EndpointError("runtime.load 'module' must be a non-empty string")
    # Make any host-staged PEP 723 pure-Python deps importable before the script.
    _ensure_site_packages_on_path()
    _importlib.import_module(module)
    import ducklink  # resident once imported; on the /app search path

    return len(ducklink.REGISTRY)


def _m_runtime_manifest(payload: bytes):
    """Return ducklink's registered-function manifest (JSON-able list<map>)."""
    import ducklink

    return ducklink.runtime.manifest()


def _m_offload(payload: bytes):
    """Resolve ``entry`` and call it with ``args``/``kwargs``; return its value.

    This is the per-row scalar/table/aggregate dispatch the host drives once per
    row in the MVP. ``entry`` is the manifest's ``"module:callable"`` string.
    """
    req = _msgpack.unpackb(payload)
    if not isinstance(req, dict) or "entry" not in req:
        raise EndpointError("offload payload must be a msgpack map with 'entry'")
    fn = _resolve_entry(req["entry"])
    args = req.get("args", []) or []
    kwargs = req.get("kwargs", {}) or {}
    return fn(*args, **kwargs)


# --- arrow-columnar scalar dispatch (one crossing per DataChunk) -----------
#
# The default scalar dispatch. The host hands us ONE Arrow IPC stream carrying
# the whole DataChunk's argument columns; we apply the target fn over the zipped
# rows and hand back ONE Arrow IPC stream carrying the result column. Only bytes
# cross compose:dynlink/endpoint; the batch/vectorization is ducklink-owned.

# One crossing per chunk -> this counter tracks chunks, not rows. Emitted on
# stderr so the host-side CLI verification can prove ~ceil(rows/2048) crossings
# (NOT one per row). Cheap; guarded so it never breaks the dispatch.
_ARROW_DISPATCH_CALLS = 0


def _encode_result_column(values):
    """Encode the result list as a one-column Arrow IPC stream named ``result``.

    Picks the ``_arrow_core`` per-type encoder from the first non-None value
    (DuckDB has already fixed the scalar's SQL return type, so the column is
    homogeneous). An all-NULL / empty column defaults to an int64 column of
    nulls — the host reads it back purely through the validity bitmap, so the
    physical type of an all-NULL column is immaterial.
    """
    import _arrow_core

    sample = next((v for v in values if v is not None), None)
    if sample is None:
        return _arrow_core.ipc_encode_int64("result", values)
    if isinstance(sample, bool):
        return _arrow_core.ipc_encode_bool("result", values)
    if isinstance(sample, int):
        return _arrow_core.ipc_encode_int64("result", values)
    if isinstance(sample, float):
        return _arrow_core.ipc_encode_float64("result", values)
    if isinstance(sample, str):
        return _arrow_core.ipc_encode_string("result", values)
    # Coerce anything else to its str() so an authored fn returning e.g. a
    # Decimal still crosses cleanly (the host's declared VARCHAR return catches
    # the type mismatch if it is not actually a string column).
    return _arrow_core.ipc_encode_string("result", [None if v is None else str(v) for v in values])


def _m_offload_arrow(payload: bytes):
    """Arrow-columnar scalar dispatch: apply ``entry`` over an Arrow column batch.

    Payload is a msgpack map ``{"entry": "<mod:fn>", "arrow": <ipc bytes>}``. The
    ``arrow`` bytes are one Arrow IPC stream whose columns (``arg0``, ``arg1``,
    ...) are the scalar's positional arguments for the whole DataChunk. Returns
    one Arrow IPC stream with a single column ``result`` of the same length.

    NULL semantics match DuckDB scalars: if ANY argument in a row is NULL the
    result is NULL and the target fn is NOT called for that row.
    """
    global _ARROW_DISPATCH_CALLS
    import _arrow_core

    req = _msgpack.unpackb(payload)
    if not isinstance(req, dict) or "entry" not in req or "arrow" not in req:
        raise EndpointError("offload_arrow payload must be a msgpack map with 'entry' and 'arrow'")
    fn = _resolve_entry(req["entry"])

    # Decode the multi-column input batch: [(name, type, values), ...] with
    # None entries marking nulls. Sort by the numeric suffix of "argN" so the
    # positional order is exactly what the SQL call site passed (independent of
    # however arrow orders the schema fields).
    cols = _arrow_core.ipc_decode_batch(bytes(req["arrow"]))
    cols.sort(key=lambda c: int(c[0][3:]) if c[0].startswith("arg") else 0)
    value_lists = [c[2] for c in cols]

    _ARROW_DISPATCH_CALLS += 1
    n = len(value_lists[0]) if value_lists else 0
    try:
        sys.stderr.write(
            "[ducklink-pyendpoint] offload_arrow #%d entry=%s rows=%d arity=%d\n"
            % (_ARROW_DISPATCH_CALLS, req["entry"], n, len(value_lists))
        )
    except Exception:
        pass

    out = [None] * n
    for i in range(n):
        row = [vl[i] for vl in value_lists]
        if any(v is None for v in row):
            # DuckDB NULL semantics: propagate NULL without calling the fn.
            continue
        out[i] = fn(*row)

    return RAW(_encode_result_column(out))


# --- dispatch --------------------------------------------------------------


METHODS = {
    "ping": _m_ping,
    "echo": _m_echo,
    "runtime.load": _m_runtime_load,
    "runtime.manifest": _m_runtime_manifest,
    "offload": _m_offload,
    "offload_arrow": _m_offload_arrow,
}


def handle(method: str, payload: bytes) -> bytes:
    """Dispatch one ``(method, payload)`` request to its Python function.

    Returns the msgpack-encoded result bytes (or the raw bytes for ``echo``).
    Raises :class:`EndpointError` / any exception on failure; the reactor shim
    converts that into the WIT ``error`` variant.
    """
    fn = METHODS.get(method)
    if fn is None:
        raise EndpointError("unknown method: %r" % (method,))
    out = fn(bytes(payload))
    if isinstance(out, RAW):
        return bytes(out)
    return _msgpack.packb(out)
