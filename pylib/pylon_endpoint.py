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

  runtime.load     importlib-import a user script so its ``@ducklink`` decorators
                   fire and populate ducklink's registry; returns the function
                   count registered so far (the host reads the full manifest via
                   ``runtime.manifest``).
  runtime.manifest returns ``ducklink.runtime.manifest()`` — the JSON-able
                   ``list[dict]`` describing every registered function.
  offload          resolves the manifest ``entry`` (``"module:callable"``), calls
                   it with ``args``/``kwargs``, and returns its value.

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


# --- dispatch --------------------------------------------------------------


METHODS = {
    "ping": _m_ping,
    "echo": _m_echo,
    "runtime.load": _m_runtime_load,
    "runtime.manifest": _m_runtime_manifest,
    "offload": _m_offload,
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
