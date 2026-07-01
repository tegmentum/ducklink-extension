"""A ducklink Python source-tier extension served over http(s) for the
arbitrary-URL loader test (Part 2). No inline deps — it only proves the URL is
downloaded, cached, and run like a local script.
"""

from __future__ import annotations

import ducklink


@ducklink.scalar
def url_greet(who: str) -> str:
    """Prove a URL-hosted script ran."""
    return f"hello-from-url:{who}"
