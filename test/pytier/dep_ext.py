# /// script
# dependencies = ["six"]
# ///
"""A ducklink Python source-tier extension with a PEP 723 inline dependency.

Declares a single PURE-PYTHON dependency (`six`) in its PEP 723 `# /// script`
block, and its `@ducklink.scalar` function imports + uses `six`. `ducklink_run`
must resolve + stage the `six` wheel into the resident interpreter's
`/app/site-packages` BEFORE loading this script, or the `import six` fails.

Verification:
  SELECT six_type_name(1)   -> 'int'   (six.integer_types check succeeded)
  SELECT six_type_name(2.0) -> 'float' (proves the dep actually ran)
"""

from __future__ import annotations

import ducklink


@ducklink.scalar
def six_type_name(x: float) -> str:
    """Return the Python type name, using `six` to prove the dep was staged.

    `six.integer_types` is `(int,)` on Python 3 — importing and using `six` here
    means the wheel was fetched from PyPI, unzipped into `/app/site-packages`, and
    made importable inside the wasm interpreter.
    """
    import six

    if isinstance(x, bool):
        return "bool"
    # DuckDB hands DOUBLE args as float; a whole number stays exactly representable.
    if isinstance(x, six.integer_types) or (isinstance(x, float) and x.is_integer()):
        return "int"
    return "float"
