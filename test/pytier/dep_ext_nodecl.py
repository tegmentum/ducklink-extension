"""Control for the PEP 723 staging test: SAME `import six` scalar, but with NO
PEP 723 `# /// script` dependency block.

Because nothing declares `six`, `ducklink_run` stages no wheel, so the resident
interpreter has no `six` on `sys.path`. Calling the function must FAIL with a
`ModuleNotFoundError: six` — proving the staging (not something else) is what
makes `dep_ext.py` work.
"""

from __future__ import annotations

import ducklink


@ducklink.scalar
def six_type_name_nodecl(x: float) -> str:
    import six  # unstaged: no PEP 723 block declares it

    if isinstance(x, six.integer_types) or (isinstance(x, float) and x.is_integer()):
        return "int"
    return "float"
