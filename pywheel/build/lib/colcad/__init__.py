"""``colcad``: the colcad binary, packaged for ``chaski[node]``.

Carries no logic of its own — ``chaski.Node``
resolves the binary through ``BINARY_PATH`` when the caller has not set
``COLCAD_BINARY`` and no ``colcad`` is on PATH.
"""

import stat
from pathlib import Path

BINARY_PATH = Path(__file__).resolve().parent / "bin" / "colcad"
# The contracts bundle generated from the same commit as the binary
# (scripts/build_colcad_wheels.py): ``chaski.Node`` points colcad at it, since
# a host has no /etc/colca/contracts-bundle.json and a colcad without a bundle
# only applies its minimal built-in checks.
BUNDLE_PATH = Path(__file__).resolve().parent / "contracts-bundle.json"

# Wheel packaging preserves *nix executable bits (wheel>=0.42), but this is
# cheap insurance against an extraction path that does not — an un-executable
# BINARY_PATH would fail with a confusing "Permission denied" far from here,
# in chaski.Node.start()'s subprocess.Popen call.
try:
    if BINARY_PATH.exists():
        mode = BINARY_PATH.stat().st_mode
        BINARY_PATH.chmod(mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
except OSError:
    pass

__all__ = ["BINARY_PATH", "BUNDLE_PATH"]
