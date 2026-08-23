"""Every curated ligature must exist in the font the UI actually ships.

The icon set and the font are two owners of one fact — which ligatures exist.
Without this check they drift silently and a tag renders a blank glyph on
every deployment, which is exactly what `location_on` did.

This test is only ever collected where `ui/node_modules` is guaranteed to
exist (the pipeline's `test-ui` job runs `npm ci` first) — see the
`icon_test_args` guard in `scripts/dev.py`'s `run_unit_phase`, which leaves it
out of collection everywhere else instead of letting it skip silently. So a
missing font here is a real environment defect, not something to skip past.
"""

import pathlib

from fontTools.ttLib import TTFont

from colca_data_contracts.icons import ICON_SET

FONT = (
    pathlib.Path(__file__).resolve().parents[2]
    / "ui/node_modules/@alpamayo-solutions/design/assets/fonts"
    / "material-symbols/material-symbols-outlined.woff2"
)


def test_every_curated_icon_exists_in_the_shipped_font():
    assert FONT.exists(), f"shipped font not found at {FONT} — was `npm ci` run in ui/?"
    glyphs = set(TTFont(FONT).getGlyphOrder())
    missing = sorted(name for name in ICON_SET if name not in glyphs)
    assert not missing, (
        f"{len(missing)} curated icons are not in the shipped font and would "
        f"render as a blank glyph: {missing}"
    )
