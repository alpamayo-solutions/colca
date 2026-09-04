"""Legacy setup.py entry point, kept for exactly one reason: `bdist_wheel
--python-tag py3 --plat-name <tag>` is how a CROSS-COMPILED, Python-version-
independent binary wheel forces both its interpreter tag and its platform
tag — the build host is always linux/amd64 (the CI runner) and its
interpreter is whatever that runner's Python happens to be, but colcad is
neither: the payload may be linux/arm64 or macosx/arm64, and colcad itself
works identically under any Python since it is not a Python extension at
all. PEP 517 has no hook for overriding either tag from pyproject.toml
alone, so this file exists purely to give `python setup.py bdist_wheel`
something to run — every other setting lives in pyproject.toml.
"""

from setuptools import setup

setup()
