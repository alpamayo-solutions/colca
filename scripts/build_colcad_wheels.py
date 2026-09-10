#!/usr/bin/env python3
"""Build colcad platform wheels for chaski[node].

colcad is Go, so every platform cross-compiles from one build host with no C
toolchain (CGO_ENABLED=0), using the same `go build -trimpath` invocation as
deploy/Dockerfile. The wheel and the image are therefore built from the same
source in the same way.

Each target gets its own setuptools build (`setup.py bdist_wheel --plat-name
<tag>`; pywheel/setup.py explains why the legacy entry point is still needed),
staged in a throwaway copy of pywheel/ so the builds never share a `build/`
directory.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PYWHEEL_SRC = REPO_ROOT / "pywheel"

# (GOOS, GOARCH, wheel platform tag). The binary is static, so one manylinux
# baseline covers glibc hosts. Go 1.26 needs macOS 12.
TARGETS = [
    ("linux", "amd64", "manylinux_2_17_x86_64"),
    ("linux", "arm64", "manylinux_2_17_aarch64"),
    ("darwin", "arm64", "macosx_12_0_arm64"),
    ("darwin", "amd64", "macosx_12_0_x86_64"),
]


def build_one(goos: str, goarch: str, plat_tag: str, version: str, out_dir: Path, bundle: Path) -> Path:
    with tempfile.TemporaryDirectory(prefix=f"colcad-{goos}-{goarch}-") as tmp:
        staged = Path(tmp) / "source"
        shutil.copytree(PYWHEEL_SRC, staged)

        bin_dir = staged / "colcad" / "bin"
        bin_dir.mkdir(parents=True)
        # The bundle of the same commit goes beside the binary: a host has no
        # /etc/colca/contracts-bundle.json, and without a bundle colcad only
        # applies its minimal built-in checks. chaski.Node points colcad at it.
        shutil.copy2(bundle, staged / "colcad" / "contracts-bundle.json")
        binary_path = bin_dir / ("colcad.exe" if goos == "windows" else "colcad")

        env = {**os.environ, "GOOS": goos, "GOARCH": goarch, "CGO_ENABLED": "0"}
        subprocess.run(
            ["go", "build", "-trimpath", "-ldflags", f"-s -w -X main.version={version}",
             "-o", str(binary_path), "./cmd/colcad"],
            cwd=REPO_ROOT, env=env, check=True,
        )
        binary_path.chmod(0o755)

        pyproject = staged / "pyproject.toml"
        content = pyproject.read_text(encoding="utf-8")
        stamped = content.replace('version = "0.0.0"', f'version = "{version}"', 1)
        if stamped == content:
            raise RuntimeError(f"{pyproject}: expected exactly one 0.0.0 version placeholder to stamp")
        pyproject.write_text(stamped, encoding="utf-8")

        subprocess.run(
            [sys.executable, "setup.py", "bdist_wheel",
             "--python-tag", "py3", "--plat-name", plat_tag,
             "--dist-dir", str(out_dir.resolve())],
            cwd=staged, check=True,
        )

    wheels = sorted(out_dir.glob(f"colcad-{version}-py3-none-{plat_tag}.whl"))
    if len(wheels) != 1:
        raise RuntimeError(f"expected exactly one wheel for {plat_tag} in {out_dir}, found {wheels}")
    return wheels[0]


def generate_bundle(out: Path) -> Path:
    """The contracts bundle for this commit, from the same generator `make bundle` runs."""
    out.parent.mkdir(parents=True, exist_ok=True)
    commit = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=REPO_ROOT,
                            capture_output=True, text=True, check=False).stdout.strip() or "unknown"
    subprocess.run(
        ["uv", "run", "--project", "contracts", "python", "contracts/scripts/generate_bundle.py", str(out), commit],
        cwd=REPO_ROOT, check=True,
    )
    return out


def build(version: str, out_dir: Path) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="colcad-bundle-") as tmp:
        bundle = generate_bundle(Path(tmp) / "contracts-bundle.json")
        built = []
        for goos, goarch, plat_tag in TARGETS:
            print(f"building colcad {version} for {goos}/{goarch} ({plat_tag})")
            wheel = build_one(goos, goarch, plat_tag, version, out_dir, bundle)
            print(f"  -> {wheel}")
            built.append(wheel)
        return built


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True, help="PEP 440 version to stamp every wheel with")
    parser.add_argument("--out", type=Path, required=True, help="wheel output directory")
    args = parser.parse_args()
    for wheel in build(args.version, args.out):
        print(wheel)


if __name__ == "__main__":
    main()
