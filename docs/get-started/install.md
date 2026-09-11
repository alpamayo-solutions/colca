# Install

## Container image

```bash
docker pull ghcr.io/alpamayo-solutions/colca:<version>
```

The image holds every binary and the contracts bundle of its release, for
linux/amd64 and linux/arm64. `colcad` is the entrypoint. The `main` tag follows
the main branch.

## Python packages

`colca-data-contracts` and the `colcad` wheels for Linux and macOS are on PyPI
and attached to each [release](https://github.com/alpamayo-solutions/colca/releases):

```bash
pip install colca-data-contracts
pip install colcad
```

[chaski](../chaski/index.md) uses the `colcad` wheel to run a node inside a
Python process.

## From source

With Go 1.26:

```bash
make build    # every binary into bin/
```

## Checking a release

From 0.1.1 on, release images are signed with cosign. To check that an image was
built by this repository's CI from a version tag (cosign 3 or later):

```bash
cosign verify ghcr.io/alpamayo-solutions/colca:<version> \
  --certificate-identity-regexp '^https://github\.com/alpamayo-solutions/colca/\.github/workflows/ci\.yml@refs/tags/v' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

Releases also come with SBOMs and signed build attestations;
[SECURITY.md](https://github.com/alpamayo-solutions/colca/blob/main/SECURITY.md#supply-chain)
shows how to verify those.
