# Install

## Container image

```bash
docker pull ghcr.io/alpamayo-solutions/colca:<version>
```

The image holds every binary and the contracts bundle of its release, for
linux/amd64 and linux/arm64. `colcad` is the entrypoint. The `main` tag follows
the main branch.

## Python packages

Each [release](https://github.com/alpamayo-solutions/colca/releases) carries
`colca-data-contracts` as wheel and sdist, and `colcad` wheels for Linux and
macOS. [chaski](../chaski/index.md) uses the `colcad` wheel to run a node inside
a Python process.

## From source

With Go 1.26:

```bash
make build    # every binary into bin/
```

## Checking a release

Releases come with SBOMs and signed build attestations;
[SECURITY.md](https://github.com/alpamayo-solutions/colca/blob/main/SECURITY.md#supply-chain)
shows how to verify them.
