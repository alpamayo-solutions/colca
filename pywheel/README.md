# colcad wheel

The `colcad` binary and the contracts bundle of the same commit, packaged as a
platform wheel so that `pip install "chaski[node]"` brings a node along.

The package has no logic of its own. `chaski.Node` finds the binary through
`colcad.BINARY_PATH` and the bundle through `colcad.BUNDLE_PATH`.

Wheels are built by cross-compiling from one machine:

```bash
make wheels VERSION=0.1.0     # writes build/wheels/colcad-0.1.0-py3-none-<platform>.whl
```
