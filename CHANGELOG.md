# Changelog

## [0.1.8](https://github.com/alpamayo-solutions/colca/compare/v0.1.7...v0.1.8) (2026-09-17)


### Features

* **clients:** a TypeScript client for the door and live values ([#14](https://github.com/alpamayo-solutions/colca/issues/14)) ([f5f5de7](https://github.com/alpamayo-solutions/colca/commit/f5f5de7513a430e2a31ebf03e470be24f44addb1))
* **clients:** commands that wait for their acknowledgement ([#20](https://github.com/alpamayo-solutions/colca/issues/20)) ([f028f63](https://github.com/alpamayo-solutions/colca/commit/f028f636831ca5240e06ae72b58a2280e4b22763))


### Fixes

* **contracts:** type optional fields in the bundle on every Python ([f5f5de7](https://github.com/alpamayo-solutions/colca/commit/f5f5de7513a430e2a31ebf03e470be24f44addb1))

## [0.1.7](https://github.com/alpamayo-solutions/colca/compare/v0.1.6...v0.1.7) (2026-09-15)


### Features

* **contracts:** let annotation types carry metadata ([#12](https://github.com/alpamayo-solutions/colca/issues/12)) ([95e4739](https://github.com/alpamayo-solutions/colca/commit/95e4739ad79d100b5d07a3223fa466d3655679b2))

## [0.1.6](https://github.com/alpamayo-solutions/colca/compare/v0.1.5...v0.1.6) (2026-09-14)


### Fixes

* **mqttsrv:** flush PUBACKs mochi buffered before shutdown closes the connection ([#10](https://github.com/alpamayo-solutions/colca/issues/10)) ([3aa1767](https://github.com/alpamayo-solutions/colca/commit/3aa17671c931acc3460df78cea7302666acbb3d8))

## [0.1.5](https://github.com/alpamayo-solutions/colca/compare/v0.1.4...v0.1.5) (2026-09-12)


### Fixes

* **mqttsrv:** let in-flight publishes finish before shutdown disconnects clients ([#8](https://github.com/alpamayo-solutions/colca/issues/8)) ([fd0e476](https://github.com/alpamayo-solutions/colca/commit/fd0e4767c11579c374033d1e73051285dd4df099))

## [0.1.4](https://github.com/alpamayo-solutions/colca/compare/v0.1.3...v0.1.4) (2026-09-12)


### Fixes

* **mqttsrv:** answer refusals with codes a PUBACK may carry ([2541eb0](https://github.com/alpamayo-solutions/colca/commit/2541eb074ad564e84028a51b53da0b69b20e004f))

## [0.1.3](https://github.com/alpamayo-solutions/colca/compare/v0.1.2...v0.1.3) (2026-09-11)


### Fixes

* **engine:** audit a refused retained publish with its topic ([19eec5b](https://github.com/alpamayo-solutions/colca/commit/19eec5b0626beb041724a7f6dcff4363db7a5d0b))
* **mqttsrv:** drop refused publishes instead of passing them on ([1e8eaa7](https://github.com/alpamayo-solutions/colca/commit/1e8eaa7fd2549a8be5cbf23c1387d779d8d2510c))

## [0.1.2](https://github.com/alpamayo-solutions/colca/compare/v0.1.1...v0.1.2) (2026-09-11)


### Fixes

* **historian:** publish logs under the configured topic root ([8d44a42](https://github.com/alpamayo-solutions/colca/commit/8d44a42a6b79ec9f5d31b26e17d48fbdc34bda36))


### Documentation

* install the Python packages from PyPI ([00a0bb2](https://github.com/alpamayo-solutions/colca/commit/00a0bb2ecccdaafc86885fb77ada62fc24e6a09d))

## [0.1.1](https://github.com/alpamayo-solutions/colca/compare/v0.1.0...v0.1.1) (2026-09-11)


### Fixes

* **image:** sign images with cosign ([0eacd8a](https://github.com/alpamayo-solutions/colca/commit/0eacd8a65a9f06313269d48320a8b39abdf69f72))


### Documentation

* show how to verify the image signature ([9db7f9c](https://github.com/alpamayo-solutions/colca/commit/9db7f9ca6ee9fa2d399ce9e4adf96dc1c4d355ba))

## 0.1.0 (2026-09-11)

First public release. The [README](https://github.com/alpamayo-solutions/colca#readme) and the [documentation](https://alpamayo-solutions.github.io/colca/) show what Colca does and how to run it.
