# Changelog

## [0.13.2](https://github.com/alpamayo-solutions/colca/compare/v0.13.1...v0.13.2) (2026-09-24)


### Fixes

* **clients:** tell a command that was not sent from one not yet answered ([#67](https://github.com/alpamayo-solutions/colca/issues/67)) ([a0d7776](https://github.com/alpamayo-solutions/colca/commit/a0d777699ae2a7778fe566b3e5dedc484c07b632))

## [0.13.1](https://github.com/alpamayo-solutions/colca/compare/v0.13.0...v0.13.1) (2026-09-24)


### Fixes

* **store:** log Pebble background errors at ERROR, once per window ([#65](https://github.com/alpamayo-solutions/colca/issues/65)) ([298d11e](https://github.com/alpamayo-solutions/colca/commit/298d11e72b98a195c4ba53d55bb921689b645fb2))

## [0.13.0](https://github.com/alpamayo-solutions/colca/compare/v0.12.0...v0.13.0) (2026-09-24)


### Features

* **clock:** order consumer completion behind replicated data ([#62](https://github.com/alpamayo-solutions/colca/issues/62)) ([e5a6c7c](https://github.com/alpamayo-solutions/colca/commit/e5a6c7c296faf1047affdd4c0ab2e9c3cd0bd034))


### Fixes

* **clock:** register historian with a declared service category ([#64](https://github.com/alpamayo-solutions/colca/issues/64)) ([7d83165](https://github.com/alpamayo-solutions/colca/commit/7d83165ec8978fb808e58e833a9f90c565402185))

## [0.12.0](https://github.com/alpamayo-solutions/colca/compare/v0.11.0...v0.12.0) (2026-09-24)


### Features

* **clock:** add optional application time definitions ([#60](https://github.com/alpamayo-solutions/colca/issues/60)) ([075c748](https://github.com/alpamayo-solutions/colca/commit/075c748e112a19bb534bcc14f098f1413a98eb89))

## [0.11.0](https://github.com/alpamayo-solutions/colca/compare/v0.10.0...v0.11.0) (2026-09-24)


### Features

* **contracts:** readable names beside an alarm's acknowledged_by and silenced_by ([7d1b053](https://github.com/alpamayo-solutions/colca/commit/7d1b053a0ceac5b315774c8fee3c00e458f7d3d1))
* **historian:** publish a retained _ServiceDetails with a last will ([877fd89](https://github.com/alpamayo-solutions/colca/commit/877fd898a2b7a00975be6c97deb6ea8bbdf6c9a7))


### Fixes

* **auth:** log a token group the node does not define once, at info ([cae946e](https://github.com/alpamayo-solutions/colca/commit/cae946e9e8d071da37ef4fc8354b61d3cd1d01d9))
* historian polling and service record, alarm actor names, quiet unknown token groups ([19d6587](https://github.com/alpamayo-solutions/colca/commit/19d6587f49c347a3a69830381ae8514209418a5e))
* **historian:** follow at once only after a full page ([8013e8f](https://github.com/alpamayo-solutions/colca/commit/8013e8fd782d5f79b8379de8e5b400f31bbb58c4))
* **registry:** serialise local self-registration ([ba9d8b1](https://github.com/alpamayo-solutions/colca/commit/ba9d8b161184432fc995753bf3c60beda7799dd0))

## [0.10.0](https://github.com/alpamayo-solutions/colca/compare/v0.9.1...v0.10.0) (2026-09-24)


### Features

* **uns:** autobind applies a tag's semantic type and description ([#52](https://github.com/alpamayo-solutions/colca/issues/52)) ([b654a73](https://github.com/alpamayo-solutions/colca/commit/b654a73156490f3c4a939bea451a37c5e234623c))

## [0.9.1](https://github.com/alpamayo-solutions/colca/compare/v0.9.0...v0.9.1) (2026-09-23)


### Fixes

* **mqtt:** keep subscriptions whose packet id matches an in-flight delivery ([98ccf4d](https://github.com/alpamayo-solutions/colca/commit/98ccf4dea7b3771fcb14980635aa11542e7439e3))
* **mqtt:** keep subscriptions whose packet id matches an in-flight delivery ([a525bd5](https://github.com/alpamayo-solutions/colca/commit/a525bd5aa1e59e0459f6d067e5fdd35508808981))

## [0.9.0](https://github.com/alpamayo-solutions/colca/compare/v0.8.1...v0.9.0) (2026-09-23)


### ⚠ BREAKING CHANGES

* **auth:** auth.issuer is refused at load with the replacement spelled out; write `issuers: [{ url: <issuer> }]`. Persisted JWKS are now stored per URL, so a node upgraded while its issuer is unreachable has no cached keys until the first successful fetch.

### Features

* **auth:** accept tokens from several issuers ([2f53e43](https://github.com/alpamayo-solutions/colca/commit/2f53e4331281643988b56d0ac87071413550ed5f))
* **uns:** acknowledge alarms under their own command class ([25c3eb1](https://github.com/alpamayo-solutions/colca/commit/25c3eb17ba07d602b8a57e58b4750aa7026e474f))
* **uns:** acknowledge alarms under their own command class ([670a0ee](https://github.com/alpamayo-solutions/colca/commit/670a0eed095fc5a8538c0fb077bdc3b907000092))

## [0.8.1](https://github.com/alpamayo-solutions/colca/compare/v0.8.0...v0.8.1) (2026-09-23)


### Fixes

* **uns:** name the parent of an element the walk authors ([22d3aa9](https://github.com/alpamayo-solutions/colca/commit/22d3aa929f9c14f441f2c22283212cb07b0cc897))
* **uns:** name the parent of an element the walk authors ([9ffe06d](https://github.com/alpamayo-solutions/colca/commit/9ffe06d268b3b4bb4590406119eaa346ae13d1a9))

## [0.8.0](https://github.com/alpamayo-solutions/colca/compare/v0.7.0...v0.8.0) (2026-09-23)


### Features

* **contracts:** place annotations on an element and relate them to each other ([867eefa](https://github.com/alpamayo-solutions/colca/commit/867eefa2e1cf050f01deab4bc3baf3a3888cdd48))
* **contracts:** place annotations on an element and relate them to each other ([8f388e2](https://github.com/alpamayo-solutions/colca/commit/8f388e2e8e60386e2942a6ca1e77f5db05fe701b))


### Fixes

* **uns:** size the annotation position list without arithmetic on the signal count ([d73ca01](https://github.com/alpamayo-solutions/colca/commit/d73ca010a268d9756ed640eec63c899582ac9a81))

## [0.7.0](https://github.com/alpamayo-solutions/colca/compare/v0.6.0...v0.7.0) (2026-09-23)


### Features

* **contracts:** _Finding — what a service found, with its handling attached ([0fb678f](https://github.com/alpamayo-solutions/colca/commit/0fb678ff39768b07cb7576157d1bf96b7d638e4f))


### Fixes

* **grantsync:** one element Keycloak refuses costs that element, not every group ([a8d7e7b](https://github.com/alpamayo-solutions/colca/commit/a8d7e7b7b3798b57c41fc87a4d06e89ae501c4f1))
* **grantsync:** one element Keycloak refuses costs that element, not every group ([1ca17bc](https://github.com/alpamayo-solutions/colca/commit/1ca17bc5a26d50b5aa2f78fe597cbc0168da0fef))
* **registry:** a revoke retires the records the identity authored ([9671d16](https://github.com/alpamayo-solutions/colca/commit/9671d16c2a664a4f438ea239ea7b69ea3f95c250))
* **registry:** a revoke retires the records the identity authored ([c4a4b03](https://github.com/alpamayo-solutions/colca/commit/c4a4b03328e4a24eac9a82d802f530d45a3602f2))
* **uns:** a document's supplied version resolves, so its element can be deleted ([dd501f9](https://github.com/alpamayo-solutions/colca/commit/dd501f9a0b8234f9781881a2683ffa31295d688d))
* **uns:** a document's supplied version resolves, so its element can be deleted ([b644879](https://github.com/alpamayo-solutions/colca/commit/b64487964adfae8a16641b5b8ed910c4b01af2d0))
* **uns:** a root's children are children, so deleting it is refused ([0b06da3](https://github.com/alpamayo-solutions/colca/commit/0b06da3207bb646a8a24244dcda1618f02d22e6f))
* **uns:** a root's children are children, so deleting it is refused ([b582d85](https://github.com/alpamayo-solutions/colca/commit/b582d851dc46333c854407ae404402ffbe44c18c))
* **uns:** an upsert moves an identity instead of refusing it ([7a0370a](https://github.com/alpamayo-solutions/colca/commit/7a0370a55048b84cb80d40b53948491dcc1a40b3))
* **uns:** an upsert moves an identity instead of refusing it ([34e1947](https://github.com/alpamayo-solutions/colca/commit/34e194763614e368416b63dce9403f8bd1c8263d))

## [0.6.0](https://github.com/alpamayo-solutions/colca/compare/v0.5.0...v0.6.0) (2026-09-19)


### Features

* **uns:** operator-input constants and actor attribution on commanded writes ([#33](https://github.com/alpamayo-solutions/colca/issues/33)) ([2869873](https://github.com/alpamayo-solutions/colca/commit/2869873ae3710f9862f7ce68321545a987d404a9))

## [0.5.0](https://github.com/alpamayo-solutions/colca/compare/v0.4.0...v0.5.0) (2026-09-19)


### ⚠ BREAKING CHANGES

* **alarms:** a publisher that silenced an alarm through an alarm_acknowledgement edit must send a _CmdOperate to the alarm's evaluator.

### Features

* **alarms:** an operator's act is a command to the evaluator, not an edit ([#31](https://github.com/alpamayo-solutions/colca/issues/31)) ([c317950](https://github.com/alpamayo-solutions/colca/commit/c317950f55ae349f42942f201550cd373f1186bb))

## [0.4.0](https://github.com/alpamayo-solutions/colca/compare/v0.3.0...v0.4.0) (2026-09-19)


### Features

* **door:** Tombstone retires a record at a topic ([#29](https://github.com/alpamayo-solutions/colca/issues/29)) ([3e1fc72](https://github.com/alpamayo-solutions/colca/commit/3e1fc72a144c86b1af6ee8f04a18c83364e91322))

## [0.3.0](https://github.com/alpamayo-solutions/colca/compare/v0.2.1...v0.3.0) (2026-09-18)


### Features

* _AlarmState, the standing alarm as retained state ([#27](https://github.com/alpamayo-solutions/colca/issues/27)) ([79943d7](https://github.com/alpamayo-solutions/colca/commit/79943d7cffecb501209edb350452258817be3e30))

## [0.2.1](https://github.com/alpamayo-solutions/colca/compare/v0.2.0...v0.2.1) (2026-09-17)


### Fixes

* **standalone:** retire commands accepted before ownership transfer ([#25](https://github.com/alpamayo-solutions/colca/issues/25)) ([5f13d18](https://github.com/alpamayo-solutions/colca/commit/5f13d18411d7b5fdade131cc98fce63c35539193))

## [0.2.0](https://github.com/alpamayo-solutions/colca/compare/v0.1.9...v0.2.0) (2026-09-17)


### Features

* **fleet:** support durable offline replication and standalone handover ([#23](https://github.com/alpamayo-solutions/colca/issues/23)) ([b5b3eee](https://github.com/alpamayo-solutions/colca/commit/b5b3eee7d6b3393205b71831bbfd2742977651c6))

## [0.1.9](https://github.com/alpamayo-solutions/colca/compare/v0.1.8...v0.1.9) (2026-09-17)


### Fixes

* **ci:** publish the npm tarball by its path, not as a GitHub shorthand ([#21](https://github.com/alpamayo-solutions/colca/issues/21)) ([0167dd2](https://github.com/alpamayo-solutions/colca/commit/0167dd2803281cec73546a66b2d5f66bfa0ada93))

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
