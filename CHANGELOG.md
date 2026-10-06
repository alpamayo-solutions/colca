# Changelog

## [0.29.4](https://github.com/alpamayo-solutions/colca/compare/v0.29.3...v0.29.4) (2026-10-06)


### Fixes

* **uns:** autobind adopts a declared signal at PREKIT's spelling ([#164](https://github.com/alpamayo-solutions/colca/issues/164)) ([026fb1d](https://github.com/alpamayo-solutions/colca/commit/026fb1d54912c2c84f119ad59a10f5f57e3727b4))

## [0.29.3](https://github.com/alpamayo-solutions/colca/compare/v0.29.2...v0.29.3) (2026-10-06)


### Performance

* let one parent take a thousand children's telemetry ([#160](https://github.com/alpamayo-solutions/colca/issues/160)) ([fa48613](https://github.com/alpamayo-solutions/colca/commit/fa48613484ca442db8dd28f9bed58d3bdd5e4e2a))

## [0.29.2](https://github.com/alpamayo-solutions/colca/compare/v0.29.1...v0.29.2) (2026-10-05)


### Fixes

* **watch:** take a narrowed hint's next offset with its scoped positions ([#157](https://github.com/alpamayo-solutions/colca/issues/157)) ([919dd7f](https://github.com/alpamayo-solutions/colca/commit/919dd7fd28525340bae12c8b37c74f6ae54a5172))

## [0.29.1](https://github.com/alpamayo-solutions/colca/compare/v0.29.0...v0.29.1) (2026-10-05)


### Fixes

* **cursors:** start a cursor that was never acked at the oldest retained record ([#155](https://github.com/alpamayo-solutions/colca/issues/155)) ([3e3aa74](https://github.com/alpamayo-solutions/colca/commit/3e3aa74540736b11a71ef659dc7cb60668eacf18))

## [0.29.0](https://github.com/alpamayo-solutions/colca/compare/v0.28.0...v0.29.0) (2026-10-05)


### Features

* **commands:** answer fetchLogs from the node's own logs stream ([#152](https://github.com/alpamayo-solutions/colca/issues/152)) ([aefe089](https://github.com/alpamayo-solutions/colca/commit/aefe0894e64ffeb5b6cf766e2622a7777527187c))
* **logs:** collapse repeated log records and cap each service's log rate ([#153](https://github.com/alpamayo-solutions/colca/issues/153)) ([e29936c](https://github.com/alpamayo-solutions/colca/commit/e29936c753ebf2dc679e4c94de3871d23c07bb66))
* **repl:** forward logs to the parent from a minimum level ([#151](https://github.com/alpamayo-solutions/colca/issues/151)) ([1d8c16d](https://github.com/alpamayo-solutions/colca/commit/1d8c16d445a68cad5c6a9453d0b7d02a8f706a42))

## [0.28.0](https://github.com/alpamayo-solutions/colca/compare/v0.27.1...v0.28.0) (2026-10-05)


### Features

* **retention:** per-signal retention, zstd storage and repeat capped prunes ([#149](https://github.com/alpamayo-solutions/colca/issues/149)) ([f4f466e](https://github.com/alpamayo-solutions/colca/commit/f4f466e2a7ceabe4d4f217062bbea8b820298bce))

## [0.27.1](https://github.com/alpamayo-solutions/colca/compare/v0.27.0...v0.27.1) (2026-10-05)


### Fixes

* **engine:** carry a moved signal's value and a placed catalogue with the move ([#145](https://github.com/alpamayo-solutions/colca/issues/145)) ([e238971](https://github.com/alpamayo-solutions/colca/commit/e23897169df2d5676056aaed51531cafdee7cd37))
* **httpapi:** name why a publish was refused and answer 503 when it was not written ([#147](https://github.com/alpamayo-solutions/colca/issues/147)) ([931239d](https://github.com/alpamayo-solutions/colca/commit/931239d57aadc2d715e55c3018ef9aa2734a16af))
* **replication:** let a parent take hundreds of children ([#148](https://github.com/alpamayo-solutions/colca/issues/148)) ([01b562b](https://github.com/alpamayo-solutions/colca/commit/01b562b01579d623e5f3f8a8814907b362ecd14f))

## [0.27.0](https://github.com/alpamayo-solutions/colca/compare/v0.26.1...v0.27.0) (2026-10-02)


### ⚠ BREAKING CHANGES

* **contracts:** _SystemElement no longer carries external_asset_id or external_asset_id_type.

### Features

* **contracts:** decode every payload tolerantly, retire _SystemElement's external asset fields ([#140](https://github.com/alpamayo-solutions/colca/issues/140)) ([eb53db2](https://github.com/alpamayo-solutions/colca/commit/eb53db2a6071940dd909b5e57051e419a57bb656))
* **cursors:** list cursors with their last ack and flag stale ones ([#144](https://github.com/alpamayo-solutions/colca/issues/144)) ([95fff62](https://github.com/alpamayo-solutions/colca/commit/95fff62035333a0a10c8c9d00f7cd1b0c2bdfe10))


### Fixes

* **contracts:** ship the semantic tags our own data models require ([#36](https://github.com/alpamayo-solutions/colca/issues/36)) ([fd9eeb6](https://github.com/alpamayo-solutions/colca/commit/fd9eeb62f267e496f41093e0996a683a3818d698))


### Performance

* **store:** index metric records by signal so a signal filter skips the rest ([#141](https://github.com/alpamayo-solutions/colca/issues/141)) ([9d289df](https://github.com/alpamayo-solutions/colca/commit/9d289df6de66b1701bbf0a090ba88331b8c39bcf)), closes [#126](https://github.com/alpamayo-solutions/colca/issues/126)

## [0.26.1](https://github.com/alpamayo-solutions/colca/compare/v0.26.0...v0.26.1) (2026-10-02)


### Fixes

* **volume-init:** tolerate files vanishing during the chown walk ([#138](https://github.com/alpamayo-solutions/colca/issues/138)) ([06cd4e6](https://github.com/alpamayo-solutions/colca/commit/06cd4e66e13658dab851132add1cadf16e5061ec))

## [0.26.0](https://github.com/alpamayo-solutions/colca/compare/v0.25.0...v0.26.0) (2026-10-01)


### Features

* **authz:** grants relative to the node a person signs in at ([#135](https://github.com/alpamayo-solutions/colca/issues/135)) ([243effa](https://github.com/alpamayo-solutions/colca/commit/243effaccff3b019b8a4f84444b56c5da4ab5399))

## [0.25.0](https://github.com/alpamayo-solutions/colca/compare/v0.24.1...v0.25.0) (2026-09-30)


### ⚠ BREAKING CHANGES

* **uns:** a _Metric for a signal bound to a data tag is refused unless it comes from the service whose catalogue holds that tag. Services that publish values for signals they do not produce (test probes, bridges, scripts writing over a connector's signals) now get 403 / PUBACK 0x87 and must either unbind the signal, publish their own catalogue and bind to it, or use the admin token.

### Fixes

* **uns:** accept a bound signal's _Metric only from its producer ([#133](https://github.com/alpamayo-solutions/colca/issues/133)) ([9928492](https://github.com/alpamayo-solutions/colca/commit/99284923e859b7a66b491407a5e80a7ef4acc8e5))

## [0.24.1](https://github.com/alpamayo-solutions/colca/compare/v0.24.0...v0.24.1) (2026-09-30)


### Fixes

* **uns:** authorize edit external references at their source entity ([#130](https://github.com/alpamayo-solutions/colca/issues/130)) ([82a3309](https://github.com/alpamayo-solutions/colca/commit/82a33097b7940f586a0b5df1e85dc5306781f164))

## [0.24.0](https://github.com/alpamayo-solutions/colca/compare/v0.23.1...v0.24.0) (2026-09-30)


### Features

* **commands:** commands wait for an offline child node without a lifetime cap ([#129](https://github.com/alpamayo-solutions/colca/issues/129)) ([c5cbbb4](https://github.com/alpamayo-solutions/colca/commit/c5cbbb421cef7174a08b00849934c77a1f4db3d5))

## [0.23.1](https://github.com/alpamayo-solutions/colca/compare/v0.23.0...v0.23.1) (2026-09-30)


### Fixes

* **fetch:** bound a filtered scan and skip the payload decode ([#127](https://github.com/alpamayo-solutions/colca/issues/127)) ([8a3c9ab](https://github.com/alpamayo-solutions/colca/commit/8a3c9ab6be2bdf3177d7db8f2a0575c453ba8823)), closes [#125](https://github.com/alpamayo-solutions/colca/issues/125)

## [0.23.0](https://github.com/alpamayo-solutions/colca/compare/v0.22.2...v0.23.0) (2026-09-30)


### Features

* **uns:** compare and set a single metadata key through _CmdEdit ([#123](https://github.com/alpamayo-solutions/colca/issues/123)) ([bfcc115](https://github.com/alpamayo-solutions/colca/commit/bfcc1155fb9b5d1aa75c81c81913376610ca0aa0))

## [0.22.2](https://github.com/alpamayo-solutions/colca/compare/v0.22.1...v0.22.2) (2026-09-30)


### Fixes

* **historian:** skip samples of signals marked is_logged false ([#121](https://github.com/alpamayo-solutions/colca/issues/121)) ([456d9b2](https://github.com/alpamayo-solutions/colca/commit/456d9b25c61c24167e3c496d9216fa11786fe6d5))

## [0.22.1](https://github.com/alpamayo-solutions/colca/compare/v0.22.0...v0.22.1) (2026-09-29)


### Fixes

* **mqtt:** retained messages no longer age out after a day ([#119](https://github.com/alpamayo-solutions/colca/issues/119)) ([3f605ac](https://github.com/alpamayo-solutions/colca/commit/3f605ace93836404f9f46a9f6504a123c7eaae7d))

## [0.22.0](https://github.com/alpamayo-solutions/colca/compare/v0.21.0...v0.22.0) (2026-09-28)


### Features

* **registry:** rebuilt children replicate again, decommissioned children leave no ghosts ([#117](https://github.com/alpamayo-solutions/colca/issues/117)) ([d9ac44e](https://github.com/alpamayo-solutions/colca/commit/d9ac44ebf9c94192bb3ba456826efc296ca77a3d))

## [0.21.0](https://github.com/alpamayo-solutions/colca/compare/v0.20.0...v0.21.0) (2026-09-28)


### Features

* **historian:** import immutable offline metric archives ([#115](https://github.com/alpamayo-solutions/colca/issues/115)) ([2c39233](https://github.com/alpamayo-solutions/colca/commit/2c3923348aa9a21bc30ab40be1b31e7deaad163d))

## [0.20.0](https://github.com/alpamayo-solutions/colca/compare/v0.19.2...v0.20.0) (2026-09-27)


### ⚠ BREAKING CHANGES

* drive consumers and coordination from durable change events

### Features

* drive consumers and coordination from durable change events ([5c2019e](https://github.com/alpamayo-solutions/colca/commit/5c2019ef544298d19d943c56d8bffd4a435894f9))


### Fixes

* **mqtt:** reject late connection admission during shutdown ([#114](https://github.com/alpamayo-solutions/colca/issues/114)) ([1f7744f](https://github.com/alpamayo-solutions/colca/commit/1f7744f051b1396e6cea373e930eb4d66e424be6))
* satisfy consumer lint and batching conformance checks ([678c98d](https://github.com/alpamayo-solutions/colca/commit/678c98d8bd5ead6e4d37f35ba9542a0b03063a98))

## [0.19.2](https://github.com/alpamayo-solutions/colca/compare/v0.19.1...v0.19.2) (2026-09-27)


### Fixes

* **engine:** refuse payloads nested deeper than 32 levels ([#110](https://github.com/alpamayo-solutions/colca/issues/110)) ([fc6e8bf](https://github.com/alpamayo-solutions/colca/commit/fc6e8bf407087afa416b1af2c8eaba60e53e1e3a))

## [0.19.1](https://github.com/alpamayo-solutions/colca/compare/v0.19.0...v0.19.1) (2026-09-27)


### Fixes

* **engine:** answer a command no grant covers with a 403 ack ([#108](https://github.com/alpamayo-solutions/colca/issues/108)) ([42e6a3d](https://github.com/alpamayo-solutions/colca/commit/42e6a3dbe47ddd40cebb8535940a84a24202696a))

## [0.19.0](https://github.com/alpamayo-solutions/colca/compare/v0.18.3...v0.19.0) (2026-09-26)


### Features

* cursor watchdog instead of timed catch-up, historian wakes on /watch, POST /publish/batch ([#106](https://github.com/alpamayo-solutions/colca/issues/106)) ([3802225](https://github.com/alpamayo-solutions/colca/commit/38022255eb040a5cea97c21118816a1902ce6efc))

## [0.18.3](https://github.com/alpamayo-solutions/colca/compare/v0.18.2...v0.18.3) (2026-09-26)


### Fixes

* **historian:** store a null sample as a retraction row ([#104](https://github.com/alpamayo-solutions/colca/issues/104)) ([309bf30](https://github.com/alpamayo-solutions/colca/commit/309bf3085137af87759ee519aaa709357ebc793d))

## [0.18.2](https://github.com/alpamayo-solutions/colca/compare/v0.18.1...v0.18.2) (2026-09-26)


### Features

* MQTT delivery counters, limited-by-caller series at 0, /fetch read-ahead, commands.strict ([#102](https://github.com/alpamayo-solutions/colca/issues/102)) ([cc08463](https://github.com/alpamayo-solutions/colca/commit/cc0846360b31e483dd8b89cc4134626a847d92e4))

## [0.18.1](https://github.com/alpamayo-solutions/colca/compare/v0.18.0...v0.18.1) (2026-09-26)


### Features

* kv depth and folders, fetch contract filter in the TypeScript client ([#100](https://github.com/alpamayo-solutions/colca/issues/100)) ([467833e](https://github.com/alpamayo-solutions/colca/commit/467833ee1f36f767dd42f46afccd3b0cfbd813e4))

## [0.18.0](https://github.com/alpamayo-solutions/colca/compare/v0.17.7...v0.18.0) (2026-09-26)


### Features

* per-caller HTTP read metrics ([#99](https://github.com/alpamayo-solutions/colca/issues/99)) ([c9f07bb](https://github.com/alpamayo-solutions/colca/commit/c9f07bb5d3751ed22d4316433100831e394eefb0))
* stream watch, KV index by contract, and answers for commands nobody executes ([#97](https://github.com/alpamayo-solutions/colca/issues/97)) ([9a52a04](https://github.com/alpamayo-solutions/colca/commit/9a52a041f7e3aeb52537e95913d7b47006980329))

## [0.17.7](https://github.com/alpamayo-solutions/colca/compare/v0.17.6...v0.17.7) (2026-09-26)


### Fixes

* a login right after the identity provider comes up is no longer refused ([cbd7514](https://github.com/alpamayo-solutions/colca/commit/cbd75147f2e6531fcee3f7ccecddc49a7dea666f))
* a login right after the identity provider comes up is no longer refused ([4d95457](https://github.com/alpamayo-solutions/colca/commit/4d9545798fc6a7ee2d88814daf02d22ef556c285))

## [0.17.6](https://github.com/alpamayo-solutions/colca/compare/v0.17.5...v0.17.6) (2026-09-26)


### Fixes

* refused publishes log one short line; a reused correlation id gets a 422 ack ([#93](https://github.com/alpamayo-solutions/colca/issues/93)) ([65bad1f](https://github.com/alpamayo-solutions/colca/commit/65bad1f48d40cd4b1e542d4dc5c9cc0e9a83296b))

## [0.17.5](https://github.com/alpamayo-solutions/colca/compare/v0.17.4...v0.17.5) (2026-09-26)


### Fixes

* acks only to their sender, a correlation id runs once, short refusal logs ([#91](https://github.com/alpamayo-solutions/colca/issues/91)) ([f04ee0a](https://github.com/alpamayo-solutions/colca/commit/f04ee0ae3f8817b6e5a6442c29f3ed905b17a4d8))

## [0.17.4](https://github.com/alpamayo-solutions/colca/compare/v0.17.3...v0.17.4) (2026-09-26)


### Fixes

* refuse topics MQTT cannot carry; refetch the JWKS sooner on an unknown kid ([#89](https://github.com/alpamayo-solutions/colca/issues/89)) ([e931446](https://github.com/alpamayo-solutions/colca/commit/e93144612e4b8f5f8f40d576c22621f2842e3636))

## [0.17.3](https://github.com/alpamayo-solutions/colca/compare/v0.17.2...v0.17.3) (2026-09-25)


### Fixes

* **mqtt:** guard the takeover test hook's held client with a mutex ([#87](https://github.com/alpamayo-solutions/colca/issues/87)) ([dafd45e](https://github.com/alpamayo-solutions/colca/commit/dafd45ebe08b270d1177a45aabbc25e0a12ca01e))

## [0.17.2](https://github.com/alpamayo-solutions/colca/compare/v0.17.1...v0.17.2) (2026-09-25)


### Fixes

* **historian:** announce the release version in its service details ([#85](https://github.com/alpamayo-solutions/colca/issues/85)) ([3295c7b](https://github.com/alpamayo-solutions/colca/commit/3295c7b08861690079956674ac25e0d92ac44ce2))

## [0.17.1](https://github.com/alpamayo-solutions/colca/compare/v0.17.0...v0.17.1) (2026-09-25)


### Fixes

* **mqtt:** a replaced connection's last will no longer overwrites the new connection's state ([#83](https://github.com/alpamayo-solutions/colca/issues/83)) ([4050252](https://github.com/alpamayo-solutions/colca/commit/4050252abd23adc451c5aec3478bf55268ba822d))

## [0.17.0](https://github.com/alpamayo-solutions/colca/compare/v0.16.0...v0.17.0) (2026-09-25)


### Features

* **contracts:** catalog product and recipe records (batch 2026-09-25b) ([#81](https://github.com/alpamayo-solutions/colca/issues/81)) ([c8546c1](https://github.com/alpamayo-solutions/colca/commit/c8546c1d4cfd309f0679fd60fa405eddbcabb4a0))

## [0.16.0](https://github.com/alpamayo-solutions/colca/compare/v0.15.0...v0.16.0) (2026-09-25)


### Features

* **contracts:** _AlarmSilence per element and alarm type, retention on findings and alarms ([#79](https://github.com/alpamayo-solutions/colca/issues/79)) ([ca80f52](https://github.com/alpamayo-solutions/colca/commit/ca80f52e3c8207bf2d4fa217c47f54013c9a190e))

## [0.15.0](https://github.com/alpamayo-solutions/colca/compare/v0.14.1...v0.15.0) (2026-09-25)


### Features

* **historian:** declare metadata.app_class core in _ServiceDetails ([#76](https://github.com/alpamayo-solutions/colca/issues/76)) ([4e20f3e](https://github.com/alpamayo-solutions/colca/commit/4e20f3ef199de25edffa29db2961bc13506d96b8))


### Fixes

* **mqtt:** back-channel logout ends a session that took over its client id ([#78](https://github.com/alpamayo-solutions/colca/issues/78)) ([38aad7c](https://github.com/alpamayo-solutions/colca/commit/38aad7c39db3b9906c08c6267fdefd1cb0a3c901))

## [0.14.1](https://github.com/alpamayo-solutions/colca/compare/v0.14.0...v0.14.1) (2026-09-25)


### Fixes

* **clock:** allow mounted local workers to read coordination records ([#74](https://github.com/alpamayo-solutions/colca/issues/74)) ([d46d81c](https://github.com/alpamayo-solutions/colca/commit/d46d81c2be6352c5c2bdc551bbbec2cf43a633e8))

## [0.14.0](https://github.com/alpamayo-solutions/colca/compare/v0.13.2...v0.14.0) (2026-09-25)


### Features

* **auth:** renew a person's token on the open connection, end sessions on logout ([#73](https://github.com/alpamayo-solutions/colca/issues/73)) ([3e3f6fa](https://github.com/alpamayo-solutions/colca/commit/3e3f6fadf043dc693e61e5e9d4b4c1c88aa66df7))
* **uns:** write a constant only while it holds the writer's expected value ([#72](https://github.com/alpamayo-solutions/colca/issues/72)) ([c1f8aec](https://github.com/alpamayo-solutions/colca/commit/c1f8aeca66aa986d4fae784d96550b8dad059263))


### Fixes

* **clients:** end a renewed live connection with a DISCONNECT ([#70](https://github.com/alpamayo-solutions/colca/issues/70)) ([7e49170](https://github.com/alpamayo-solutions/colca/commit/7e491706641cff536b4356718103d47dab774c61))
* **historian:** report its status in _ServiceDetails ([#69](https://github.com/alpamayo-solutions/colca/issues/69)) ([0bb38ea](https://github.com/alpamayo-solutions/colca/commit/0bb38ea96a581c397957b0c3eb95c8af011f40ab))

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
