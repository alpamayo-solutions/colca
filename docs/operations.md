# Operating Colca

## Health

`GET /healthz` answers as soon as the node is up. `GET /metrics` serves
Prometheus metrics; scrape it over the local door so that no self-signed
certificate is involved.

Metrics describe progress as numbers, not connection states. The ones worth an
alert first:

| Metric | Alert when |
|---|---|
| `time() - colca_uplink_last_success_timestamp_seconds` | the uplink has not succeeded for longer than you can tolerate (only on nodes with a parent; it stays `0` on a root) |
| `colca_cursor_lag_records` | a consumer falls behind |
| `colca_retention_pressure` | above `1`: the pruner wants to remove more than a cursor allows, and the disk grows |
| `colca_stream_live_bytes` | a stream grows past what the disk holds |
| `colca_rejected_publishes_total` | rises: clients send what the node refuses; `reason` says why |
| `colca_auth_rejections_total` | rises: unknown keys or bad tokens at a door |
| `colca_replication_integrity_failures_total` | above `0`: a child pruned records before its parent received them |
| `colca_jwks_keys` | `0` on a node with an `auth:` block |
| `colca_contracts_bundle_info` | `source="builtin"` where you expected a bundle |

Counters reset when a node restarts. Gauges are read from the database at
scrape time and survive restarts.

## Retention

The pruner removes the oldest records of a stream in one atomic step and never
touches the current-state view, which only shrinks through empty payloads.

Every named cursor protects the stream, including the replication cursor that
forms a child's offline buffer. If the age or size limit wants to remove
records a cursor has not read, nothing happens: the stream stays as it is,
`colca_retention_pressure` goes above `1`, and a warning names the cursor.
Disk keeps growing until someone acts.

To let the pruner give up on a consumer that stopped, set
`ignore_cursors_after` for that stream. When a run removes records a cursor
had not read, it logs an error and writes one `_StreamGap` record into the
stream, which replicates up like anything else. Consumers see the gap in
`/fetch`.

Two things to know:

- A cursor that stays dead produces one `_StreamGap` per pruner run that
  removes something, not a single one.
- `_StreamGap` records are not shown on the node's own MQTT bus. Read them from
  the stream or at an ancestor.

## Moving a child node

A child that is re-parented or taken out of service is **drained** first: the
parent stops routing new commands to it and waits until every command already
queued has been delivered or has expired. Drains are visible in
`colca_drains_active` and `colca_drains_completed_total`.

## Backups

A node's state is its `data_dir`. Stop the node, or snapshot the filesystem,
and copy the directory. A restored node that is behind its parent simply
receives newer definitions and commands again; its children push what the
restored node is missing, because their replication cursors never moved past
what was acknowledged.

Keep `secrets_dir` and the key file separate from `data_dir`. Losing a
service's own keyring makes its stored secrets unreadable.

## Benchmarks

`cmd/colca-bench` runs five scenarios against a real edge and hub pair and
records the results:

```bash
make bench-scenarios            # ingest, live latency, catch-up, cardinality, footprint
make bench-check                # compares the newest results with bench/thresholds.json
make bench                      # store micro-benchmarks
```

Results are written to `bench/results/<host>.jsonl`, which is not committed.
Numbers from a laptop say little about an edge device with eMMC storage; run
the scenarios on the hardware you deploy to before setting thresholds.

## Known limitations

- **No clustering.** A node is a single writer on local disk. There is no
  failover inside a level; availability comes from the level below buffering
  while a node is gone.
- **Retained messages and the current-state view are unbounded.** They shrink
  only when paths are retired, so the number of distinct paths is the bound.
- **Without a schema bundle only minimal checks apply.** Run nodes with the
  bundle generated from `contracts/`.
- **Node administration uses one token per node** besides `admin:#` grants.
- **MochiMQTT needs dependency patches and shutdown handling.** The fork and
  the shutdown workaround have separate removal conditions, described below.

## MQTT dependency

`go.mod` declares MochiMQTT v2.7.9 but replaces it with the Alpamayo fork at
`4d586594fc32`. The fork adds three fixes:

| Fix | Why Colca needs it |
| --- | --- |
| [Retained-message scan locking](https://github.com/mochi-mqtt/server/pull/539) | Concurrent retained delivery and wildcard subscriptions otherwise race. |
| [WebSocket binding during Init](https://github.com/mochi-mqtt/server/pull/542) | A listener configured with `:0` must hold and report its actual port before serving. |
| [Flush buffered writes before closing](https://github.com/alpamayo-solutions/mochi-server/commit/4d586594fc32e38544b769f9972f10d8f3f78cd9) | A buffered PUBACK must reach the publisher before shutdown, or reconnect can store the same publish twice. |

The proposed [MochiMQTT v2.8.0](https://github.com/mochi-mqtt/server/pull/543)
includes the first two fixes. Its reviewed commit `3b7c3e6` does **not** include
the buffered-write flush. Removing the replacement requires all three fixes;
a version number alone is insufficient. Run `make test`, including the MQTT
retained-delivery, WebSocket-door, shutdown-drain and buffered-PUBACK regressions,
against any replacement, then run `make smoke` to verify the node tree.

Colca also handles limitations outside the fork:

- `mqttsrv.Server.Close` avoids MochiMQTT's recursive read lock in
  `Clients.GetByListener`. An upstream fix permits simplifying that part of
  shutdown; the admission gate, publish drain and idempotent close still serve
  Colca's own shutdown contract.
- Subscription quotas substitute a reserved denied filter because v2.7.9
  ignores per-filter reason codes returned by `OnSubscribe`. An upstream version
  that honours those codes permits returning MQTT 5's quota-exceeded code
  directly, while retaining MQTT 3's failed-SUBACK response and the audit event.
- The log wrapper names empty transport errors and suppresses repeated messages.
  Upstream naming fixes can replace the first behavior; they do not replace
  repeated-message suppression.

The shutdown-drain regression pauses a PUBACK in `OnPacketEncode`. It must not
rely on `OnQosPublish`: the proposed v2.8.0 no longer calls that hook for inbound
QoS 1 publishes.
