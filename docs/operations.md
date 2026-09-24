# Operating Colca

## Health

`GET /healthz` answers as soon as the node is up. Its `storage.state` turns
`failing` when the database cannot flush or compact, most often because the disk
is full; `since` and `error` say when and why. It returns to `ok` after the next
successful flush. The node logs the first such error at ERROR and repeats it at
most every 30 seconds with a `repeats_suppressed` count. `GET /metrics` serves
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
| `colca_cursor_next_record_age_seconds` | age of the next retained record waiting for a consumer; unlike cursor inactivity, this is zero when caught up |
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

A configured parent's uplink cursors are persisted during startup, before
retention runs, even when the streams are empty or the parent has never been
reachable. This protects the first accepted records through an initial outage
and restart. Startup fails if this protection cannot be written. Reconnecting
advances the cursors only after successful upload; ordinary retention can then
reclaim the acknowledged history. This protection is not a disk-capacity limit:
size storage for the expected outage, ingestion rate and other local consumers.

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
- **A deadlock in mochi-mqtt 2.7.9 is avoided, not fixed.** Colca pins a fork
  and stops its clients before closing listeners; the workaround goes once the
  fix is released upstream.


## Per-signal metric replication

A `_Signal` accepts `replication_policy` with two values:

- `replicate_to_parents` (default, including older signals without the field):
  newly accepted samples are eligible for upward replication.
- `source_local_only`: newly accepted samples remain at their source node.

The decision is stored with each sample, under the same storage lock as signal
updates. Changing the policy does not cancel already queued uploads. Re-enabling
replication does not backfill samples accepted while local-only. Restarts and
later signal edits do not change these decisions. Local MQTT delivery, retained
values and historian consumers continue to read the original samples.

Signal descriptors still replicate. The uplink sends compact offset ranges for
intentionally omitted samples, without their topics, payloads or timestamps.
The parent acknowledges those ranges durably without appending metric records.
This preserves real offset-gap detection and replay deduplication; an entirely
local-only page still exchanges progress with the parent. Reads are bounded by
physical records scanned, including excluded samples.

Deploy receivers before enabling the policy at their children. Older receivers
reject the progress entries and the child holds its cursor for retry instead of
silently discarding data. Upgrade the contracts package and consumers together;
older Python signal decoders do not recognize the new field. Downgrading a source
to a binary that does not understand its persisted upload decisions is unsupported.

## Permanent standalone handover

`standalone: true` is a permanent trust transition, distinct from an offline
parent. Remove `parent` in the same configuration and restart the node. Before
opening any door the node journals the transition, retires the old parent
cursors and existing externally enrolled identities, and disables the static
administration token. Local services, node identity and process history remain.
External machine clients must be enrolled again by the new local owner.

Commands accepted before handover are retired from HTTP fetch and MQTT replay,
including pending local commands. Their stored history and acknowledgements are
preserved; new commands remain executable. The retirement boundary survives
restarts. Upgrading an existing standalone data directory from 0.2.0 records this
boundary once at the first startup, retiring any commands pending at that time.

Human authentication stays closed until a trusted local identity controller has
preserved operators, disabled fleet accounts and transferred administration. The
controller then calls `POST /standalone/complete` on the **local** HTTP door.
Completion persists a token issuance cutoff; retries and restarts retain it.
`GET /self` exposes `standalone_since` (Unix seconds) and `standalone_ready` so
other local authentication services enforce the same boundary.

Pre-handover JWTs and personal access tokens are rejected. Only locally authored
group definitions and newly created local PATs authorize subsequent access.
Cached fleet definitions remain available as data, so an identity controller can
copy the last applied operator grants into new, locally owned groups. It must
not reuse the fleet group's identity for a competing local definition.

Removing the standalone flag or restoring an old parent configuration against
this data directory refuses startup. Rejoining a fleet requires an explicit
migration and enrollment plan, including a decision about retained history; a
configuration rollback cannot export history or restore the former owner.

This broker transition does not administer an external identity provider, host
VPN, SSH access or separate update agents. The deployment's handover controller
must retire those connections and credentials before reporting the machine as
handed over. Ordinary parent outages do not trigger any of these actions.
