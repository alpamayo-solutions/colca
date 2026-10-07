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
| `colca_cursor_unread_age_seconds` | age of the oldest record waiting that the consumer actually reads (its fetch filter applied); `0` on an idle stream. Past `cursors.lag_alarm_after` (default `60s`, `0` writes no finding) the node also writes a `cursor_lag` finding about the service |
| `colca_rejected_publishes_total` | rises: clients send what the node refuses; `reason` says why |
| `colca_auth_rejections_total` | rises: unknown keys or bad tokens at a door |
| `colca_replication_integrity_failures_total` | above `0`: a child pruned records before its parent received them |
| `colca_jwks_keys` | `0` on a node with an `auth:` block |
| `colca_contracts_bundle_info` | `source="builtin"` where you expected a bundle |

Counters reset when a node restarts. Gauges are read from the database at
scrape time and survive restarts.

### Who spends the read budgets

The door limits each caller to 5 `/kv` and 25 `/fetch` requests a second. These
say who uses them:

| Metric | Labels | Shows |
|---|---|---|
| `colca_http_kv_requests_total` | `caller`, `contract` (one contract, `multiple` or `all`), `prefix_depth` (`0` is the whole node, up to `5+`) | whole-node reads (`contract="all",prefix_depth="0"`) are the ones to remove first |
| `colca_http_kv_entries` | `caller` | histogram of entries per `/kv` page |
| `colca_http_fetch_requests_total` | `caller`, `stream` | a follower polling an idle stream shows a steady rate; `/watch` removes it |
| `colca_http_request_limited_by_caller_total` | `route` (the route pattern, such as `GET /kv`), `caller` | 429s per caller; `colca_http_request_limited_total` has them by door and class |

A caller's `colca_http_request_limited_by_caller_total` series exists at `0`
from its first request on a route, so "never limited" reads as `0`, not as a
missing series.

`caller` is a registered identity as `kind:name` (`local:dataops-line`), every
person as `human`, and the admin token as `admin`. No label carries a path.

### The replication door's limits

A parent limits each enrolled child by its node identity, which the TLS
handshake proves before anything else is read: 100 requests a second, two
pushes and two downlink polls at a time. Children behind one address, such as
a site router or a carrier NAT, do not share a budget. A caller whose
certificate is not an enrolled node's is limited by source address: 100
requests a second and 64 at a time. A parent holds at most 1024 pushes and 8192
downlink polls at once. `colca_http_request_limited_total{door="repl"}` counts
refusals by class: `node`, `auth` (by address), `replication` (pushes),
`downlink` and `transfer` (files).

A parent commits the pushes that arrive while a commit is being synced
together in the next one, up to 4096 records a commit, so its children are
not limited to one sync each. A push is answered once its records are on disk.

A parent that falls behind sheds load instead of buffering it: while 16,384
replicated records wait for its store, or its pushes in progress hold a 32nd
of its memory limit in request bytes, a new push is answered `429` with
`Retry-After: 1`, counted as `replication_backlog` or `replication_bytes`. The
child keeps the batch, waits as long as the parent asked (with jitter, so its
siblings do not return in step) and sends it again; nothing is dropped, and
the uplink logs a busy parent as a warning, not as a refusal.

colcad and colca-historian set Go's memory limit to 75 % of their cgroup's
memory ceiling (cgroup v1 or v2, the process's own cgroup and the smallest
limit on its ancestors) unless `GOMEMLIMIT` is set, so the collector works
harder near the ceiling instead of letting the kernel kill the process. They
log a warning when they find no ceiling. The store's memtables and block
cache are sized from the same ceiling, a 32nd of it each, between Pebble's
defaults (4 MiB and 8 MiB, also used when no ceiling is found) and 256 MiB: a
512 MiB edge gets 16 MiB of each, a 2 GiB hub 64 MiB, an 8 GiB hub 256 MiB.
Raising a hub's memory ceiling therefore also gives its store larger
memtables, which compact less under heavy ingest.

Replicated records reach the local MQTT bus through one goroutine after they
are durable. Each child's records, and so each topic's, arrive in order;
records of different children may arrive in another order than the store
holds them. Records waiting for the bus count against the push budget above.

### What the broker delivers

| Metric | Labels | Shows |
|---|---|---|
| `colca_mqtt_delivered_messages_total` | `door` (`mqtt`, `local`, `human`) | PUBLISH packets written to subscribers; with `colca_ingest_records_total` it gives the node's MQTT in and out |
| `colca_mqtt_delivered_payload_bytes_total` | `door` | their payload bytes |
| `colca_mqtt_publish_dropped_total` | | publishes dropped because a subscriber's queue was full |

### What the log gate holds back

| Metric | Labels | Shows |
|---|---|---|
| `colca_log_withheld_total` | `reason` (`collapsed`, `rate_limited`) | `_Log` records accepted but not stored: repeats counted in a summary, records over a service's budget counted in a drop notice |
| `colca_log_untracked_total` | `table` (`repeats`, `services`) | records stored unremembered because `logs.max_tracked` was reached; non-zero means raise it |
| `colca_log_gate_write_failures_total` | `kind` (`summary`, `drop_notice`) | summaries and drop notices the store did not take |

See [Configuration](configuration.md#logs).


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

Each cycle repeats a stream's prune while its scan stops at the record cap and
still removes records, for up to 30 s per stream, so a hub that appends more
than one capped scan per interval does not fall behind.

### Large hubs

A hub that historicizes metrics into its own historian does not need weeks of
broker history; the historian's cursor protects whatever it has not stored yet.
At the 2026-10 scale test the hub took about 140 MB of stream records per
machine-day (metrics 101 MB, logs 20 MB, annotations 16 MB, entities 4 MB),
about 1.7 TB at 1000 machines over the default 14 days, and annotations stay a
year by default.

```yaml
storage:
  compression: zstd          # about a quarter smaller at rest
retention:
  streams:
    metrics:
      max_age: 72h           # 1–3 days; the historian holds the history
      signals:               # optional: shorter for chatty signals
        - topics: ["prekit/v1/_Metric/+/+/Diagnostics/#"]
          max_age: 12h
    logs:        { max_age: 48h }
    annotations: { max_age: 336h }   # once the projector has read them
    audit:       { max_age: 2160h }
```

Every stream is pruned only below the cursors that read it, so an annotation
leaves once every consumer with a cursor on `annotations` (the projector, a
parent's uplink) has read it. `annotations` defaults to a year because
`rebuild_projection` replays the stream: a shorter `max_age` is also how far
back a rebuild can restore. `entities` are current state, kept in `/kv`
whatever the stream holds; colca compacts only `definitions` to the latest
record per topic, because a consumer that archives entity history reads every
entity record, so for `entities` bound the age instead.

A cursor that stood still for `cursors.stale_after` (default 24 h) while its
stream grew is named in the node's `stale_cursors` finding. List the cursors
with `GET /backlog` (admin token, no prefix) and retire the ones a removed
consumer left with `POST /ack {"delete":true}`; see
[HTTP API](http-api.md#cursors-that-nobody-reads).

To let the pruner give up on a consumer that stopped, set
`ignore_cursors_after` for that stream. When a run removes records a cursor
had not read, it logs an error and writes one `_StreamGap` record into the
stream, which replicates up like anything else. Consumers see the gap in
`/fetch`.

Things to know:

- A cursor that stays dead produces one `_StreamGap` per pruner run that
  removes something, not a single one.
- `_StreamGap` records are not shown on the node's own MQTT bus. Read them from
  the stream or at an ancestor.
- On the `commands` stream, every live command removed before the child node
  or machine whose delivery cursor held it received it gets a `410` `_Ack` in
  the same write, at the command's position, and counts in
  `colca_command_dropped_total{reason="pruned"}`. Like the gap marker, these
  answers are read from the stream, not the bus.

## Moving a child node

A child that is re-parented or taken out of service is **drained** first: the
parent stops routing new commands to it and waits until every command already
queued has been delivered or has expired. A command without `expires_at`
keeps the drain open until it is delivered. Drains are visible in
`colca_drains_active` and `colca_drains_completed_total`.

A completed drain retires the child: the parent tombstones every current-state
record the child and the nodes below it replicated (its elements, signals,
last metric values, services), and the tombstones travel up, so no ancestor
keeps showing the old node as live. To take a child out immediately, without
draining, use `DELETE /enroll/{ulid}?retire=true`; every command still queued
for it is answered with a `410` `_Ack`. A plain `DELETE` only
revokes: the child's replicated state stays, for a child that will be enrolled
again and resume where it stopped.

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

The `fanout` scenario measures one parent against many children, each pushing
a few records a second and holding its downlink poll open. The parent runs as a
separate process with a pprof listener, so its CPU, mutex and block profiles
can be saved:

```bash
make bench-replication          # 200 protocol children; CI gates on bench/replication-thresholds.json
bin/colca-bench fanout --children 100 --colcad bin/colcad --child-dir /Volumes/ram \
  --profile-dir prof/            # colcad children, profiles of the parent
```

The `fleet` scenario drives a parent that is already running, such as a hub
deployed with its historian and Timescale, with protocol children that each
emulate an edge: a connector scanning `--signals` every `--scan` and reporting
the `--change` share of them, one push per scan. It prints one JSON line per
`--interval`: samples generated and accepted, pushes, refusals, push latency
and the backlog the children hold. Everything else (the parent's CPU, the
historian's lag) is measured outside:

```bash
bin/colca-bench fleet --hub-api hub:443 --hub-repl hub:9443 --hub-pubkey HEX \
  --children 1000 --signals 100 --change 0.1 --duration 5m
```

`--protocol-children` runs children that speak only the replication protocol,
in the bench process: a host runs a thousand of them. With colcad children
every child keeps and syncs its own store; on one machine they share one disk,
so put `--child-dir` on a RAM disk or the children saturate the disk before the
parent is measured. `fanout_hop_p95_ms` counts from the moment a child has
stored a record to its arrival on the parent's bus.

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

## How the historian writes

`colca-historian` follows the `metrics` stream in pages of `FETCH_MAX` records
(default 5000, the most `/fetch` returns). Up to `PIPELINE_PAGES` pages
(default 4) are between fetch and marker at once:

- one goroutine fetches pages in stream order; several decode them;
- each page's rows go to `WRITERS` writers (default 4) by signal, so a signal
  always lands on the same writer and its rows are written in stream order
  (retractions depend on it);
- a writer does not wait for the other writers' share of a page before it
  starts on the next page, and writes what queued meanwhile in one
  transaction, as one `INSERT … SELECT FROM unnest(…) ON CONFLICT` per run of
  values;
- each writer's transaction also moves that writer's own marker
  (`historian:metrics/<writers>.<n>` in `colca_applied_offset`);
- the applied-offset marker (`historian:metrics`) moves, in a transaction of
  its own, to the end of the newest page that is written completely with every
  page before it, and the `/ack` follows it.

A restart reads from the cursor. Records at or below the marker are skipped,
and so are a writer's records at or below its own marker, so nothing is
written twice. A write whose answer was lost is checked against the writer's
marker before it is retried. Re-applying a page is not harmless in general: a
retraction is stored only when the row before it holds a value, and a late
sample (an older timestamp arriving after the retraction) changes that answer.
The writer markers depend on `WRITERS`. After `WRITERS` changes (or between a
pipelined run and `PIPELINE_PAGES=1`), a start skips to the smallest marker of
the previous writer count when all of its writers have one: every row at or
below it is written. Rows between that point and the previous writers' own
markers are written again, at most `PIPELINE_PAGES` pages; that is the only
case where the late-sample caveat above can still apply, and only after an
unclean stop. A failed write is retried in place with backoff; the pages
behind it wait in bounded queues.

When colcad's data volume is recreated while Timescale keeps the markers, the
first page of the new stream starts at offset 1 (or ends below the marker).
The historian then logs a warning, zeroes every writer marker in one
transaction before it writes a row of the new stream, and historises it from
its first record. A running historian finds it too: at the head, and after a
failed fetch, it waits until the pages in flight are marked and reads the
next page from the cursor again, which a new stream resets. Reading from its
own position instead would only return empty pages there.
`DB_MAX_CONNS` defaults to `WRITERS + 1`. `PIPELINE_PAGES=1` writes one page
at a time, with the marker in the same transaction when `WRITERS=1`. A drain
that reached the head the node announced on `/watch` waits for the next hint
without a final empty fetch.

On the fleet scale benchmark, round 4 (2026-10, PREKIT's Timescale image on
an amd64 VM, hub stack pinned to 6-10 cores, a 13.4 M-record backlog):

| | one page at a time | `PIPELINE_PAGES=4` |
|---|---|---|
| catch-up, 6 / 8 / 10 cores | 95 / 97 / 98 k rows/s | 125 / 128 / 127 k rows/s |
| live, 112 k records/s offered, 8 / 10 cores | 82 / 84 k rows/s, falling behind | 105 / 107 k rows/s, keeping up |

Eight pages instead of four, or eight writers instead of five, changed
nothing. At ~127 k rows/s the historian makes 25 `/fetch` requests a second,
the per-caller limit, and Postgres spends ~25 µs of CPU per row with most of
its writers busy (wait events: 77 % CPU, 10 % `WALWrite`); 20 000-record pages
(an experiment, `/fetch` allows 5000) gave only 130-134 k.

## Signals the historian does not store

`colca-historian` writes no rows for samples of a signal whose `_Signal`
definition says `"is_logged": false`. A signal without the field, a sample whose
signal has no definition on the node, and an undecodable definition are all
stored: leaving history out needs an explicit `false`.

The historian loads the `_Signal` records once from `/kv` and then follows the
entities stream with its own cursor, `c/historian/signals`, woken by `/watch`.
The flag in force when a sample is ingested decides; a later change of the
flag does not rewrite or backfill history. Skipped samples are consumed: the
metrics marker and cursor move past them like past written rows. On startup no
sample is read before the definitions are loaded.

`is_logged` only affects this historian. It does not change what the node
stores, replicates (that is `replication_policy`, above) or delivers over MQTT.

`/metrics` reports `colca_historian_samples_not_logged_total` (samples consumed
without a row) and `colca_historian_signals_not_logged` (signals currently
marked). `/healthz` fails with `signal_definitions` while the definitions
cannot be loaded or followed.

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
# Importing historical measurements

`colca-historian import` is an operator tool for archived measurements. It uses
the historian's `DATABASE_URL` and ordinary sink; no broker connection is opened.
It does not update retained live values, dispatch events to control consumers,
or advance the running historian's stream cursor. Applications should resolve
their existing signal identities through their normal public API before export.

Input is JSONL, one `{"topic": "colca/v1/_Metric/NODE/PATH", "payload": {...}}`
per line. Each payload must have a signal ULID, explicit timestamp in Unix
seconds, and a value. The topic's node must be a ULID and agree with any payload
node identity. Use `COLCA_TOPIC_ROOT` for a deployment with another root.

```sh
colca-historian import --file archive.jsonl --sha256 EXPECTED_SHA256 \
  --before 2026-01-01T00:00:00Z --dry-run
# Inside the historian's trusted deployment environment:
colca-historian import --file archive.jsonl --sha256 EXPECTED_SHA256 \
  --before 2026-01-01T00:00:00Z
```

The complete input is validated and hashed before database access. A private
temporary spool prevents input changes between validation and writing. Imports
use strict transactions of up to 1000 rows by default (`--batch-size`, maximum
5000), with a separate `historian:import:SHA256` marker. Rerunning the identical
file resumes committed batches. Errors stop the import without acknowledging
the failed batch. This has the normal sink's upsert semantics: the same signal
and timestamp replaces that historical point. Choose a ceiling before existing
live data and check for overlapping records when replacement is unintended.
The ordinary historian schema must already exist; import does not change
schema or retention. Ensure the configured retention covers the imported dates.
