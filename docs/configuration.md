# Configuration

`colcad` takes one argument, the path to a YAML file. Only `ulid`, `data_dir`
and `key_file` are required; every listener is off unless its address is set.

```bash
colcad /etc/colca/node.yaml
```

## A small tree

A root node with a local door for services and a replication door for
children:

```yaml
ulid: n-global
data_dir: /data
key_file: /keys/global.key
api:  { addr: ":443", local_addr: ":80", token: "change-me" }
mqtt: { addr: ":8883" }
mqtt_local: { addr: ":1883" }
repl: { addr: ":9443" }
```

An edge node below it. The parent's public key is pinned:

```yaml
ulid: n-edge1
data_dir: /data
key_file: /keys/edge1.key
api:  { addr: ":443", local_addr: ":80", token: "change-me" }
mqtt: { addr: ":8883" }
mqtt_local: { addr: ":1883" }
parent:
  url: https://global.example.com:9443
  pubkey: 3f1c…            # printed by colca-keygen for the parent's key
```

The edge is then placed and enrolled at its parent once. An identity binds to a
system element, not to a path, so the element comes first; its id is a ULID.
The child's mount is wherever that element sits, now and after any rename:

```bash
export COLCA_TOKEN=...   # api.token from the parent's config

# 1. the element the child hangs from
curl -sk -H "X-Colca-Token: $COLCA_TOKEN" -H "Content-Type: application/json" \
  -X POST https://global.example.com/publish \
  -d '{"topic":"colca/v1/_SystemElement/n-global/edge1","payload":{"id":"01J8Z3Y8S5ZC0KQ9M2F5T7W4XB","name":"edge1"}}'

# 2. the child's key, bound to that element
curl -sk -H "X-Colca-Token: $COLCA_TOKEN" -H "Content-Type: application/json" \
  -X POST https://global.example.com/enroll \
  -d '{"ulid":"n-edge1","kind":"node","element":"01J8Z3Y8S5ZC0KQ9M2F5T7W4XB","pubkey":"<edge1 public key>"}'
```

Machines and external services are enrolled the same way, with
`"kind":"external"` and the grants they need. There are no lists of children
or clients in the file: identities are runtime state, stored by the node.

## Node

| Key | Meaning |
|---|---|
| `ulid` | The node's identity. Appears in every topic the node owns, in logs and in `/healthz`. |
| `name` | Optional display name for the node's own `_Node` record. |
| `topic_root` | First segment of every topic, `colca` by default. `COLCA_TOPIC_ROOT` overrides it. All nodes of one tree must agree. See [Topics](topics.md#choosing-the-root). |
| `standalone` | `false` by default. Permanently retire fleet trust; cannot coexist with `parent`. See [handover](operations.md#permanent-standalone-handover). |
| `data_dir` | Directory of the node's database: streams, current state, cursors. |
| `secrets_dir` | Separate database for sealed secrets. Empty disables `/secrets`. Must differ from `data_dir`. |
| `key_file` | PEM file with the node's ed25519 private key, from `colca-keygen`. |
| `log_level` | `debug` for debug logging, anything else for info. |
| `addr_file` | If set, the node writes the addresses its listeners actually bound to as JSON once they are up. Useful with `:0` ports. |

## Listeners

| Key | Meaning |
|---|---|
| `api.addr` | HTTPS API for machines, administrators and people. |
| `api.token` | Admin token for `X-Colca-Token`. Empty authenticates nobody. |
| `api.local_addr` | Plaintext HTTP door for local services. Never publish it. |
| `mqtt.addr` | MQTT over TLS for enrolled machines and external services. |
| `mqtt_local.addr` | Plaintext MQTT door for local services. Never publish it. |
| `mqtt_human.tcp_addr` | MQTT over TLS for people, token in the password. |
| `mqtt_human.ws_addr` | MQTT over WebSocket for browsers. |
| `repl.addr` | Replication door for child nodes. |
| `tls.cert_file`, `tls.key_file` | Optional certificate for the HTTP API and the doors people use, for clients that expect one from a CA. Replication and the machine door always present the node's own key. |
| `parent.url` | `https://host:port` of the parent's replication door. Absent means this node is a root. |
| `parent.pubkey` | Hex public key of the parent, checked on every connection. |

## People

| Key | Default | Meaning |
|---|---|---|
| `auth.issuers[].url` | | An accepted `iss` of tokens. At least one is required. |
| `auth.issuers[].jwks_url` | `auth.jwks_url` | Where this issuer's signing keys are fetched from. |
| `auth.audience` | | Expected `aud`, the same for every issuer. |
| `auth.jwks_url` | | Signing keys shared by every issuer without its own `jwks_url`. |
| `auth.jwks_refresh` | `1h` | How often keys are refreshed in the background. |

## Retention

```yaml
retention:
  interval: 5m
  streams:
    metrics:  { max_age: 336h, max_bytes: 4GiB }
    commands: { max_age: 2160h }
```

| Key | Default | Meaning |
|---|---|---|
| `retention.interval` | `5m` | How often the pruner runs. `0` turns it off; leaving the key out keeps the default. |
| `…streams.<name>.max_age` | see below | Records older than this may be pruned. Go duration syntax; there is no `d` unit. |
| `…streams.<name>.max_bytes` | none | Size limit, `KiB`/`MiB`/`GiB`/`TiB` or bytes. Applies in addition to the age. |
| `…streams.<name>.ignore_cursors_after` | `0` | After how long a cursor that stopped moving no longer protects the stream. `0` means never. |
| `…streams.<name>.keep_forever` | `false` | Never prune this stream. Cannot be combined with `max_age`. |
| `…streams.metrics.signals` | none | Per-signal windows inside the stream's own, see below. |

Default ages: `metrics` and `logs` 14 days (`336h`); `entities`, `alarms`,
`annotations` and `audit` 365 days (`8760h`); `commands` 90 days (`2160h`).
`commands` cannot be set below 7 days. A delivery cursor (a child node's
downlink cursor, a machine's delivery cursor) protects what is queued for it
beyond `max_age`; set `ignore_cursors_after` on `commands` to bound how long an
absent child keeps the queue. Commands pruned that way are answered `410`. `definitions` are compacted,
not aged.

### Per-signal retention

`signals` gives chosen metric signals a shorter window than the stream. Rules
are tried in order; the first one that selects a signal sets its window, and a
signal no rule selects keeps the stream's `max_age`.

```yaml
retention:
  streams:
    metrics:
      max_age: 72h
      signals:
        - topics: ["prekit/v1/_Metric/+/+/Diagnostics/#"]
          max_age: 6h
        - signal_ids: ["7SMDVD5TGG1H1KHMW9XGQV4TEJ"]
          max_age: 1h
        - topics: ["prekit/v1/_Metric/#"]   # everything else
          max_age: 24h
```

| Key | Meaning |
|---|---|
| `…signals[].topics` | MQTT filters on the signal's `_Metric` topic (`+` one level, `#` the rest). The topic carries the node and the path, so a rule can select by machine, by line or by signal name. |
| `…signals[].signal_ids` | Signal ids, for single signals. |
| `…signals[].max_age` | The signal's window. Must be shorter than the stream's `max_age`, which still applies to every signal. A stream with `keep_forever` cannot have `signals`. |

For each selected signal the pruner removes records older than the window but
keeps the one with the latest timestamp among them: the value in force when
the window opens. It walks a signal in offset order and stops at its first
record inside the window, so a late sample appended after that stays until the
stream's `max_age` removes it. A signal
that stopped changing keeps its last value however old it is, and `/kv` keeps
the current value as before. The cursor rule is the stream's: nothing a
protecting cursor has not read is removed, and a cursor that went stale under
`ignore_cursors_after` is passed with one `_StreamGap` marker per pass naming
it, from its position to the protecting floor. That range is sparse: the
records of other signals in it are still there. A stale cursor that acks during
the pass protects again from its next batch on.

The rules are node configuration rather than a signal attribute because how
long to keep history is a decision of the node that stores it: a hub that
historicizes keeps hours, the edge that buffers for it keeps days, for the same
signal. A rule covers thousands of signals without editing any of them.

Per-signal retention reads the signal index, so it covers records written since
the index existed on the node (colca 0.27). Older records leave with the
stream's `max_age`. Its deletes are spread across the stream rather than a
prefix, so Pebble reclaims their space as compaction reaches them.

## Storage

| Key | Default | Meaning |
|---|---|---|
| `storage.compression` | `snappy` | Block compression of the stream store: `snappy` or `zstd`. |

`zstd` stores the same records in about a quarter less space than `snappy`
(scale-test store: 10.0 MB → 7.4 MB; hub record mix: 219 → 155 bytes per
record) for more CPU in flushes and compactions and roughly twice the
decompression time on long scans. Pebble records the codec per block, so either
setting reads both: switching applies to data written from then on, and older
tables are rewritten in the new format as compaction reaches them. Switching
back is as safe.

## Cursors

| Key | Default | Meaning |
|---|---|---|
| `cursors.lag_alarm_after` | `60s` | How old the oldest unread record a consumer reads (its fetch filter applied) may get before the node writes a `cursor_lag` finding about its service. `0` writes none. |
| `cursors.stale_after` | `24h` | How long a cursor may stand still while records wait past it before the node's `stale_cursors` finding names it, read or not. `0` writes none. |

A stale cursor is only reported. To let retention pass it, set
`ignore_cursors_after` on the stream; to remove it, retire it (see
[HTTP API](http-api.md#cursors-that-nobody-reads)).

## Logs

The node gates the `_Log` records written on it, by its services and by
itself, before they are stored. Records replicated from a child are not gated
again; the child gated them.

| Key | Default | Meaning |
|---|---|---|
| `logs.window` | `60s` | The collapse and rate-cap window. `0` turns both off. At most `24h`. |
| `logs.max_per_service` | `600` | Records one service may store per window. `0` means no cap. |
| `logs.max_tracked` | `4096` | Distinct repeat keys, and separately services, held in memory. |

```yaml
logs:
  window: 60s
  max_per_service: 600
  max_tracked: 4096
```

- **Repeats collapse.** The key is the topic (service and level), the
  `logger_name` and the `message`. The first record of a key is stored at
  once. Identical records later in its window are not stored, only counted.
  When the window ends with a count, one record is written at the same topic,
  attributed to the original writer: the last withheld payload, its message
  suffixed like `connection refused (×1200 in 60 s)`, and `extra.repeated`,
  `extra.repeat_window_s`, `extra.first_repeat_at` and `extra.last_repeat_at`
  (when the node admitted the first and last repeat). A window without
  repeats writes nothing.
- **Each service has a budget.** A service is a `_Log` topic without its level
  segment. It may store `max_per_service` records per window; collapsed
  repeats and summaries do not count. Records beyond the budget are dropped
  and counted, and when the window ends one `WARNING` record at the service's
  position, written by the node (`colca`), says
  `1834 log record(s) dropped: press exceeded 600 records in 60 s`, with
  `extra.dropped`, `extra.window_s` and `extra.service`.
- **Memory is bounded.** At most `max_tracked` repeat keys and
  `max_tracked` services are held. A repeat key holds one pending payload of
  at most 64 KiB; a larger record is never collapsed, only counted against its
  service's budget. A record that finds no room is stored without being
  collapsed or capped and counted in `colca_log_untracked_total`.
- A window ends on its own timer; a node that stops writes every pending
  summary and drop notice before its store closes.
- A withheld record was accepted: MQTT answers with a successful PUBACK and
  does not fan it out, `POST /publish` answers `202` with
  `{"withheld":"collapsed"|"rate_limited"}` (see [HTTP API](http-api.md)).
  Publishers must not send it again.

The collapse only works when identical events carry identical messages, so a
publisher puts the event's own text in `message`, not a formatted line with a
timestamp (see [Topics](topics.md#log-records)).

## Limits

| Key | Default | Meaning |
|---|---|---|
| `limits.max_record_bytes` | `4MiB` | Largest payload a record may carry. |
| `limits.max_blob_bytes` | `32MiB` | Largest file attached to a `_Resource`. |
| `blob_gc.interval` | `15m` | How often unreferenced files are collected. `0` turns it off. |
| `blob_gc.grace` | `1h` | How long an unreferenced file is kept before it may go. |
| `mqtt_limits.max_clients` | `4096` | Concurrent MQTT connections. |
| `mqtt_limits.max_subscriptions_per_client` | `1024` | |
| `mqtt_limits.receive_maximum` | `1024` | MQTT 5 receive maximum announced to clients. |
| `mqtt_limits.maximum_inflight` | `65535` | Unacknowledged QoS 1/2 messages per client. |
| `mqtt_limits.max_pending_writes_per_client` | `8192` | Queued outbound packets before a slow client is dropped. |
| `mqtt_limits.max_topic_aliases_per_client` | `256` | |
| `mqtt_limits.max_session_expiry` | `168h` | Upper bound for a client's requested session expiry. |

## Time

Nodes share one clock: the root's. Each node learns its offset over the
replication link and tells its machines on `<root>/v1/_TimeSync/<node>`.

| Key | Default | Meaning |
|---|---|---|
| `time_sync.beacon_interval` | `30s` | How often the node publishes its time beacon. |
| `time_sync.hold_ms` | `10000` | Clock difference above which a machine holds commands. |
| `time_sync.drift_warn_ms` | `5000` | Clock difference above which a warning is logged. |

## Commands

| Key | Default | Meaning |
|---|---|---|
| `commands.strict` | `false` | Answer every command to this node that no service announced with a `404` `_Ack`, also at elements where nobody announces anything. Turn it on where every executor announces its commands. |

## Contracts

| Key | Default | Meaning |
|---|---|---|
| `contracts.bundle` | `/etc/colca/contracts-bundle.json` | Schema bundle to enforce. Without one, a small built-in set of checks applies. |
| `contracts.sha256` | | Expected digest of the bundle. A mismatch stops the node from starting. |

## Environment

| Variable | Used by | Meaning |
|---|---|---|
| `COLCA_TOPIC_ROOT` | every binary, `colca-data-contracts`, chaski | Topic root; overrides `topic_root`. |
| `COLCA_ADMIN_TOKEN` | tools and examples | Admin token to present to a node. |
