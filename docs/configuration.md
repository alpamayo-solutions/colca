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

Default ages: `metrics` and `logs` 14 days (`336h`); `entities`, `alarms`,
`annotations` and `audit` 365 days (`8760h`); `commands` 90 days (`2160h`).
`commands` cannot be set below 7 days, so a valid command is never pruned
before a child that was offline could fetch it. `definitions` are compacted,
not aged.

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
