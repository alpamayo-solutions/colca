# HTTP API

JSON in, JSON out. The API door speaks HTTPS with the node's self-signed
certificate; the local door speaks plain HTTP inside the deployment network.

A caller is one of:

- a **machine or external service**, identified by its client certificate;
  reads are limited to its grants and cursor names are prefixed with its id,
- a **person**, with `Authorization: Bearer <token>`; reads are limited to
  `read:` grants, and the only thing a person publishes is a command,
- a **local service** on the local door, named by `X-Colca-Service`,
- the **administrator**, with `X-Colca-Token: <api.token>`.

## Reading and writing

| Method and path | Who | Request | Response |
|---|---|---|---|
| `GET /healthz` | anyone | | `{"ok":true,"ulid":"…","storage":{"state":"ok"}}` |
| `GET /metrics` | anyone | | Prometheus text |
| `POST /publish` | machine, service, person (commands), admin | `{"topic":"…","payload":{…}}` | `{"stream":"…","offset":N,"topic":"…"}`, or `202` `{"stream":"logs","topic":"…","withheld":"…"}` without `offset` |
| `POST /publish/batch` | machine, service | `{"records":[{"topic":"…","payload":{…}},…]}` (1–5000 records, 16 MiB) | `{"accepted":N,"results":[{"stream":"…","offset":N} or {"stream":"logs","withheld":"…"} or {"error":"…","reason":"…"},…]}` |
| `GET /fetch` | machine, service, person, admin | `?stream=S&cursor=NAME&max=100&prefix=P&contract=_Annotation&from=N` | `{"records":[{"offset":N,"topic":"…","payload":{…},"ts":T}],"next":N,"from":N,"store":"ID"}` |
| `GET /watch` | machine, service, person, admin | `?stream=S&stream=S2&interval_ms=100` | NDJSON, one line per change: `{"streams":["S"],"next":{"S":N}}` |
| `POST /ack` | owner of the cursor, admin; to retire a stale cursor also `cmd:<node>/#:admin` | `{"cursor":"NAME","stream":"S","offset":N}` (optionally `"store":"ID"`), or `{"cursor":"NAME","stream":"S","delete":true}` to retire it | `{"moved":true}` or `{"deleted":true}`; `409` store changed, `422` past the head |
| `GET /backlog` | local service (`?prefix=` required), admin | `?prefix=c/projector/` | `{"queues":[{"cursor":"…","stream":"S","position":N,"head":N,"lag_records":N,"last_ack_ms":T,"stale":false,"read_since_start":true}]}` |
| `GET /kv` | machine, service, person, admin | `?prefix=P&max=1000&after=TOKEN&contract=_Signal&depth=1` | `{"entries":[{"path":"…","node_id":"…","topic":"…","payload":{…},"ts":T,"offset":N}],"next":"TOKEN"}` |
| `POST /kv/lookup` | machine, service, person, admin | body `{"topics": ["…"]}`, at most 1000 | `{"entries":[…]}`, each entry shaped as `/kv`'s (attribution included) — the current entry of each named topic the caller may read; topics with no entry are left out |
| `GET /self` | local service | | the service's registry entry, limits, `standalone_since` and `standalone_ready` |
| `POST /standalone/complete` | local service on a standalone node | | finish the identity handover; returns its durable issuance cutoff |

- `/publish/batch` judges every record as `/publish` would and writes the
  admitted ones with one append per stream, in order; a refused record does
  not stop the others. Commands and audit records are refused in a batch. For
  a high-rate publisher, such as a bridge relaying a plant: one request per
  batch instead of one per sample.
- A refused record of a batch, and a refused `/publish`, names its `reason`.
  `not_written` means the record was admitted but the store did not write it
  (a failed write, or another record of the same append was over the record
  cap): send it again. Every other reason is a verdict on the record itself,
  and the same record is refused again: `too_large` and the reasons of
  `colca_rejected_publishes_total` (`validation`, `grammar`, `node_id`,
  `identity`, `write_denied`, `not_producer`, …), except `draining`, which
  passes.
- A `_Log` record the [log gate](configuration.md#logs) accepts without
  storing it is answered `202` with `{"withheld":"collapsed"|"rate_limited",
  "stream":"logs","topic":"…"}` (in a batch, `{"stream":"logs","withheld":…}`,
  counted in `accepted`). Such an answer has no `offset`, since nothing was
  stored; a client reads `offset` only when `withheld` is absent. It is not an error and must not be sent again: a
  collapsed repeat is counted in its window's summary record, a record over
  the service's budget in the drop notice. Over MQTT the same record gets a
  successful PUBACK and is not fanned out.
- `/fetch` never moves a cursor. `/ack` takes the last offset you processed and
  only moves forward.
- An `/ack` past the stream's head (an offset at or above the next offset) is
  refused with `422` for every caller: it would skip records not yet written.
- `/fetch` names the store the page was read from (`store`, new whenever the
  node's store is created from nothing, for instance after its data volume
  was recreated). A consumer that passes it back as `"store"` on `/ack` gets
  `409` with `"store_changed":true` when the node's store is another one by
  then: the acked offsets name other records there, and the consumer should
  read the new store from its cursor instead. `store` is optional on `/ack`;
  clients that do not send it (chaski, the TypeScript client) are not checked.
  Nodes before this release send no `store`.
- `from=N` reads ahead of the cursor, starting at offset `N`, so a consumer can
  fetch its next page (`from` = the previous page's `next`) while it still
  processes and acks the previous one. It never reads behind the cursor. The
  response's `from` is where the page started; nodes before 0.18.2 ignore the
  parameter and do not send it.
- `prefix` filters on the path part of the topic, not the raw topic.
- `topic` on `/fetch` (repeatable, colca 0.19+) keeps records whose raw topic
  matches one of the MQTT filters (`+`, `#`). A consumer woken by a set of
  topics passes the same set, so it reads exactly what wakes it. Skipped
  records move `next` like `contract` does; ack `next - 1` after an empty or
  short page so they do not stay unread on your cursor.
- `max` defaults to 100 for `/fetch` (at most 5000) and to 1000 for `/kv`
  (at most 10000). Pass `next` back as `after` until it is empty.
- `contract` on `/kv` and `/fetch` may be repeated. An unknown name is a `400`.
  On `/kv` the node keeps an index by contract, so a filtered read costs what
  it returns, not what lies under the prefix. On `/fetch` the other records
  are skipped and `next` moves past them.
- `depth=N` on `/kv` keeps entries at most `N` path segments below `prefix`
  (`prefix=plant/&depth=1` is the level below `plant`); deeper subtrees are
  skipped, not read.
- `folders=true` (needs `depth`) adds `"folders":["plant/l1",…]` to the `/kv`
  page: the paths at the cut that have deeper entries, whether or not they hold
  a record themselves, so a tree view reads one level per call and knows which
  rows expand. Each folder appears once, on the page where its subtree is
  skipped, counts towards `max`, and ignores `contract`. A caller sees a folder
  its read zones cover or lead to.
- Payloads are passed through as raw JSON; numbers keep the exact form the
  publisher sent.
- `records` and `entries` are always arrays.
- A record or entry carries `written_by`, `actor_id`, `actor_label` and
  `actor_kind` when the write that produced it named them (a person's token,
  or a service acting for one); a write that carried none omits all four.
  `/kv`'s entry is the retained projection of the same write `/fetch` returns
  on the stream, so both carry the same four fields.
- `written_by` names who is authenticated for the write, not necessarily who
  decided it: a machine or service publishing directly is its own
  `written_by`, but the state a `_CmdEdit` or `_CmdConfigure` command
  produces is `written_by` the node that executed it — the node is what
  physically appended the record — while `actor_id`/`actor_label`/`actor_kind`
  still name the person or service that commanded it. An operator's
  `_CmdEdit` on their own constant (see [security.md](security.md#grants))
  is the case this exists for: `/kv` can show "set by \<operator\> at \<time\>"
  from Colca's own verified identity, not a client-written field.

When a cursor stands below what retention has already removed, the response
carries a `gap` object that names the missing offsets and times, and `records`
continue after the gap:

```json
{
  "records": [ … ],
  "next": 50123,
  "gap": { "stream": "metrics", "from_offset": 57, "to_offset": 49999,
           "first_ts": 1755100000000, "last_ts": 1755700000000, "approx": false }
}
```

Acknowledge `gap.to_offset` to move past it.

A cursor that was never acked has no position below anything: its first
`/fetch` starts at the stream's oldest retained record (`from` is the
low-water mark) and carries no gap, so a consumer added after retention ran
starts cleanly. Only a saved position, or a `from=` read-ahead position, below
the low-water mark is a gap.

### Waiting for new records

A consumer that follows a stream does not need to poll `/fetch` while the
stream is idle. `GET /watch?stream=entities&stream=annotations` holds the
connection open and writes one JSON line whenever a selected stream grows:

```json
{"streams":["entities","annotations"],"next":{"entities":1201,"annotations":88}}
{"streams":["annotations"],"next":{"annotations":91}}
{"streams":[]}
```

- The first line names every selected stream: drain them all after each
  (re)connect, and a hint missed while disconnected costs nothing.
- `next` is the stream's next offset. A consumer whose cursor already stands
  there can skip the fetch.
- Lines are at least `interval_ms` apart (default 100, at most 10000); streams
  that grow in between are merged into one line and named once. A busy stream
  therefore costs at most one line per interval.
- A line with no streams is a heartbeat, written after 5 s of silence. Treat
  15 s without a line as a dead connection and reconnect.
- A hint carries no records and moves no cursor; read with `/fetch` and `/ack`
  as before. Take no timed fallback poll: a consumer that stops reading is
  caught by the cursor watchdog below, not hidden by a poll.

### Consumers that stop reading

A consumer reads when woken: an MQTT message on its topics, a `/watch` hint, a
reconnect. Nothing reads on a timer, so a lost wake or a stuck loop would leave
records waiting unseen. The node watches every cursor instead:

- Every few seconds it takes the oldest record past the cursor that the
  consumer reads (its last `/fetch` filter applied) and reports its age as
  `colca_cursor_unread_age_seconds{cursor,stream}`. An idle stream reads `0`:
  only records that wait count, not how old the last one is.
- When that age passes `cursors.lag_alarm_after` (default 60 s) it writes a
  retained `_Finding` with reason `cursor_lag` next to the service's own record,
  `_Finding/{node}/{mount}/{service}/cursor_lag`, and retires it once the cursor
  has caught up. The alarm path raises it like any other finding.
- Only a cursor fetched since the node started raises a finding. A cursor
  nobody fetches is abandoned (an old buffer generation, a renamed consumer):
  the gauge and `colca_retention_blocked_by_cursor` show it. Retire it.
- A cursor that stood still for `cursors.stale_after` (default 24 h) while
  records wait past it is named in the node's `_Finding/{node}/stale_cursors`,
  read or not: it holds back retention either way. The finding is retired once
  no cursor is stale.

### Cursors that nobody reads

A consumer that is removed leaves its cursors behind, and every cursor protects
its stream from the pruner. Nothing removes them on its own.

- **List.** `GET /backlog` with the admin token, without a prefix, lists every
  cursor; a local service lists by `prefix`. Each row has `last_ack_ms` (when
  it last moved, `0` when unknown), `stale` (by `cursors.stale_after`) and
  `read_since_start` (whether anyone fetched it since the node started).
- **Retire.** `POST /ack {"cursor":…,"stream":…,"delete":true}` as the
  cursor's owner or the admin. A holder of the admin command class over the
  whole node (`cmd:<node>/#:admin`, `cmd:$node/#:admin` or `cmd:#:admin`) may
  retire another identity's cursor (`c/<name>/…`, `<ulid>/…`) too, once it is
  stale as `GET /backlog` reports it (`409` before), and each such retirement
  is written to the audit stream (`outcome: success`, the actor, the cursor and
  its owner). It may not move one, nor retire a command delivery floor
  (`…/cmd`), a node-owned cursor (`downlink:…`, `down-def:…`) or a cursor of
  colca's own services (`c/historian/…`). The node logs who retired it. A consumer that
  fetches the cursor again starts it over at the stream's oldest retained
  record, without a gap.
- **Expire.** `retention.streams.<stream>.ignore_cursors_after` lets the pruner
  pass a cursor that has not moved for that long, with a `_StreamGap` record a
  returning consumer sees. Off unless configured.
- A live consumer that filters its fetch must ack the page's last scanned
  offset (`next - 1`) also when the page held nothing of its own, or its cursor
  stands still and turns stale.
- A service subscribes to its own finding and fails its health check while it
  stands. chaski and `colca-historian` do so.

## Administration

| Method and path | Request | Response |
|---|---|---|
| `POST /enroll` | `{"ulid","pubkey","kind":"external"\|"node","element","grants":[…]}` | `{"ulid":"…","offset":N}`; `409` when the key or the element is already taken, `422` on an invalid entry or an element this node does not hold |
| `GET /enroll` | `?max=1000&after=TOKEN` | the locally enrolled entries, public keys only |
| `DELETE /enroll/{ulid}` | `?retire=true` | `{"revoked":true,"offset":N}`; the same batch retires the records the identity authored about itself — its `_ServiceDetails`, at every mount it published one at. Only that identity may write them, so one left behind could never be retired by anyone. A plain revoke keeps what a child node replicated up: the child may be enrolled again and resumes from its own cursor. With `retire=true` (kind `node` only, `409` otherwise; `400` on a value that is not a boolean) the same batch also tombstones every current-state record the child and the nodes below it replicated, clears its replication marks, and deletes its definitions cursor (`downlink-def:<ulid>`) and every cursor in its own namespace (`<ulid>/...`), so nothing it left stands as live or holds retention and the same identity enrolled later starts clean (a plain revoke keeps the definitions cursor as the child's catch-up position); the response adds `"retired":true,"records_retired":N`. The tombstones replicate up, so ancestors retire their copies too |
| `POST /enroll/{ulid}/drain` | | `{"ulid","offset","status":"draining"}`; decommissions a child node: new commands under its mount are refused, and once its queue is delivered or expired it is retired as with `DELETE ?retire=true`. `409` for an entry that is not a node or is already draining |
| `GET /debug/state` | | the next offset of every stream |

A node that is reachable only through the tree is administered with
`_CmdAdmin` commands sent to it from an ancestor: `enroll`, `revoke`, and
`fetchLogs`, which returns one page of its own logs in the ack's `result`. See
[Fetching a node's logs](concepts.md#fetching-a-nodes-logs).

## Secrets

Available when `secrets_dir` is set. The owner is the calling service; a local
service cannot address another service's secrets.

| Method and path | Request | Response |
|---|---|---|
| `PUT /secrets/{owner}/{name}` | `{"envelope":{…},"expires_at":null,"expected_revision":N}` | metadata, never the ciphertext |
| `GET /secrets/{owner}` | `?max=1000&after=TOKEN` | metadata page |
| `GET /secrets/{owner}/{name}` | | metadata; `410` once expired |
| `DELETE /secrets/{owner}/{name}` | `?expected_revision=N` | `{"deleted":true}` |

`expected_revision` of `0` means create only; any other value is a
compare-and-swap.

## Errors and limits

| Status | Meaning |
|---|---|
| `202` | `/publish`: a `_Log` record the log gate collapsed or capped; accepted, not stored |
| `400` | malformed request |
| `401` | a presented credential was not accepted |
| `403` | the caller may not do this |
| `413` | body too large; a record over the record cap has `reason` `too_large` |
| `422` | bad topic, unknown contract or invalid payload, with `reason` when the node names one |
| `429` | rate limit; see `Retry-After` |
| `503` | `/publish`: the record was admitted but not written (`reason` `not_written`); send it again |

Every route belongs to a rate class. Authenticated callers get their own
quota; the local door and unauthenticated traffic are limited per source
address. Per caller: `/kv` 5 requests a second (burst 10), `/fetch` 25 (burst
50), `/watch` 2 new connections a second (burst 8) and 8 open at a time.

## MQTT reason codes

Over MQTT 5 at QoS 1 or higher, a refused publish is answered in the `PUBACK`:
`0x90` unknown topic or contract, `0x99` invalid payload, `0x87` not
authorized, `0x89` node is draining. MQTT 3.1.1 has no reason codes; the
publish is dropped and counted in `colca_rejected_publishes_total`.

`/healthz` includes `clock`: `now_ms`, `is_root`, `offset_ms`, nullable `sync_age_seconds`, and `beacon_interval_seconds`. These describe the existing `_TimeSync` authority; a null age means an edge has never synchronized. Host NTP status is not inferred.

### Local stream wakeups and backlog telemetry

Internal services may subscribe to `GET /watch?stream=metrics&stream=entities` on
the local door with their existing service identity. The NDJSON response starts
with a catch-up hint and then emits `{"streams":["metrics"]}` after successful
durable commits. Five-second heartbeats contain no stream names. Hints can
coalesce: capture the local wakeup version before fetching, drain the durable
cursor to empty, then wait on that version. A disconnect requires reconnecting
and draining again; a hint never acknowledges or contains records.

This route is not on the published API and does not accept forwarded human bearer
identities. Streaming clients must use the heartbeat/read deadline rather than an
ordinary short whole-request timeout. `/fetch` and `/ack` retain their normal
ownership rules. Fixed minimum spacing between drain starts can batch bursts;
nonempty pages within a drain do not need an additional delay.

`GET /backlog?prefix=c/projector/` on the same local door returns selected durable
cursor positions, stream heads and `lag_records`, with each cursor's
`last_ack_ms`, `stale` and `read_since_start`. It is read-only. Values count
outstanding offset distance (an upper bound after compaction), not bytes. Up to
32 nonempty prefixes and 256 matching cursors are accepted; excess results fail
instead of truncating away a potentially overloaded consumer. A missing expected
cursor is not evidence of an empty queue.
