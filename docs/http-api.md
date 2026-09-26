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
| `POST /publish` | machine, service, person (commands), admin | `{"topic":"…","payload":{…}}` | `{"stream":"…","offset":N,"topic":"…"}` |
| `POST /publish/batch` | machine, service | `{"records":[{"topic":"…","payload":{…}},…]}` (1–5000 records, 16 MiB) | `{"accepted":N,"results":[{"stream":"…","offset":N} or {"error":"…"},…]}` |
| `GET /fetch` | machine, service, person, admin | `?stream=S&cursor=NAME&max=100&prefix=P&contract=_Annotation&from=N` | `{"records":[{"offset":N,"topic":"…","payload":{…},"ts":T}],"next":N,"from":N}` |
| `GET /watch` | machine, service, person, admin | `?stream=S&stream=S2&interval_ms=100` | NDJSON, one line per change: `{"streams":["S"],"next":{"S":N}}` |
| `POST /ack` | owner of the cursor, admin | `{"cursor":"NAME","stream":"S","offset":N}` | `{"moved":true}` |
| `GET /kv` | machine, service, person, admin | `?prefix=P&max=1000&after=TOKEN&contract=_Signal&depth=1` | `{"entries":[{"path":"…","node_id":"…","topic":"…","payload":{…},"ts":T,"offset":N}],"next":"TOKEN"}` |
| `GET /self` | local service | | the service's registry entry, limits, `standalone_since` and `standalone_ready` |
| `POST /standalone/complete` | local service on a standalone node | | finish the identity handover; returns its durable issuance cutoff |

- `/publish/batch` judges every record as `/publish` would and writes the
  admitted ones with one append per stream, in order; a refused record does
  not stop the others. Commands and audit records are refused in a batch. For
  a high-rate publisher, such as a bridge relaying a plant: one request per
  batch instead of one per sample.
- `/fetch` never moves a cursor. `/ack` takes the last offset you processed and
  only moves forward.
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
- `max` defaults to 100 for `/fetch` (at most 1000) and to 1000 for `/kv`
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
- A service subscribes to its own finding and fails its health check while it
  stands. chaski and `colca-historian` do so.

## Administration

| Method and path | Request | Response |
|---|---|---|
| `POST /enroll` | `{"ulid","pubkey","kind":"external"\|"node","element","grants":[…]}` | `{"ulid":"…","offset":N}`; `409` when the key or the element is already taken, `422` on an invalid entry or an element this node does not hold |
| `GET /enroll` | `?max=1000&after=TOKEN` | the locally enrolled entries, public keys only |
| `DELETE /enroll/{ulid}` | | `{"revoked":true,"offset":N}`; the same batch retires the records the identity authored about itself — its `_ServiceDetails`, at every mount it published one at. Only that identity may write them, so one left behind could never be retired by anyone |
| `GET /debug/state` | | the next offset of every stream |

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
| `400` | malformed request |
| `401` | a presented credential was not accepted |
| `403` | the caller may not do this |
| `413` | body too large |
| `422` | bad topic, unknown contract or invalid payload |
| `429` | rate limit; see `Retry-After` |

Every route belongs to a rate class. Authenticated callers get their own
quota; the local door and unauthenticated traffic are limited per source
address. Per caller: `/kv` 5 requests a second (burst 10), `/fetch` 25 (burst
50), `/watch` 2 new connections a second (burst 8) and 8 open at a time.

## MQTT reason codes

Over MQTT 5 at QoS 1 or higher, a refused publish is answered in the `PUBACK`:
`0x90` unknown topic or contract, `0x99` invalid payload, `0x87` not
authorized, `0x89` node is draining. MQTT 3.1.1 has no reason codes; the
publish is dropped and counted in `colca_rejected_publishes_total`.
