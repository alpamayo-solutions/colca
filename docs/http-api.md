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
| `GET /healthz` | anyone | | `{"ok":true,"ulid":"…"}` |
| `GET /metrics` | anyone | | Prometheus text |
| `POST /publish` | machine, service, person (commands), admin | `{"topic":"…","payload":{…}}` | `{"stream":"…","offset":N,"topic":"…"}` |
| `GET /fetch` | machine, service, person, admin | `?stream=S&cursor=NAME&max=100&prefix=P` | `{"records":[{"offset":N,"topic":"…","payload":{…},"ts":T}],"next":N}` |
| `POST /ack` | owner of the cursor, admin | `{"cursor":"NAME","stream":"S","offset":N}` | `{"moved":true}` |
| `GET /kv` | machine, service, person, admin | `?prefix=P&max=1000&after=TOKEN&contract=_Signal` | `{"entries":[{"path":"…","node_id":"…","topic":"…","payload":{…},"ts":T,"offset":N}],"next":"TOKEN"}` |
| `GET /self` | local service | | the service's own registry entry and limits |

- `/fetch` never moves a cursor. `/ack` takes the last offset you processed and
  only moves forward.
- `prefix` filters on the path part of the topic, not the raw topic.
- `max` defaults to 100 for `/fetch` (at most 1000) and to 1000 for `/kv`
  (at most 10000). Pass `next` back as `after` until it is empty.
- `contract` on `/kv` may be repeated. An unknown name is a `400`.
- Payloads are passed through as raw JSON; numbers keep the exact form the
  publisher sent.
- `records` and `entries` are always arrays.

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

## Administration

| Method and path | Request | Response |
|---|---|---|
| `POST /enroll` | `{"ulid","pubkey","kind":"external"\|"node","element","grants":[…]}` | `{"ulid":"…","offset":N}`; `409` when the key or the element is already taken, `422` on an invalid entry or an element this node does not hold |
| `GET /enroll` | `?max=1000&after=TOKEN` | the locally enrolled entries, public keys only |
| `DELETE /enroll/{ulid}` | | `{"revoked":true,"offset":N}` |
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
address.

## MQTT reason codes

Over MQTT 5 at QoS 1 or higher, a refused publish is answered in the `PUBACK`:
`0x90` unknown topic or contract, `0x99` invalid payload, `0x87` not
authorized, `0x89` node is draining. MQTT 3.1.1 has no reason codes; the
publish is dropped and counted in `colca_rejected_publishes_total`.
