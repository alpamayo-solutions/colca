# @alpamayo-solutions/colca-client

A Colca node's door, in TypeScript: records over HTTP — `fetch`, `ack`,
`publish`, `kv`, `self` — and live values over MQTT. No runtime dependencies
for the door, no framework, no opinion about how your service is built.

```sh
npm install @alpamayo-solutions/colca-client
```

## A consumer

```ts
import { Door, Stream } from "@alpamayo-solutions/colca-client";

const door = new Door({ baseUrl: "http://colca", service: "my-app" });
const panels = new Stream(door, "annotations", door.cursorName("panels"), {
  prefix: "wisewoods/line1",
});

for await (const record of panels.follow()) {
  // Acked page by page: a handler that throws sees its page again.
  console.log(record.topic, record.payload);
}
```

## A producer

```ts
await door.publishTo(
  { root: "steine", contract: "_Metric", node: "n-technikum", path: "wisewoods/line1/mas2/grit" },
  { signal_id: "01M2AB…", timestamp: Date.now() / 1000, value: 60 },
);
```

## Live values

`@alpamayo-solutions/colca-client/live` keeps one MQTT connection to the node's
WebSocket door for people and applications, and hands out values as they change.

```sh
npm install mqtt
```

```ts
import { Live } from "@alpamayo-solutions/colca-client/live";

const live = new Live({
  url: "wss://node:8885",
  // Asked before every connection, so hand back a token that is valid now.
  token: () => auth.freshToken(),
});

const stop = live.subscribe("steine/v1/_Metric/n-technikum/wisewoods/#", (value) => {
  show(value.topic, value.payload);
});
live.onState((state) => showOffline(state !== "online"));
```

Data and entity paths are retained at the node, so a subscription starts with
the current values and continues with the changes; nothing has to be fetched
first. Around that the client does what a page left open all day needs:

- **One connection for the whole page.** `subscribe` returns the function that
  ends that subscription and no other; the node's subscription goes when the
  last listener on a filter does.
- **A second subscriber gets the value at once.** The client keeps the last value
  of every topic, and `latest()` and `values()` read it.
- **A fresh token before the old one runs out.** The node ends a session when its
  token expires. The client reconnects shortly before, with a new token and every
  subscription sent again, and stays `online` while doing so.
- **Waits that grow after a drop**, jittered, each attempt with a fresh token.

These are values, not a log. A change during a reconnect is superseded by the
retained value that follows it. Whatever must see every record reads a stream
through the door.

### Commands

A person's session sends commands, not values. `command()` sends one and waits
for the executor's `_Ack`:

```ts
const ack = await live.command("steine/v1/_CmdParam/n-technikum/wisewoods/line1/mas2/sta1/aggos/setGrit", {
  params: { signal: "grit", value: 120 },
});
if (ack.result_code !== 200) showRefusal(ack.message);
```

It adds the correlation id and the expiry, subscribes to the acknowledgements
before it sends, so an executor that answers at once is not missed, and matches
the answer by its id wherever in the tree it arrives. The promise settles with
the `_Ack` whatever its result code, and rejects with `CommandTimeout` when
nobody answers in time (30 s by default, which is also the command's expiry).
Offline, nothing is queued: a setpoint sent minutes late is a different
setpoint.

`publish()` sends a single record without waiting, and `newUlid()` makes ids
that sort by the time they were made, as the node's own do.

## Which door, which credential

| Door      | URL                                                     | Credential                                                        |
| --------- | ------------------------------------------------------- | ----------------------------------------------------------------- |
| local     | `http://colca` (port 80, inside the deployment network) | none — reachability is the credential; `service` names the caller |
| published | `https://node:443`                                      | a person's bearer token, or a machine's pinned client certificate |

A client certificate is a TLS matter, so it belongs to the runtime rather than
to this package: hand in a `fetch` that carries it (`options.fetch`).

## Things the node insists on

- **Cursors belong to their caller.** They are named `c/{service}/…`, which is
  what `door.cursorName()` builds; the node refuses any other name.
- **Fetching never moves a cursor.** Only `ack` does, and only forward. `tail`
  reads the end of a stream without touching it, which is what a view wants.
- **A gap is not an error.** When records were pruned below the cursor, the page
  says so. `Stream` passes it to `onGap` and acks past it, so it is reported
  once rather than forever.
- **The node decides what you see.** `/fetch` and `/kv` filter by the caller's
  read grants; a publish is judged against its zone and grants. A 403 carries
  the node's reason.

## Ids

`deriveAnnotationId()` computes an annotation's id the way the node's own
contracts package does. It is derived rather than drawn so that create, update
and delete of one annotation are appends under the same id — a producer that
runs twice overwrites its own record instead of doubling it.

The agreement is checked against `clients/spec/vectors.json`, generated from
`colca-data-contracts`. If a rule changes there, the tests here fail.

## Development

```sh
npm install
npm run lint
npm run typecheck
npm test
npm run build
```

The wire format itself is documented in
[HTTP API](https://alpamayo-solutions.github.io/colca/http-api/).
